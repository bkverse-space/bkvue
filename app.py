import os
import re
import math
from datetime import datetime, timedelta, timezone
from functools import wraps
from urllib.parse import quote

import requests
from flask import Flask, jsonify, render_template, request, url_for
from kubernetes import client as kube_client
from kubernetes import config as kube_config
from kubernetes.client.exceptions import ApiException
from kubernetes.config.config_exception import ConfigException


app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "change-this-in-production")
DISPLAY_TIMEZONE = timezone(timedelta(hours=8), name="UTC+8")


class RegistryError(Exception):
    def __init__(self, message, status_code=502):
        super().__init__(message)
        self.status_code = status_code


class RegistryClient:
    """Small client for the Docker Distribution Registry v2 API."""

    def __init__(self):
        self.base_url = os.getenv("REGISTRY_URL", "http://localhost:5000").rstrip("/")
        self.verify_tls = os.getenv("REGISTRY_VERIFY_TLS", "true").lower() not in ("0", "false", "no")
        self.timeout = int(os.getenv("REGISTRY_TIMEOUT", "15"))
        self.session = requests.Session()
        username = os.getenv("REGISTRY_USERNAME")
        password = os.getenv("REGISTRY_PASSWORD")
        if username and password:
            self.session.auth = (username, password)

    def _request(self, method, path, headers=None, **kwargs):
        headers = headers.copy() if headers else {}
        try:
            response = self.session.request(
                method,
                f"{self.base_url}{path}",
                headers=headers,
                timeout=self.timeout,
                verify=self.verify_tls,
                **kwargs,
            )
        except requests.RequestException as exc:
            raise RegistryError(f"Unable to reach registry: {exc}") from exc

        if response.status_code == 401:
            token = self._bearer_token(response)
            if token:
                headers["Authorization"] = f"Bearer {token}"
                try:
                    response = self.session.request(
                        method,
                        f"{self.base_url}{path}",
                        headers=headers,
                        timeout=self.timeout,
                        verify=self.verify_tls,
                        **kwargs,
                    )
                except requests.RequestException as exc:
                    raise RegistryError(f"Unable to reach registry: {exc}") from exc

        if response.status_code >= 400:
            detail = response.text
            try:
                detail = response.json().get("errors", [{}])[0].get("message", detail)
            except (ValueError, IndexError, AttributeError):
                pass
            raise RegistryError(detail or f"Registry returned HTTP {response.status_code}", response.status_code)
        return response

    def _bearer_token(self, response):
        """Resolve a standard Docker Registry bearer challenge, when present."""
        challenge = response.headers.get("WWW-Authenticate", "")
        if not challenge.lower().startswith("bearer "):
            return None
        params = dict(re.findall(r'([a-zA-Z]+)="([^"\\]*)"', challenge))
        realm = params.pop("realm", None)
        if not realm:
            return None
        try:
            token_response = self.session.get(
                realm,
                params=params,
                timeout=self.timeout,
                verify=self.verify_tls,
            )
            token_response.raise_for_status()
            payload = token_response.json()
        except (requests.RequestException, ValueError) as exc:
            raise RegistryError(f"Unable to get registry access token: {exc}") from exc
        return payload.get("token") or payload.get("access_token")

    def ping(self):
        response = self._request("GET", "/v2/")
        return response.headers.get("Docker-Distribution-Api-Version", "registry v2")

    def repositories(self):
        response = self._request("GET", "/v2/_catalog?n=1000")
        return sorted(response.json().get("repositories", []))

    def tags(self, repository):
        path = f"/v2/{quote(repository, safe='/')}/tags/list?n=1000"
        response = self._request("GET", path)
        return sorted(response.json().get("tags") or [], reverse=True)

    def manifest(self, repository, reference):
        path = f"/v2/{quote(repository, safe='/')}/manifests/{quote(reference, safe='')}"
        headers = {
            "Accept": ", ".join(
                [
                    "application/vnd.oci.image.manifest.v1+json",
                    "application/vnd.oci.image.index.v1+json",
                    "application/vnd.docker.distribution.manifest.v2+json",
                    "application/vnd.docker.distribution.manifest.list.v2+json",
                ]
            )
        }
        response = self._request("GET", path, headers=headers)
        return response.json(), response.headers.get("Docker-Content-Digest"), response.headers.get("Content-Type")

    def blob(self, repository, digest):
        path = f"/v2/{quote(repository, safe='/')}/blobs/{quote(digest, safe=':')}"
        response = self._request("GET", path)
        return response.json()


class PrometheusClient:
    """Read cluster metrics from the Prometheus instant-query API."""

    def __init__(self):
        self.base_url = os.getenv("PROMETHEUS_URL", "http://prometheus-server.monitoring.svc:9090").rstrip("/")
        self.verify_tls = os.getenv("PROMETHEUS_VERIFY_TLS", "true").lower() not in ("0", "false", "no")
        self.timeout = int(os.getenv("PROMETHEUS_TIMEOUT", "10"))
        self.session = requests.Session()
        token = os.getenv("PROMETHEUS_BEARER_TOKEN")
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"

    def query_value(self, query):
        result = self._query(query)
        if not result:
            return None
        try:
            return float(result[0]["value"][1])
        except (KeyError, IndexError, TypeError, ValueError):
            return None

    def query_vector(self, query, label):
        values = {}
        for item in self._query(query):
            try:
                key = item["metric"][label]
                values[key] = float(item["value"][1])
            except (KeyError, IndexError, TypeError, ValueError):
                continue
        return values

    def _query(self, query):
        try:
            response = self.session.get(
                f"{self.base_url}/api/v1/query",
                params={"query": query},
                timeout=self.timeout,
                verify=self.verify_tls,
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise RegistryError(f"无法查询 Prometheus: {exc}") from exc
        if payload.get("status") != "success":
            raise RegistryError(payload.get("error", "Prometheus 查询失败"))
        return payload.get("data", {}).get("result", [])


class ArgoCDClient:
    """Read Argo CD Application CRs through the in-cluster Kubernetes API."""

    def __init__(self):
        load_kubernetes_config()
        self.namespace = os.getenv("ARGOCD_NAMESPACE", "argocd")
        self.api = kube_client.CustomObjectsApi()

    def applications(self):
        try:
            result = self.api.list_namespaced_custom_object(
                group="argoproj.io",
                version="v1alpha1",
                namespace=self.namespace,
                plural="applications",
            )
        except ApiException as exc:
            raise RegistryError(f"无法读取 Argo CD Applications: {exc.reason}", exc.status) from exc
        return [self._summary(item) for item in result.get("items", [])]

    def application(self, name):
        try:
            item = self.api.get_namespaced_custom_object(
                group="argoproj.io",
                version="v1alpha1",
                namespace=self.namespace,
                plural="applications",
                name=name,
            )
        except ApiException as exc:
            message = "未找到该项目。" if exc.status == 404 else f"无法读取 Argo CD Application: {exc.reason}"
            raise RegistryError(message, exc.status) from exc
        return self._summary(item, include_history=True)

    def _summary(self, application, include_history=False):
        metadata = application.get("metadata", {})
        spec = application.get("spec", {})
        status = application.get("status", {})
        sync = status.get("sync", {}).get("status", "Unknown")
        health = status.get("health", {}).get("status", "Unknown")
        history = sorted(status.get("history", []), key=lambda item: item.get("deployedAt", ""), reverse=True)
        operation = status.get("operationState", {})
        deployed_at = (history[0].get("deployedAt") if history else None) or operation.get("finishedAt")
        images = list(dict.fromkeys(status.get("summary", {}).get("images") or []))
        resources = sorted(
            status.get("resources") or [],
            key=lambda resource: (
                {"Deployment": 0, "StatefulSet": 0, "DaemonSet": 0, "Job": 1, "Service": 2, "Ingress": 3}.get(
                    resource.get("kind"), 4
                ),
                resource.get("kind", ""),
                resource.get("name", ""),
            ),
        )
        return {
            "name": metadata.get("name", "unknown"),
            "namespace": spec.get("destination", {}).get("namespace") or "未指定",
            "sync": sync,
            "health": health,
            "revision": status.get("sync", {}).get("revision") or "-",
            "deployed_at": format_created(parse_created(deployed_at)) if deployed_at else "暂无发布记录",
            "images": images,
            "image_versions": [image_tag(image) for image in images],
            "is_ready": sync == "Synced" and health == "Healthy",
            "resources": [
                {
                    "kind": resource.get("kind", "Unknown"),
                    "name": resource.get("name", "unknown"),
                    "namespace": resource.get("namespace") or spec.get("destination", {}).get("namespace") or "-",
                    "sync": resource.get("status", "Unknown"),
                    "health": (resource.get("health") or {}).get("status", "Unknown"),
                    "hook": resource.get("hook"),
                }
                for resource in resources
            ] if include_history else [],
            "history": [
                {
                    "revision": item.get("revision", "-"),
                    "deployed_at": format_created(parse_created(item.get("deployedAt"))) if item.get("deployedAt") else "-",
                    "source": item.get("source", {}).get("path") or "-",
                }
                for item in history
            ] if include_history else [],
        }


class ClusterKubernetesClient:
    """Read node and pod status from the Kubernetes core API."""

    def __init__(self):
        load_kubernetes_config()
        self.api = kube_client.CoreV1Api()
        self.apps_api = kube_client.AppsV1Api()
        self.batch_api = kube_client.BatchV1Api()

    def nodes(self):
        try:
            return [self._node_summary(node) for node in self.api.list_node().items]
        except ApiException as exc:
            raise RegistryError(f"无法读取集群节点: {exc.reason}", exc.status) from exc

    def node(self, name):
        try:
            node = self.api.read_node(name)
            pods = self.api.list_pod_for_all_namespaces(field_selector=f"spec.nodeName={name}").items
        except ApiException as exc:
            message = "未找到该节点。" if exc.status == 404 else f"无法读取节点详情: {exc.reason}"
            raise RegistryError(message, exc.status) from exc
        summary = self._node_summary(node)
        summary["conditions"] = [
            {
                "type": condition.type,
                "status": condition.status,
                "reason": condition.reason or "-",
                "message": condition.message or "-",
            }
            for condition in node.status.conditions or []
        ]
        summary["pods"] = [
            {
                "namespace": pod.metadata.namespace,
                "name": pod.metadata.name,
                "phase": pod.status.phase or "Unknown",
                "restarts": sum(status.restart_count or 0 for status in (pod.status.container_statuses or [])),
            }
            for pod in sorted(pods, key=lambda item: (item.metadata.namespace, item.metadata.name))
        ]
        return summary

    def pod_logs(self, namespace, name, container=None, tail_lines=500, previous=False):
        try:
            pod = self.api.read_namespaced_pod(name, namespace)
        except ApiException as exc:
            message = "未找到该 Pod。" if exc.status == 404 else f"无法读取 Pod: {exc.reason}"
            raise RegistryError(message, exc.status) from exc

        containers = [item.name for item in (pod.spec.containers or [])]
        containers.extend(item.name for item in (pod.spec.init_containers or []))
        if not containers:
            raise RegistryError("该 Pod 未包含可读取日志的容器。", 404)
        selected_container = container or containers[0]
        if selected_container not in containers:
            raise RegistryError("指定的容器不属于该 Pod。", 400)
        try:
            logs = self.api.read_namespaced_pod_log(
                name,
                namespace,
                container=selected_container,
                tail_lines=tail_lines,
                timestamps=True,
                previous=previous,
            )
        except ApiException as exc:
            raise RegistryError(f"无法读取 Pod 日志: {exc.reason}", exc.status) from exc
        logs = logs or "该条件下没有日志输出。"
        container_statuses = pod.status.container_statuses or []
        return {
            "namespace": namespace,
            "name": name,
            "containers": containers,
            "container": selected_container,
            "tail_lines": tail_lines,
            "previous": previous,
            "logs": logs,
            "line_count": len(logs.splitlines()),
            "phase": pod.status.phase or "Unknown",
            "node_name": pod.spec.node_name or "-",
            "pod_ip": pod.status.pod_ip or "-",
            "restarts": sum(status.restart_count or 0 for status in container_statuses),
            "started_at": format_created(pod.status.start_time) if pod.status.start_time else "-",
            "read_at": datetime.now(DISPLAY_TIMEZONE).strftime("%Y-%m-%d %H:%M UTC+8"),
        }

    def project_pods(self, resources):
        """Resolve Argo-managed workloads to their runtime Pods via owner references."""
        pod_cache = {}
        replica_set_cache = {}
        records = {}

        def namespace_pods(namespace):
            if namespace not in pod_cache:
                try:
                    pod_cache[namespace] = self.api.list_namespaced_pod(namespace).items
                except ApiException as exc:
                    raise RegistryError(f"无法读取 Namespace {namespace} 中的 Pod: {exc.reason}", exc.status) from exc
            return pod_cache[namespace]

        def namespace_replica_sets(namespace):
            if namespace not in replica_set_cache:
                try:
                    replica_set_cache[namespace] = self.apps_api.list_namespaced_replica_set(namespace).items
                except ApiException as exc:
                    raise RegistryError(f"无法读取 Namespace {namespace} 中的 ReplicaSet: {exc.reason}", exc.status) from exc
            return replica_set_cache[namespace]

        def owner_uids(resource):
            return {reference.uid for reference in (resource.metadata.owner_references or []) if reference.uid}

        def add_pod(pod, workload):
            key = (pod.metadata.namespace, pod.metadata.name)
            record = records.setdefault(
                key,
                {
                    "namespace": pod.metadata.namespace,
                    "name": pod.metadata.name,
                    "phase": pod.status.phase or "Unknown",
                    "restarts": sum(status.restart_count or 0 for status in (pod.status.container_statuses or [])),
                    "workloads": set(),
                },
            )
            record["workloads"].add(workload)

        for resource in resources:
            kind = resource.get("kind")
            namespace = resource.get("namespace")
            name = resource.get("name")
            if not namespace or not name:
                continue
            pods = namespace_pods(namespace)
            workload = f"{kind}/{name}"
            try:
                if kind == "Pod":
                    for pod in pods:
                        if pod.metadata.name == name:
                            add_pod(pod, workload)
                elif kind == "Deployment":
                    deployment = self.apps_api.read_namespaced_deployment(name, namespace)
                    replica_set_uids = {
                        replica_set.metadata.uid
                        for replica_set in namespace_replica_sets(namespace)
                        if deployment.metadata.uid in owner_uids(replica_set)
                    }
                    for pod in pods:
                        if owner_uids(pod) & replica_set_uids:
                            add_pod(pod, workload)
                elif kind == "StatefulSet":
                    stateful_set = self.apps_api.read_namespaced_stateful_set(name, namespace)
                    for pod in pods:
                        if stateful_set.metadata.uid in owner_uids(pod):
                            add_pod(pod, workload)
                elif kind == "DaemonSet":
                    daemon_set = self.apps_api.read_namespaced_daemon_set(name, namespace)
                    for pod in pods:
                        if daemon_set.metadata.uid in owner_uids(pod):
                            add_pod(pod, workload)
                elif kind == "Job":
                    job = self.batch_api.read_namespaced_job(name, namespace)
                    for pod in pods:
                        if job.metadata.uid in owner_uids(pod):
                            add_pod(pod, workload)
            except ApiException as exc:
                if exc.status != 404:
                    raise RegistryError(f"无法解析 {workload} 的 Pod: {exc.reason}", exc.status) from exc

        result = []
        for record in records.values():
            record["workloads"] = sorted(record["workloads"])
            result.append(record)
        return sorted(result, key=lambda pod: (pod["namespace"], pod["name"]))

    @staticmethod
    def _node_summary(node):
        labels = node.metadata.labels or {}
        roles = [key.removeprefix("node-role.kubernetes.io/") or value for key, value in labels.items() if key.startswith("node-role.kubernetes.io/")]
        conditions = {condition.type: condition.status for condition in (node.status.conditions or [])}
        addresses = {address.type: address.address for address in (node.status.addresses or [])}
        return {
            "name": node.metadata.name,
            "roles": roles or ["worker"],
            "ready": conditions.get("Ready") == "True",
            "unschedulable": bool(node.spec.unschedulable),
            "internal_ip": addresses.get("InternalIP", "-"),
            "kubelet_version": node.status.node_info.kubelet_version if node.status.node_info else "-",
            "os_image": node.status.node_info.os_image if node.status.node_info else "-",
            "capacity_cpu": (node.status.allocatable or {}).get("cpu", "-"),
            "capacity_memory": (node.status.allocatable or {}).get("memory", "-"),
        }


def registry():
    return RegistryClient()


def load_kubernetes_config():
    if os.getenv("KUBERNETES_SERVICE_HOST"):
        try:
            kube_config.load_incluster_config()
        except ConfigException as exc:
            raise RegistryError("无法加载 Kubernetes ServiceAccount 配置。") from exc
    else:
        try:
            kube_config.load_kube_config()
        except ConfigException as exc:
            raise RegistryError("未检测到 Kubernetes 配置。集群内请使用 ServiceAccount 部署；本地运行请挂载 kubeconfig。") from exc


def argocd():
    return ArgoCDClient()


def prometheus():
    return PrometheusClient()


def cluster_kubernetes():
    return ClusterKubernetesClient()


def registry_view(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except RegistryError as exc:
            if request.accept_mimetypes.best == "application/json":
                return jsonify(error=str(exc)), exc.status_code
            return render_template("error.html", status=exc.status_code, message=str(exc)), exc.status_code

    return wrapped


def format_size(value):
    if value is None:
        return "-"
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024


def format_metric(value, digits=1, suffix=""):
    if value is None or not math.isfinite(value):
        return "暂无数据"
    if value == int(value):
        return f"{int(value)}{suffix}"
    return f"{value:.{digits}f}{suffix}"


def percent_level(value):
    if value is None or not math.isfinite(value):
        return "unknown"
    if value >= 90:
        return "critical"
    if value >= 75:
        return "warning"
    return "normal"


def count_level(value):
    if value is None or not math.isfinite(value):
        return "unknown"
    return "warning" if value > 0 else "normal"


def available_level(value):
    if value is None or not math.isfinite(value):
        return "unknown"
    return "normal"


def node_metric_vectors(client):
    return {
        "cpu_percent": client.query_vector(
            '100 * sum by (node) (rate(container_cpu_usage_seconds_total{container!="",image!=""}[5m])) / on(node) sum by (node) (kube_node_status_allocatable{resource="cpu",unit="core"})',
            "node",
        ),
        "memory_percent": client.query_vector(
            '100 * sum by (node) (container_memory_working_set_bytes{container!="",image!=""}) / on(node) sum by (node) (kube_node_status_allocatable{resource="memory",unit="byte"})',
            "node",
        ),
        "pods": client.query_vector('count by (node) (kube_pod_info{node!=""})', "node"),
    }


def apply_node_metrics(nodes, metric_vectors):
    for node in nodes:
        node["cpu_percent"] = metric_vectors["cpu_percent"].get(node["name"])
        node["memory_percent"] = metric_vectors["memory_percent"].get(node["name"])
        node["pod_count"] = metric_vectors["pods"].get(node["name"])
        node["cpu_label"] = format_metric(node["cpu_percent"], suffix="%")
        node["memory_label"] = format_metric(node["memory_percent"], suffix="%")
        node["pod_label"] = format_metric(node["pod_count"], 0)
        node["cpu_level"] = percent_level(node["cpu_percent"])
        node["memory_level"] = percent_level(node["memory_percent"])
    return nodes


def log_tail_lines(value):
    try:
        return max(10, min(int(value), 5000))
    except (TypeError, ValueError):
        return 500


def parse_created(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def format_created(value):
    if not value:
        return "构建时间未知"
    return value.astimezone(DISPLAY_TIMEZONE).strftime("%Y-%m-%d %H:%M UTC+8")


def image_tag(image):
    """Return an image tag without confusing a registry port for a tag."""
    image_without_digest = image.split("@", 1)[0]
    last_segment = image_without_digest.rsplit("/", 1)[-1]
    return last_segment.rsplit(":", 1)[-1] if ":" in last_segment else "未标记"


def image_created(client, repository, manifest):
    """Get the image build time from its config, with an OCI annotation fallback."""
    created = manifest.get("annotations", {}).get("org.opencontainers.image.created")
    config_digest = manifest.get("config", {}).get("digest")
    if config_digest:
        try:
            created = client.blob(repository, config_digest).get("created") or created
        except RegistryError:
            # Some proxies allow manifests but deny config blob reads. Keep the tag visible.
            pass
    return parse_created(created)


app.jinja_env.filters["size"] = format_size


@app.context_processor
def common_context():
    return {
        "registry_url": os.getenv("REGISTRY_URL", "http://localhost:5000"),
        "argocd_namespace": os.getenv("ARGOCD_NAMESPACE", "argocd"),
    }


@app.get("/")
@registry_view
def index():
    projects = argocd().applications()
    query = request.args.get("q", "").strip().lower()
    namespace_filter = request.args.get("namespace", "").strip()
    namespaces = sorted({project["namespace"] for project in projects})
    if query:
        projects = [project for project in projects if query in project["name"].lower() or query in project["namespace"].lower()]
    if namespace_filter:
        projects = [project for project in projects if project["namespace"] == namespace_filter]
    projects.sort(key=lambda project: (project["is_ready"], project["name"]))
    summary = {
        "total": len(projects),
        "ready": sum(project["is_ready"] for project in projects),
        "attention": sum(not project["is_ready"] for project in projects),
    }
    project_groups = [
        {
            "namespace": namespace,
            "projects": [project for project in projects if project["namespace"] == namespace],
        }
        for namespace in namespaces
        if any(project["namespace"] == namespace for project in projects)
    ]
    return render_template(
        "dashboard.html",
        summary=summary,
        query=query,
        namespaces=namespaces,
        namespace_filter=namespace_filter,
        project_groups=project_groups,
    )


@app.get("/projects/<project>")
@registry_view
def project_detail(project):
    return render_template("project.html", project=argocd().application(project))


@app.get("/projects/<project>/logs")
@registry_view
def project_logs(project):
    application = argocd().application(project)
    pods = cluster_kubernetes().project_pods(application["resources"])
    return render_template("project-logs.html", project=application, pods=pods)


@app.get("/cluster")
@registry_view
def cluster_status():
    client = prometheus()
    values = {
        "ready_nodes": client.query_value('count(kube_node_status_condition{condition="Ready",status="true"})'),
        "total_nodes": client.query_value("count(kube_node_info)"),
        "active_namespaces": client.query_value('count(kube_namespace_status_phase{phase="Active"})'),
        "cpu_percent": client.query_value(
            '100 * sum(rate(container_cpu_usage_seconds_total{container!="",image!=""}[5m])) / sum(kube_node_status_allocatable{resource="cpu",unit="core"})'
        ),
        "memory_percent": client.query_value(
            '100 * sum(container_memory_working_set_bytes{container!="",image!=""}) / sum(kube_node_status_allocatable{resource="memory",unit="byte"})'
        ),
        "running_pods": client.query_value('sum(kube_pod_status_phase{phase="Running"})'),
        "pending_pods": client.query_value('sum(kube_pod_status_phase{phase="Pending"})'),
        "failed_pods": client.query_value('sum(kube_pod_status_phase{phase="Failed"})'),
        "unknown_pods": client.query_value('sum(kube_pod_status_phase{phase="Unknown"})'),
        "restarts": client.query_value("sum(increase(kube_pod_container_status_restarts_total[1h]))"),
        "unavailable_deployments": client.query_value("sum(kube_deployment_status_replicas_unavailable)"),
        "unavailable_statefulsets": client.query_value(
            "sum(kube_statefulset_status_replicas - kube_statefulset_status_replicas_ready)"
        ),
        "unavailable_daemonsets": client.query_value("sum(kube_daemonset_status_number_unavailable)"),
        "failed_jobs": client.query_value("sum(kube_job_status_failed)"),
        "pending_pvcs": client.query_value('sum(kube_persistentvolumeclaim_status_phase{phase="Pending"})'),
    }
    node_summary = "暂无数据"
    if values["ready_nodes"] is not None and values["total_nodes"] is not None:
        node_summary = f"{format_metric(values['ready_nodes'], 0)} / {format_metric(values['total_nodes'], 0)}"
    not_ready_nodes = None
    if values["ready_nodes"] is not None and values["total_nodes"] is not None:
        not_ready_nodes = max(0, values["total_nodes"] - values["ready_nodes"])
    metric_groups = [
        {
            "title": "节点与资源",
            "metrics": [
                {"label": "Active Namespace", "value": format_metric(values["active_namespaces"], 0), "level": available_level(values["active_namespaces"])},
                {"label": "NotReady 节点", "value": format_metric(not_ready_nodes, 0), "level": count_level(not_ready_nodes)},
                {"label": "CPU 使用率", "value": format_metric(values["cpu_percent"], suffix="%"), "level": percent_level(values["cpu_percent"])},
                {"label": "内存使用率", "value": format_metric(values["memory_percent"], suffix="%"), "level": percent_level(values["memory_percent"])},
            ],
        },
        {
            "title": "Pod 与容器",
            "metrics": [
                {"label": "Running Pod", "value": format_metric(values["running_pods"], 0), "level": available_level(values["running_pods"])},
                {"label": "Pending Pod", "value": format_metric(values["pending_pods"], 0), "level": count_level(values["pending_pods"])},
                {"label": "Failed Pod", "value": format_metric(values["failed_pods"], 0), "level": count_level(values["failed_pods"])},
                {"label": "Unknown Pod", "value": format_metric(values["unknown_pods"], 0), "level": count_level(values["unknown_pods"])},
                {"label": "近 1 小时容器重启", "value": format_metric(values["restarts"], 0), "level": count_level(values["restarts"])},
            ],
        },
        {
            "title": "工作负载与存储",
            "metrics": [
                {"label": "不可用 Deployment", "value": format_metric(values["unavailable_deployments"], 0), "level": count_level(values["unavailable_deployments"])},
                {"label": "未就绪 StatefulSet 副本", "value": format_metric(values["unavailable_statefulsets"], 0), "level": count_level(values["unavailable_statefulsets"])},
                {"label": "不可用 DaemonSet", "value": format_metric(values["unavailable_daemonsets"], 0), "level": count_level(values["unavailable_daemonsets"])},
                {"label": "失败 Job", "value": format_metric(values["failed_jobs"], 0), "level": count_level(values["failed_jobs"])},
                {"label": "Pending PVC", "value": format_metric(values["pending_pvcs"], 0), "level": count_level(values["pending_pvcs"])},
            ],
        },
    ]
    return render_template("cluster.html", node_summary=node_summary, metric_groups=metric_groups, prometheus_url=client.base_url)


@app.get("/cluster/nodes")
@registry_view
def node_list():
    nodes = cluster_kubernetes().nodes()
    apply_node_metrics(nodes, node_metric_vectors(prometheus()))
    nodes.sort(key=lambda node: (node["ready"], node["name"]))
    return render_template("nodes.html", nodes=nodes)


@app.get("/cluster/nodes/<node_name>")
@registry_view
def node_detail(node_name):
    node = cluster_kubernetes().node(node_name)
    apply_node_metrics([node], node_metric_vectors(prometheus()))
    return render_template("node.html", node=node)


@app.get("/logs/<namespace>/<pod>")
@registry_view
def pod_logs(namespace, pod):
    log = cluster_kubernetes().pod_logs(
        namespace,
        pod,
        container=request.args.get("container"),
        tail_lines=log_tail_lines(request.args.get("tail", 500)),
        previous=request.args.get("previous") == "1",
    )
    return render_template("logs.html", log=log, project_name=request.args.get("project", ""))


@app.get("/registry")
@registry_view
def registry_index():
    client = registry()
    repositories = client.repositories()
    query = request.args.get("q", "").strip().lower()
    if query:
        repositories = [repo for repo in repositories if query in repo.lower()]
    return render_template("index.html", repositories=repositories, query=query)


@app.get("/repositories/<path:repository>")
@registry_view
def repository_detail(repository):
    client = registry()
    tag_rows = []
    for tag in client.tags(repository):
        manifest, digest, content_type = client.manifest(repository, tag)
        config = manifest.get("config", {})
        layers = manifest.get("layers", [])
        created_at = image_created(client, repository, manifest)
        tag_rows.append(
            {
                "tag": tag,
                "digest": digest or manifest.get("config", {}).get("digest"),
                "size": (config.get("size") or 0) + sum(layer.get("size") or 0 for layer in layers),
                "layers": len(layers),
                "media_type": content_type or manifest.get("mediaType", ""),
                "created_at": created_at,
                "created_label": format_created(created_at),
            }
        )
    tag_rows.sort(key=lambda item: item["created_at"] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return render_template("repository.html", repository=repository, tags=tag_rows)


@app.get("/repositories/<path:repository>/tags/<tag>")
@registry_view
def tag_detail(repository, tag):
    client = registry()
    manifest, digest, content_type = client.manifest(repository, tag)
    config = manifest.get("config", {})
    layer_size = sum(layer.get("size") or 0 for layer in manifest.get("layers", []))
    total_size = (config.get("size") or 0) + layer_size
    annotations = manifest.get("annotations", {})
    created_at = image_created(client, repository, manifest)
    created = format_created(created_at) if created_at else None
    return render_template(
        "tag.html",
        repository=repository,
        tag=tag,
        manifest=manifest,
        digest=digest,
        content_type=content_type,
        config=config,
        layer_size=layer_size,
        total_size=total_size,
        annotations=annotations,
        created=created,
    )


@app.get("/health")
def health():
    return jsonify(status="ok")


@app.get("/api/repositories")
@registry_view
def api_repositories():
    return jsonify(repositories=registry().repositories())


@app.get("/api/repositories/<path:repository>/tags")
@registry_view
def api_tags(repository):
    return jsonify(repository=repository, tags=registry().tags(repository))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), debug=os.getenv("FLASK_DEBUG") == "1")
