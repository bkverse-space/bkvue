import os
import re
import math
import json
from datetime import datetime, timedelta, timezone
from functools import wraps
from urllib.parse import quote

import requests
from flask import Flask, Response, jsonify, render_template, request, stream_with_context, url_for
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
        operation = status.get("operationState") or {}
        automated_policy = (spec.get("syncPolicy") or {}).get("automated")
        automated = automated_policy is not None
        automated_policy = automated_policy or {}
        deployed_at = (history[0].get("deployedAt") if history else None) or operation.get("finishedAt")
        desired_revision = status.get("sync", {}).get("revision") or "-"
        deployed_revision = (
            operation.get("syncResult", {}).get("revision")
            or (history[0].get("revision") if history else None)
            or "-"
        )
        declared_sources = spec.get("sources") or ([spec["source"]] if spec.get("source") else [])
        sources = [
            {
                "repository": source.get("repoURL") or "未提供仓库地址",
                "path": source.get("path") or source.get("chart") or ".",
                "target_revision": source.get("targetRevision") or "HEAD",
                "kind": "Git 路径" if source.get("path") else ("Helm Chart" if source.get("chart") else "配置源"),
            }
            for source in declared_sources
        ] or [{"repository": "未提供仓库地址", "path": "-", "target_revision": "-", "kind": "未声明"}]
        images = list(dict.fromkeys(status.get("summary", {}).get("images") or []))
        risk_signals = release_risk_signals(sync, health, images, operation)
        release = release_stage(sync, health, operation, automated)
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
            "project": spec.get("project") or "default",
            "namespace": spec.get("destination", {}).get("namespace") or "未指定",
            "sync": sync,
            "health": health,
            "revision": desired_revision,
            "desired_revision": desired_revision,
            "deployed_revision": deployed_revision,
            "sources": sources,
            "deployed_at": format_created(parse_created(deployed_at)) if deployed_at else "暂无发布记录",
            "reconciled_at": format_created(parse_created(status.get("reconciledAt"))) if status.get("reconciledAt") else "暂无记录",
            "operation_phase": operation.get("phase") or "-",
            "operation_message": operation.get("message") or "-",
            "release": release,
            "sync_policy": {
                "automated": automated,
                "prune": bool(automated_policy.get("prune")),
                "self_heal": bool(automated_policy.get("selfHeal")),
            },
            "images": images,
            "image_versions": [image_tag(image) for image in images],
            "risk_signals": risk_signals,
            "has_release_risk": bool(risk_signals),
            "risk_level": "critical" if any(item["level"] == "critical" for item in risk_signals) else "warning" if risk_signals else "normal",
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

    def stream_pod_logs(self, namespace, name, container):
        """Yield a short-lived Kubernetes log follow stream as server-sent events."""
        try:
            pod = self.api.read_namespaced_pod(name, namespace)
        except ApiException as exc:
            message = "未找到该 Pod。" if exc.status == 404 else f"无法读取 Pod: {exc.reason}"
            raise RegistryError(message, exc.status) from exc
        containers = [item.name for item in (pod.spec.containers or [])]
        containers.extend(item.name for item in (pod.spec.init_containers or []))
        if container not in containers:
            raise RegistryError("指定的容器不属于该 Pod。", 400)

        def events():
            response = None
            buffer = ""
            yield "retry: 1000\n\n"
            try:
                response = self.api.read_namespaced_pod_log(
                    name,
                    namespace,
                    container=container,
                    follow=True,
                    tail_lines=0,
                    timestamps=True,
                    _preload_content=False,
                )
                while True:
                    chunk = response.read(4096, decode_content=True)
                    if not chunk:
                        break
                    buffer += chunk.decode("utf-8", errors="replace") if isinstance(chunk, bytes) else chunk
                    while "\n" in buffer:
                        line, buffer = buffer.split("\n", 1)
                        yield f"data: {line}\n\n"
                if buffer:
                    yield f"data: {buffer}\n\n"
            except (ApiException, OSError, ValueError) as exc:
                reason = getattr(exc, "reason", str(exc))
                yield f"event: status\ndata: 无法读取实时日志: {reason}\n\n"
            finally:
                if response:
                    response.close()

        return events()

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

    def workload_versions(self, resources):
        """Read the live Pod templates for Argo-managed workloads."""
        readers = {
            "Deployment": self.apps_api.read_namespaced_deployment,
            "StatefulSet": self.apps_api.read_namespaced_stateful_set,
            "DaemonSet": self.apps_api.read_namespaced_daemon_set,
            "Job": self.batch_api.read_namespaced_job,
        }
        versions = []
        for resource in resources:
            kind = resource.get("kind")
            namespace = resource.get("namespace")
            name = resource.get("name")
            reader = readers.get(kind)
            if not reader or not namespace or not name:
                continue
            try:
                workload = reader(name, namespace)
            except ApiException as exc:
                if exc.status == 404:
                    continue
                raise RegistryError(f"无法读取 {kind}/{name} 的当前镜像: {exc.reason}", exc.status) from exc

            pod_spec = workload.spec.template.spec
            for container in pod_spec.containers or []:
                versions.append(self._workload_version(resource, container.name, container.image, False))
            for container in pod_spec.init_containers or []:
                versions.append(self._workload_version(resource, container.name, container.image, True))
        return sorted(versions, key=lambda item: (item["namespace"], item["kind"], item["name"], item["container"]))

    def resource_events(self, resources):
        """Return recent Events keyed by the Argo resource they belong to."""
        references = {}
        events_by_resource = {}
        for resource in resources:
            namespace = resource.get("namespace")
            kind = resource.get("kind")
            name = resource.get("name")
            if namespace and namespace != "-" and kind and name:
                resource_key = self._resource_event_key(namespace, kind, name)
                events_by_resource[resource_key] = []
                references.setdefault(namespace, {}).setdefault((kind, name), set()).add(resource_key)

        for pod in self.project_pods(resources):
            pod_key = self._resource_event_key(pod["namespace"], "Pod", pod["name"])
            targets = {pod_key} if pod_key in events_by_resource else set()
            targets.update(
                self._resource_event_key(pod["namespace"], *workload.split("/", 1))
                for workload in pod["workloads"]
                if self._resource_event_key(pod["namespace"], *workload.split("/", 1)) in events_by_resource
            )
            if targets:
                references.setdefault(pod["namespace"], {}).setdefault(("Pod", pod["name"]), set()).update(targets)

        for namespace, resource_refs in references.items():
            try:
                events = self.api.list_namespaced_event(namespace, limit=500).items
            except ApiException as exc:
                raise RegistryError(f"无法读取 Namespace {namespace} 的事件: {exc.reason}", exc.status) from exc
            for event in events:
                involved = getattr(event, "involved_object", None) or getattr(event, "regarding", None)
                targets = resource_refs.get((involved.kind, involved.name)) if involved else None
                if not targets:
                    continue
                occurred_at = (
                    getattr(event, "event_time", None)
                    or getattr(event, "last_timestamp", None)
                    or getattr(event, "deprecated_last_timestamp", None)
                    or (event.series.last_observed_time if getattr(event, "series", None) else None)
                    or event.metadata.creation_timestamp
                )
                count = (
                    (event.series.count if getattr(event, "series", None) else None)
                    or getattr(event, "count", None)
                    or getattr(event, "deprecated_count", None)
                    or 1
                )
                source = (
                    getattr(event, "reporting_controller", None)
                    or (event.source.component if getattr(event, "source", None) else None)
                    or (event.deprecated_source.component if getattr(event, "deprecated_source", None) else None)
                    or "-"
                )
                event_summary = {
                    "occurred_at": format_created(occurred_at) if occurred_at else "时间未知",
                    "sort_timestamp": occurred_at.timestamp() if occurred_at else 0,
                    "type": getattr(event, "type", None) or "Normal",
                    "reason": getattr(event, "reason", None) or "-",
                    "message": getattr(event, "message", None) or getattr(event, "note", None) or "-",
                    "source": source,
                    "count": count,
                }
                for target in targets:
                    events_by_resource[target].append(event_summary.copy())

        for events in events_by_resource.values():
            events.sort(key=lambda item: item["sort_timestamp"], reverse=True)
            for event in events:
                event.pop("sort_timestamp")
        return events_by_resource

    @staticmethod
    def _resource_event_key(namespace, kind, name):
        return f"{namespace}|{kind}|{name}"

    @staticmethod
    def _workload_version(resource, container, image, init_container):
        return {
            "kind": resource["kind"],
            "name": resource["name"],
            "namespace": resource["namespace"],
            "container": f"init/{container}" if init_container else container,
            "image": image or "-",
            "version": image_tag(image) if image else "未标记",
            "sync": resource["sync"],
            "health": resource["health"],
        }

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
            app.logger.warning(
                "data source request failed method=%s path=%s status=%s error=%s",
                request.method,
                request.path,
                exc.status_code,
                exc,
            )
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


def release_risk_signals(sync, health, images, operation):
    """Describe only signals that can be verified from the Application status."""
    signals = []
    if sync != "Synced":
        signals.append({"label": f"同步状态 {sync}", "level": "critical" if sync == "OutOfSync" else "warning"})
    if health != "Healthy":
        signals.append({"label": f"健康状态 {health}", "level": "critical" if health == "Degraded" else "warning"})
    operation_phase = operation.get("phase")
    if operation_phase in {"Error", "Failed"}:
        signals.append({"label": f"最近操作 {operation_phase}", "level": "critical"})
    if not images:
        signals.append({"label": "未发现当前镜像", "level": "warning"})
    elif any(image_tag(image).lower() == "latest" for image in images):
        signals.append({"label": "使用 latest 镜像标签", "level": "warning"})
    return signals


def release_stage(sync, health, operation, automated):
    phase = operation.get("phase")
    if phase == "Running":
        return {"key": "syncing", "label": "同步中", "level": "progressing", "description": "Argo CD 正在执行同步操作。"}
    if phase in {"Failed", "Error"}:
        return {"key": "failed", "label": "发布异常", "level": "degraded", "description": "最近一次 Argo CD 同步操作失败。"}
    if sync != "Synced":
        if automated:
            return {"key": "waiting_auto_sync", "label": "等待自动同步", "level": "progressing", "description": "Argo CD 已发现配置差异，自动同步策略已启用。"}
        return {"key": "pending", "label": "配置未同步", "level": "warning", "description": "Argo CD 已发现配置差异，但未启用自动同步。"}
    if health != "Healthy":
        return {"key": "verifying", "label": "等待健康", "level": "warning", "description": "同步状态已一致，正在等待资源达到健康状态。"}
    return {"key": "completed", "label": "发布完成", "level": "healthy", "description": "配置已同步，Application 当前健康。"}


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
    project_filter = request.args.get("project", "").strip()
    argocd_projects = sorted({project["project"] for project in projects})
    if query:
        projects = [
            project
            for project in projects
            if query in project["name"].lower()
            or query in project["project"].lower()
            or query in project["namespace"].lower()
        ]
    if project_filter:
        projects = [project for project in projects if project["project"] == project_filter]
    projects.sort(key=lambda project: (project["is_ready"], project["name"]))
    summary = {
        "total": len(projects),
        "ready": sum(project["is_ready"] for project in projects),
        "attention": sum(not project["is_ready"] for project in projects),
    }
    project_groups = [
        {
            "project": argocd_project,
            "projects": [project for project in projects if project["project"] == argocd_project],
        }
        for argocd_project in argocd_projects
        if any(project["project"] == argocd_project for project in projects)
    ]
    return render_template(
        "dashboard.html",
        summary=summary,
        query=query,
        argocd_projects=argocd_projects,
        project_filter=project_filter,
        project_groups=project_groups,
    )


@app.get("/risks")
@registry_view
def risk_assessment():
    projects = argocd().applications()
    query = request.args.get("q", "").strip().lower()
    project_filter = request.args.get("project", "").strip()
    argocd_projects = sorted({project["project"] for project in projects})
    if query:
        projects = [
            project
            for project in projects
            if query in project["name"].lower()
            or query in project["project"].lower()
            or query in project["namespace"].lower()
            or any(query in signal["label"].lower() for signal in project["risk_signals"])
        ]
    if project_filter:
        projects = [project for project in projects if project["project"] == project_filter]
    risks = [project for project in projects if project["has_release_risk"]]
    risks.sort(key=lambda project: (project["risk_level"] != "critical", project["project"], project["name"]))
    return render_template(
        "risks.html",
        risks=risks,
        query=query,
        project_filter=project_filter,
        argocd_projects=argocd_projects,
        summary={
            "total": len(projects),
            "risk": len(risks),
            "critical": sum(project["risk_level"] == "critical" for project in risks),
            "warning": sum(project["risk_level"] == "warning" for project in risks),
        },
    )


@app.get("/sources")
@registry_view
def configuration_sources():
    query = request.args.get("q", "").strip().lower()
    applications = argocd().applications()
    source_rows = []
    for application in applications:
        for source in application["sources"]:
            row = {**source, "application": application}
            if query and not any(
                query in value.lower()
                for value in (
                    application["name"],
                    application["project"],
                    application["namespace"],
                    source["repository"],
                    source["path"],
                )
            ):
                continue
            source_rows.append(row)
    source_rows.sort(key=lambda row: (row["repository"], row["path"], row["application"]["name"]))
    repositories = []
    for row in source_rows:
        if not repositories or repositories[-1]["repository"] != row["repository"]:
            repositories.append({"repository": row["repository"], "sources": []})
        repositories[-1]["sources"].append(row)
    return render_template(
        "sources.html",
        query=query,
        repositories=repositories,
        summary={
            "repositories": len(repositories),
            "applications": len({row["application"]["name"] for row in source_rows}),
            "out_of_sync": sum(row["application"]["sync"] != "Synced" for row in source_rows),
            "unhealthy": sum(row["application"]["health"] != "Healthy" for row in source_rows),
        },
    )


@app.get("/releases")
@registry_view
def release_control():
    query = request.args.get("q", "").strip().lower()
    applications = argocd().applications()
    if query:
        applications = [
            application
            for application in applications
            if query in application["name"].lower()
            or query in application["project"].lower()
            or query in application["namespace"].lower()
            or query in application["release"]["label"].lower()
        ]
    stage_order = {"failed": 0, "pending": 1, "waiting_auto_sync": 2, "syncing": 3, "verifying": 4, "completed": 5}
    applications.sort(key=lambda application: (stage_order[application["release"]["key"]], application["name"]))
    return render_template(
        "releases.html",
        applications=applications,
        query=query,
        summary={
            "total": len(applications),
            "waiting": sum(application["release"]["key"] in {"pending", "waiting_auto_sync"} for application in applications),
            "syncing": sum(application["release"]["key"] == "syncing" for application in applications),
            "attention": sum(application["release"]["key"] in {"failed", "verifying"} for application in applications),
        },
    )


@app.get("/releases/<project>")
@registry_view
def release_detail(project):
    application = argocd().application(project)
    return render_template("release.html", project=application)


@app.get("/projects/<project>")
@registry_view
def project_detail(project):
    application = argocd().application(project)
    client = cluster_kubernetes()
    application["workload_versions"] = client.workload_versions(application["resources"])
    application["version_tags"] = sorted({item["version"] for item in application["workload_versions"]})
    events_by_resource = client.resource_events(application["resources"])
    for resource in application["resources"]:
        resource["events"] = events_by_resource.get(
            client._resource_event_key(resource["namespace"], resource["kind"], resource["name"]), []
        )
    return render_template("project.html", project=application)


@app.get("/projects/<project>/logs")
@registry_view
def project_logs(project):
    application = argocd().application(project)
    client = cluster_kubernetes()
    workload_options = [
        {
            "target": f"{resource['namespace']}|{resource['kind']}|{resource['name']}",
            "label": f"{resource['kind']}/{resource['name']}",
        }
        for resource in application["resources"]
        if resource["kind"] in {"Deployment", "StatefulSet", "DaemonSet", "Job"}
    ]
    target = request.args.get("target") or (workload_options[0]["target"] if workload_options else "")
    if target not in {option["target"] for option in workload_options}:
        target = workload_options[0]["target"] if workload_options else ""
    pod_options = []
    if target:
        namespace, kind, workload_name = target.split("|", 2)
        workload_ref = f"{kind}/{workload_name}"
        selected_resources = [
            resource
            for resource in application["resources"]
            if resource["namespace"] == namespace and resource["kind"] == kind and resource["name"] == workload_name
        ]
        pods = client.project_pods(selected_resources)
        pod_options = [
        {
            "target": f"{pod['namespace']}|{pod['name']}",
            "label": f"{pod['namespace']}/{pod['name']}",
        }
        for pod in pods
        if pod["namespace"] == namespace and workload_ref in pod["workloads"]
    ]
    pod_target = request.args.get("pod") or (pod_options[0]["target"] if pod_options else "")
    if pod_target not in {option["target"] for option in pod_options}:
        pod_target = pod_options[0]["target"] if pod_options else ""
    log = None
    if pod_target:
        namespace, name = pod_target.split("|", 1)
        log = client.pod_logs(
            namespace,
            name,
            container=request.args.get("container"),
            tail_lines=log_tail_lines(request.args.get("tail", 500)),
            previous=request.args.get("previous") == "1",
        )
    return render_template(
        "project-logs.html",
        project=application,
        target=target,
        pod_target=pod_target,
        workload_options=workload_options,
        pod_options=pod_options,
        log=log,
    )


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


@app.get("/traffic")
def traffic_topology():
    return render_template("traffic.html")


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


@app.get("/logs/<namespace>/<pod>/stream")
@registry_view
def pod_log_stream(namespace, pod):
    if request.args.get("previous") == "1":
        raise RegistryError("前一实例日志不支持实时跟随。", 400)
    events = cluster_kubernetes().stream_pod_logs(namespace, pod, request.args.get("container", ""))
    return Response(
        stream_with_context(events),
        content_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


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
