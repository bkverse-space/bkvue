# bkvue

`bkvue` 是面向单 Kubernetes 集群的只读发布信息工作台。每个 Argo CD `Application` 对应一个项目，工作台从集群内的 Argo CD Application CR 读取当前发布状态，不连接 Git 平台，也不会执行同步、回滚、删除或任何配置变更。

## 功能

- 项目概览：同步状态、健康状态、目标 Namespace、当前 Revision 与最近发布时间
- 项目详情：当前镜像、发布历史与 Argo CD Application 状态
- 集群总览：Prometheus 提供的节点、资源使用率和工作负载异常指标
- 节点详情：节点 Condition、容量、实时利用率和调度 Pod
- 工作负载日志：直接查看 Deployment、StatefulSet、DaemonSet、Job 的当前 Pod 集合，按 Pod 切换；也支持单个 Pod 的容器、最近 100/500/1000/5000 行与前一实例日志
- 镜像仓库：浏览 Docker Registry 的仓库、tag、镜像大小、镜像层和构建时间
- 全部时间以 UTC+8 显示

## 数据来源

| 信息 | 来源 |
| --- | --- |
| 项目、目标 Namespace、同步状态、健康状态、Revision、发布历史、当前镜像 | Argo CD `Application` CR |
| 镜像 tag、大小、镜像层、构建时间 | Docker Registry HTTP API v2 |
| 集群节点、资源、Pod 与 Deployment 状态 | Prometheus HTTP API |

工作台以 `ARGOCD_NAMESPACE` 中的每个 Application 作为一个项目。目标 Namespace 取自 `spec.destination.namespace`。

## 集群部署

先构建并推送镜像到你的镜像仓库，再修改 [k8s/workbench.yaml](k8s/workbench.yaml) 的 `image` 和 `REGISTRY_URL`：

```bash
docker build -t registry.example.com/bkvue:1.0.0 .
docker push registry.example.com/bkvue:1.0.0
kubectl apply -f k8s/workbench.yaml
```

清单创建的 ServiceAccount 在 `argocd` namespace 拥有 `applications.argoproj.io` 的 `get/list/watch` 权限，并通过 ClusterRole 拥有 `nodes`、`pods`、Deployment、ReplicaSet、StatefulSet、DaemonSet、Job 的 `get/list/watch` 与 `pods/log` 的 `get` 权限。这些权限均为只读。若 Argo CD 不在 `argocd` namespace，请同时修改 Role、RoleBinding 与 `ARGOCD_NAMESPACE`。

默认 Prometheus 地址为 `http://prometheus-server.monitoring.svc:9090`。请按实际 Service 地址修改 `PROMETHEUS_URL`。如果 Prometheus 启用认证，将 Bearer Token 以环境变量或 Kubernetes Secret 注入 `PROMETHEUS_BEARER_TOKEN`；工作台不会写入 Prometheus。

## Helm 部署

Chart 位于 [charts/bkvue](charts/bkvue)。示例：

```bash
helm upgrade --install bkvue ./charts/bkvue \
  --namespace bkvue \
  --create-namespace \
  --set image.repository=registry.example.com/bkvue \
  --set image.tag=1.0.0 \
  --set registry.url=https://registry.example.com \
  --set prometheus.url=http://prometheus-server.monitoring.svc:9090
```

详见 Chart 的 [README](charts/bkvue/README.md)。

## 本地运行

本地运行时应用使用当前 kubeconfig 读取 Application：

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
ARGOCD_NAMESPACE=argocd REGISTRY_URL=http://localhost:5000 python app.py
```

也可以使用 Docker Compose：

```bash
cp .env.example .env
docker compose up --build
```

Docker Compose 不具备集群内 ServiceAccount 身份。若需要本地读取 Argo CD，使用覆盖文件映射 kubeconfig：

```bash
KUBECONFIG_PATH=/absolute/path/to/kubeconfig docker compose -f docker-compose.yml -f docker-compose.kubeconfig.yml up --build
```

正式部署请使用 `k8s/workbench.yaml`，它会自动使用 Pod 的 ServiceAccount。

## 环境变量

| 变量 | 默认值 | 用途 |
| --- | --- | --- |
| `ARGOCD_NAMESPACE` | `argocd` | Argo CD Application CR 所在 namespace |
| `PROMETHEUS_URL` | `http://prometheus-server.monitoring.svc:9090` | Prometheus HTTP API 地址 |
| `PROMETHEUS_BEARER_TOKEN` | 空 | 可选的 Prometheus 只读 Bearer Token |
| `PROMETHEUS_VERIFY_TLS` | `true` | Prometheus TLS 证书校验 |
| `REGISTRY_URL` | `http://localhost:5000` | Docker Registry v2 地址 |
| `REGISTRY_USERNAME` / `REGISTRY_PASSWORD` | 空 | Registry Basic Auth 凭据 |
| `REGISTRY_VERIFY_TLS` | `true` | 开发环境自签证书可设置为 `false` |
| `REGISTRY_TIMEOUT` | `15` | Registry 请求超时秒数 |
| `SECRET_KEY` | 开发值 | 生产环境请设置随机长值 |
