# bkvue

`bkvue` 是面向单 Kubernetes 集群的只读发布信息工作台。每个 Argo CD `Application` 对应一个项目，工作台从集群内的 Argo CD Application CR 读取当前发布状态，不连接 Git 平台，也不会执行同步、回滚、删除或任何配置变更。

## 功能

- 项目概览：同步状态、健康状态、目标 Namespace、当前 Revision 与最近发布时间
- 项目详情：当前镜像、工作负载镜像对比、Kubernetes 事件、发布历史与 Argo CD Application 状态
- 风险评估：集中展示同步、健康、操作状态与镜像标签带来的发布风险
- 集群总览：Prometheus 提供的节点、资源使用率和工作负载异常指标
- 节点详情：节点 Condition、容量、实时利用率和调度 Pod
- 工作负载日志：选择 Deployment、StatefulSet、DaemonSet、Job 后，再选择其下属 Pod 和容器查看日志；默认实时跟随并定位到日志底部，支持最近 100/500/1000/5000 行与前一实例日志
- 镜像仓库：浏览 Docker Registry 的仓库、tag、镜像大小、镜像层和构建时间
- 全部时间以 UTC+8 显示

## 数据来源

| 信息 | 来源 |
| --- | --- |
| 项目、目标 Namespace、同步状态、健康状态、Revision、发布历史、当前镜像 | Argo CD `Application` CR |
| 镜像 tag、大小、镜像层、构建时间 | Docker Registry HTTP API v2 |
| 集群节点、资源、Pod 与 Deployment 状态 | Prometheus HTTP API |

工作台以 `ARGOCD_NAMESPACE` 中的每个 Application 作为一个项目。目标 Namespace 取自 `spec.destination.namespace`。

## Kubernetes 权限

[k8s/rbac.yaml](k8s/rbac.yaml) 只包含 ServiceAccount 与只读 RBAC，不包含 Deployment、Service、Ingress 或 Namespace。请先创建部署所在的 namespace，然后按实际 namespace 修改清单中的 ServiceAccount 与 RoleBinding subject，最后应用：

```bash
kubectl apply -f k8s/rbac.yaml
```

工作负载须设置 `serviceAccountName: bkvue`。该 ServiceAccount 在 `argocd` namespace 拥有 `applications.argoproj.io` 的 `get/list/watch` 权限，并通过 ClusterRole 拥有 `nodes`、`pods`、`events`、Deployment、ReplicaSet、StatefulSet、DaemonSet、Job 的 `get/list/watch` 与 `pods/log` 的 `get` 权限。这些权限均为只读。若 Argo CD 不在 `argocd` namespace，请同时修改 Role、RoleBinding 与 `ARGOCD_NAMESPACE`。

默认 Prometheus 地址为 `http://prometheus-server.monitoring.svc:9090`。请按实际 Service 地址修改 `PROMETHEUS_URL`。如果 Prometheus 启用认证，将 Bearer Token 以环境变量或 Kubernetes Secret 注入 `PROMETHEUS_BEARER_TOKEN`；工作台不会写入 Prometheus。

## CI 与镜像发布

GitHub Actions 会在 Pull Request 与 `main` 推送时校验 Python 语法、Docker Compose 配置并构建镜像。推送形如 `v0.1.1` 的 Git tag 时，会将镜像发布到 `ghcr.io/bkverse-space/bkvue`，标签为该 Git tag 与 `latest`。

历史 tag 可从 Actions 页面通过 `workflow_dispatch` 手动补发。

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

正式部署时，请在你的 Deployment、Argo CD Application 或其他部署定义中设置 `serviceAccountName: bkvue`。

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
