# bkvue

Read-only Kubernetes release workbench backed by Argo CD Application CRs, Docker Registry, and Prometheus.

## Install

```bash
helm upgrade --install bkvue ./charts/bkvue \
  --namespace bkvue \
  --create-namespace \
  --set image.repository=registry.example.com/bkvue \
  --set image.tag=1.0.0 \
  --set registry.url=https://registry.example.com \
  --set prometheus.url=http://prometheus-server.monitoring.svc:9090
```

The Helm identity needs permission to create a Role and RoleBinding in `argocd` (or the namespace configured by `argocd.namespace`), plus a ClusterRole and ClusterRoleBinding when `rbac.readNodesAndPods=true`.

## Authentication Secrets

Registry Basic Auth can reference an existing Secret:

```yaml
registry:
  auth:
    existingSecret: registry-credentials
    usernameKey: username
    passwordKey: password
```

Prometheus Bearer Auth can reference an existing Secret:

```yaml
prometheus:
  auth:
    existingSecret: prometheus-token
    tokenKey: token
```

## Read-only RBAC

The chart grants only `get`, `list`, and `watch` verbs:

- Argo CD `applications.argoproj.io` in the configured Argo CD namespace
- Cluster-scoped `nodes` and `pods` when `rbac.readNodesAndPods=true`
- The `pods/log` subresource with `get` only

Set `rbac.readNodesAndPods=false` when node and pod detail pages are not required.
