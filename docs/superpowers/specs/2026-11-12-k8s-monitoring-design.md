# StreamPark Kubernetes 监控功能设计

日期：2026-11-12
分支：feature/k8s
状态：已确认（用户授权自主推进）

## 1. 背景与目标

用户将使用 K8S 运行 Flink，希望在 StreamPark 上追加：

1. **K8S 集群运行情况查看与监控**：节点健康、资源用量、工作负载、事件。
2. **运行在 K8S 上的 Flink 应用查看与监控**：作业状态 + 所占 K8S 资源。

数据策略：
- **实时值**：K8S API 直连（必选，零外部依赖）。
- **历史趋势**：Prometheus 集成为**可选**配置（不配则只看实时值）。
- **多集群**：支持注册管理多个 K8S 集群。

### 非目标（本期不做）

- 告警规则与通知
- Flink 应用详情页内嵌 K8S 资源视图（二期）
- Grafana iframe/跳转集成
- 集群级 RBAC/多租户隔离

## 2. 关键选型

| 决策点 | 选择 | 理由 |
|---|---|---|
| K8S 客户端 | console-service 引入独立 `io.fabric8:kubernetes-client` | API 完整（Nodes/Pods/Deployments/Events/metrics）；Flink shaded fabric8 包名为 `org.apache.flink.kubernetes.shaded.io.fabric8`，两者共存不冲突 |
| 实时资源用量 | metrics.k8s.io（metrics-server 标准接口） | 零额外部署（K8S 环境通常已装）；未装时优雅降级 |
| 历史趋势 | Prometheus HTTP API（`/api/v1/query`、`/api/v1/query_range`） | 后端封装 PromQL 模板，前端 echarts 渲染；可选项 |
| 前端图表 | 引入 echarts | 当前项目无图表库 |

## 3. 整体架构

```
前端 (Vue3 + ant-design-vue 3 + echarts)
  │  /k8s/cluster/*   /k8s/monitor/*
  ▼
K8sClusterController ──► K8sClusterService ──► t_k8s_cluster (MyBatis-Plus)
K8sMonitorController ──► K8sMonitorService ──► K8sClientManager (fabric8, Caffeine 按 clusterId 缓存)
                                  │
                                  ├──► K8S API: nodes/pods/deployments/events（状态）
                                  ├──► metrics.k8s.io: 节点/Pod 用量（实时值）
                                  └──► K8sPrometheusClient（可选）: 历史趋势
```

模块归属：
- 后端代码全部放在 `streampark-console/streampark-console-service`（`org.apache.streampark.console.kubernetes` 包），仿照 `core` 包的 Controller/Assembler/Service/ServiceImpl/Mapper 分层。
- 前端放在 `streampark-console/streampark-console-webapp/src/views/kubernetes/`。

## 4. 数据模型

新表 `t_k8s_cluster`（仿 `t_yarn_queue`）：

| 字段 | 类型 | 说明 |
|---|---|---|
| id | bigint PK | 主键 |
| cluster_name | varchar(128) | 集群显示名，唯一 |
| kubeconfig | text | kubeconfig 文件**内容**（非路径） |
| prometheus_url | varchar(255) 可空 | 如 `http://prometheus:9090`；空 = 只看实时 |
| description | varchar(255) 可空 | 备注 |
| create_time | datetime | 创建时间 |
| modify_time | datetime | 修改时间 |

Schema 变更清单（全部同步修改）：
- `streampark-console-service/src/main/assembly/script/schema/mysql-schema.sql`
- `streampark-console-service/src/main/assembly/script/schema/pgsql-schema.sql`
- `streampark-console-service/src/main/resources/db/schema-h2.sql`（测试库）

说明：当前 revision 为 3.0.0（架构基线尚未发布），无需新增 upgrade/{mysql,pgsql}/x.y.z.sql 升级脚本（该惯例仅针对已发布版本间的升级）；`sql-rev.dict` 为 SQL 编辑器补全词典，与本变更无关。

Entity/Mapper：`K8sCluster` 实体 + `K8sClusterMapper`（MyBatis-Plus BaseMapper，无复杂 SQL 可不用 XML）。

## 5. 后端 API 设计

### 5.1 集群管理 `K8sClusterController`（`/k8s/cluster`）

| 接口 | 说明 | 权限码 |
|---|---|---|
| POST `/create` | 创建集群注册 | `k8sCluster:create` |
| POST `/update` | 更新 | `k8sCluster:update` |
| POST `/delete` | 删除 | `k8sCluster:delete` |
| GET `/list` | 列表（分页，仿 YarnQueueService 分页） | `k8sCluster:view` |
| GET `/get?id=` | 详情 | `k8sCluster:view` |
| POST `/test` | 测试连接（请求体含 kubeconfig），返回 `{connected, version, message}` | `k8sCluster:view` |

鉴权沿用项目现有 `@RequiresPermissions` + Shiro 体系。

### 5.2 监控查询 `K8sMonitorController`（`/k8s/monitor`，均带 `clusterId` 参数）

| 接口 | 返回 |
|---|---|
| GET `/overview` | 节点 ready/total、CPU/内存 allocatable 与 usage 汇总、Pod 状态分布、namespace 数、近期 Warning 事件计数 |
| GET `/nodes` | 节点列表：名称/角色/版本/状态/内部 IP/Pod 数/CPU 与内存 allocatable·usage·usage率 |
| GET `/node?name=` | 单节点详情（conditions、labels、taints） |
| GET `/workloads?namespace=&keyword=` | Deployment 列表：名称/namespace/副本 ready·desired/镜像/创建时间/`streamParkManaged` 标注 + Pod 列表：名称/namespace/节点/状态/重启次数/CPU·内存实时用量/IP/创建时间 |
| GET `/events?namespace=&limit=` | 近期事件（type/reason/object/message/count/lastSeen），Warning 优先 |
| GET `/namespaces` | namespace 列表（供筛选下拉） |
| GET `/history` | Prometheus 趋势代理：参数 `clusterId, kind(node|pod|cluster), resource(cpu|memory), name, range(1h|6h|24h|7d)`；未配置 prometheus_url 返回 `PROMETHEUS_NOT_CONFIGURED` 业务码，前端据此隐藏趋势入口 |

`streamParkManaged` 标注逻辑：Deployment 名 == 某 `FlinkApplication.jobName` 且 namespace 匹配该应用的 `k8sNamespace` → 返回 `appId/appName`，前端高亮并显示应用名。

前端判断「是否已配置 Prometheus」：以 `/k8s/cluster/list` 返回的 `prometheus_url` 非空为准，据此显示/隐藏趋势入口。

### 5.3 核心类

| 类 | 职责 |
|---|---|
| `K8sClientManager` | 按 clusterId 用 Caffeine 缓存 fabric8 `KubernetesClient`；kubeconfig 内容解析为 `Config`；集群更新/删除时 invalidate |
| `K8sMonitorService` | 聚合 K8S API + metrics API 查询，组装 VO |
| `K8sPrometheusClient` | HTTP 调用 Prometheus（复用项目现有 HTTP 工具），PromQL 模板：`node_cpu_seconds_total`、`node_memory_*`、`kube_pod_container_resource_requests`（依赖 kube-state-metrics）、`container_*` 等 |
| `FlinkAppK8sMatcher` | 查询 `FlinkApplicationMapper` 建立 `(namespace, jobName) → appId/appName` 映射，一次查询避免 N+1 |

## 6. 前端设计

### 6.1 页面结构

1. **集群管理**（设置菜单下）：`views/kubernetes/cluster/{View.vue, ClusterModal.vue, index.data.ts}` —— 表格 + 新建/编辑弹窗（kubeconfig 用 textarea，Prometheus 地址可选）+ 测试连接按钮。
2. **K8S 监控**（一级菜单 `views/kubernetes/monitor/index.vue`）：
   - 顶部：集群切换 Select + 自动刷新开关（10s，可暂停）+ 刷新时间
   - Tab 总览：统计卡（节点 Ready、运行 Pod 数、CPU 分配率环形图、内存分配率环形图、Warning 事件数）+ Warning 事件摘要表
   - Tab 节点：节点表格；行点击开抽屉 —— 实时状态 + 历史趋势（CPU/内存折线，echarts；未配 Prometheus 时显示引导文案）
   - Tab 工作负载：Deployment 表格（StreamPark 管理行高亮 + 应用名标签）；Pod 表格（支持 namespace/关键字筛选）
   - Tab Flink 应用：复用现有 watcher 数据的 K8S Flink 应用清单（状态/槽位/checkpoint）+ 对应 Pod 实时资源

### 6.2 工程改动

- `package.json` 新增 `echarts` 依赖
- `api/kubernetes/{cluster.ts, monitor.ts}`
- `t_menu` 插入菜单（K8S 集群管理 + K8S 监控）与权限码（k8sCluster:*)，同步 `data.sql`（mysql/pgsql）与 i18n `src/locales/lang/{zh-CN,en}/menu.ts`
- 路由由后端 t_menu 驱动（PermissionModeEnum.BACK），组件路径按现有 glob 约定

## 7. 错误处理

| 场景 | 处理 |
|---|---|
| kubeconfig 无效/不可达 | `/test` 返回具体错误消息；监控接口返回统一错误码 + 消息，前端 Message 提示 |
| metrics-server 未安装 | metrics 查询失败时降级：状态列正常、用量列显示 `—`，页面顶部一次性提示「metrics-server 未安装，无法展示资源用量」 |
| Prometheus 未配置 | 前端隐藏趋势抽屉/入口 |
| Prometheus 配置但不可达 | 趋势区显示错误态与重试按钮 |
| client 失效 | K8sClientManager 捕获连接异常时 invalidate 缓存并重建一次 |

## 8. 测试

- 后端单测：fabric8 `kubernetes-server-mock`（mockcrd/server 模拟 API Server）覆盖 K8sMonitorService 各查询组装；MockWebServer 覆盖 K8sPrometheusClient（正常/超时/未配置）；K8sClusterService CRUD 用 H2。
- 前端：项目无测试基建，按现状手动验证（含降级场景：无 metrics-server、无 Prometheus）。

## 9. 分期

- **本期（一次交付）**：集群注册管理、K8S 实时监控四 Tab、可选 Prometheus 趋势、StreamPark Flink 应用标注。
- **后续可选**：Flink 应用详情页 K8S 资源增强、告警规则、Grafana 跳转。
