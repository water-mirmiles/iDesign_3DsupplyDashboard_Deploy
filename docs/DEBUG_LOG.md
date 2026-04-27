# Supply3D 技术复盘：核心坑位与 Root Cause

> 本文档记录 V1.0 研发过程中反复出现的四类问题及其工程化解决方案，供部署与二次开发对照。

---

## 1. 路径问题：Mac 与 Linux 绝对路径不一致 → 资产 404

### 现象
- `GET /api/asset-details` 能返回 `physicalPath` 与文件大小，但浏览器请求 `/storage/assets/...` 返回 **404**。
- 或列表与弹窗对同一编号结论不一致（一端能扫到文件、一端不能）。

### Root Cause
1. **进程工作目录 (`process.cwd`) 与仓库结构**：在仓根启动与在 `server/` 目录启动时，`server/storage` 的解析方式不同；若混用 `__dirname` 与 `cwd` 或缓存 JSON 中写入本机绝对路径，换机部署后静态托管根目录与扫描目录**错位**。
2. **前端静态资源基址**：生产构建后 `getStorageBaseUrl` 若仍默认 `http://localhost:3001`，浏览器会向**用户本机**拉取 `/storage`，而非部署服务器，表现为「接口 OK、预览 404」。
3. **弹窗 API 基址**：清单页使用同源 `/api/inventory-real`，弹窗若硬编码 `localhost:3001` 请求 `asset-details`，则列表与弹窗命中**两套后端/两套数据**。

### 解决方案（已实现方向）
- 后端统一 **`STORAGE_ROOT`**（按 `cwd` 解析 `server/storage` 或 `server` 下的 `storage`），并对静态托管使用 **`path.resolve` 后的绝对路径** `ABS_STORAGE_ROOT`，启动打印 `[Static-Check]` 便于对账。
- 缓存写入前做**路径可移植化**（避免把 `/Users/...` 或 `/root/...` 写入 JSON）。
- 生产环境前端：**同源相对路径**请求 API 与 `/storage`（`import.meta.env.PROD` 下 `API_BASE`、`getStorageBaseUrl` 等与当前站点对齐）。

---

## 2. 编码问题：文件名归一化 → 物理对账失败

### 现象
- 磁盘上存在 `L-B20067M.obj`，列表或弹窗却报「未匹配 / Not Found」。
- 同款编号在 Excel 与文件名中分别使用**长横杠、短横杠、Unicode 连字符**或夹带**不可见空白**。

### Root Cause
- 物理扫描与清单对账使用**字符串字面量**比较，未对文件名与业务编号做同一套规范化。
- Mac 拷贝资产时可能产生 **`._*` 影子文件**，干扰「最近邻」诊断或占用目录遍历注意力（虽不一定命中主键，但增加误判成本）。

### 解决方案（已实现方向）
- 后端统一 **`normalizeAssetCodeKey`**：`NFKC`、小写、多类横杠统一为 `-`、剥离空白与不可见字符、去扩展名后再比较。
- 扫描目录时 **跳过 `._` 前缀文件**。
- 扩展名比较使用**小写归一**（`.OBJ` / `.obj` 等价）。

---

## 3. API 稳定性：AI Studio → Vertex AI 迁移中的限制与 404

### 现象
- 映射/对话链路间歇失败：配额、限流、或返回 **模型不存在 / 404**。
- 不同环境（密钥、项目 ID、区域、`GOOGLE_APPLICATION_CREDENTIALS`）下行为不一致。

### Root Cause
1. **QPS / 并发**：Vertex 与企业配额对推理请求有速率与并发限制，高峰时触发 **429** 或空响应。
2. **模型别名与区域**：`gemini-*` 模型名、区域（location）与当前项目启用模型列表不一致时，SDK 报 **404**。
3. **凭证路径**：容器或 Linux 上 JSON 密钥路径错误时，认证失败表现为**模糊错误**而非明确「文件未找到」。

### 解决方案（工程侧）
- 服务端集中初始化 **Vertex AI**，模型名使用**可配置/锁定清单**，启动时尽可能 **list models** 或记录当前锁定模型，便于日志对齐。
- 对映射与 AI 调用增加 **重试/退避**（若已实现）与清晰错误日志；关键路径避免无界并发。
- 部署检查清单：**项目 ID、区域、服务账号 JSON、API 启用状态** 与文档一致。

---

## 4. 数据一致性：Excel 原始状态与统计口径

### 现象
- 业务口头称「生效 / 作废」，Excel 列为 `effective` / `invalid` 或其它枚举，Dashboard KPI 与清单筛选**对不上**。
- 弹窗「关联款号」条数与列表筛选结果不一致（缓存快照滞后）。

### Root Cause
1. **源表枚举与内部枚举**：聚合层需将多语言、多写法映射为统一 `data_status`（如 `active` / `draft` / `obsolete` / `other`）；若未按 **DDL COMMENT** 或主数据规则清洗，统计口径会漂。
2. **Dashboard 默认口径**：产品规则要求「默认仅生效款」参与部分 KPI 时，必须与 `data_status === 'active'`（或映射后的等价条件）严格一致。
3. **快照缓存**：`final_dashboard_data.json` / `full_inventory_cache.json` 若未及时刷新，弹窗仅读旧 `inventory` 会出现 **0 条关联**；而列表经 `/api/inventory-real` 可能已触发重算。

### 解决方案（已实现方向）
- 在 **dataEngine / 聚合入口** 统一清洗 `data_status`，与字段注释及业务定义对齐。
- **asset-details**：当**物理文件存在**且缓存反查关联款号为 0 时，**现场 `processAllData` + 持久化快照**再返回，并打 `[Logic-Audit]` / `[Realtime-Link]` 日志。
- KPI 计算函数明确区分 **全量款号 / 生效款号**（如 `activeStyles` 仅统计 `data_status === 'active'`）。

---

## 文档维护

- 部署新环境时，优先对照 **`[Static-Check]`** 与 **`STORAGE_ROOT`** 日志，确认与真实上传目录一致。
- 出现「列表与弹窗不一致」时，先查 **是否同源请求**、再查 **快照时间** 与 **normalizeAssetCodeKey** 是否覆盖该款编号写法。
