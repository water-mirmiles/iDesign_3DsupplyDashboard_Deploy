# PRD：iDesign 3D Supply Dashboard

## 业务背景
本项目用于**鞋履供应链**场景下的 **3D 楦/3D 大底（Last/Sole）资产收集与覆盖进度看板**，帮助业务快速掌握：

- 款号（Style）数据的总体规模、活跃（生效）规模
- 3D 楦/底资产的匹配覆盖率
- 不同品牌的覆盖结构
- 3D 资产新增趋势（按日/周/月/季/年）

## 当前现状（基于 codebase 梳理）

### 页面与组件组织方式
- **未使用路由库**（如 react-router）。`src/App.tsx` 通过 `currentView` 字符串 `switch` 在不同页面间切换，并由 `src/components/Layout.tsx` 的 `Sidebar` 按钮触发 `onNavigate()` 完成切页。
- 认证为**本地假登录**：`src/views/Login.tsx` 仅校验用户名/密码非空即可进入系统。

### Dashboard：常量 Mock 数据
`src/views/Dashboard.tsx` 中看板数据全部为**组件文件内常量**，没有真实数据层/请求层：

- KPI：`mockKPIs`（常量对象），UI 直接读取渲染
- 品牌覆盖：`mockBrandCoverage`（常量数组），直接作为 `recharts` 的 `data`
- 趋势数据：`trendDataMap`（常量 map，按 day/week/month/quarter/year）
  - 切换周期时使用 `setTimeout(..., 600)` **模拟网络请求**，从 `trendDataMap[period]` 设置到 state 中

### SchemaMapping：includes 假解析
`src/views/SchemaMapping.tsx` 的“字段解析/映射”目前为**字符串包含（includes）规则驱动的模拟逻辑**：

- 输入区：用户粘贴 DDL/数据字典文本到 `textarea`
- “一键智能化梳理”：
  - `setTimeout(..., 2000)` 模拟 AI 解析耗时
  - 将输入 `toLowerCase()` 后通过 `text.includes('style_wms')`、`text.includes('brand')` 等规则决定 `mappedSources`
  - 若无命中，使用 `unknown_field_1/unknown_field_2` 兜底
- “保存全局映射”按钮目前**未实现持久化**

## 当前数据结构模型（types）
位于 `src/types/index.ts`，当前前端主要数据模型如下：

### 看板相关
- **`DashboardKPIs`**：看板 KPI（品牌总数、款号总数/生效数、3D 楦/底匹配数与总数、整体覆盖率等）
- **`BrandCoverageStats`**：按品牌统计已关联/未关联的款号数量（用于堆叠柱状图）
- **`AssetTrendStats`**：3D 资产新增趋势（date、newLasts、newSoles）

### 业务查询/清单
- **`InventoryItem`**：款号清单项（包含 `style_wms`、brand、last/sole 状态、`data_status` 等）
- **`LastSoleRelation`**：楦→底→款号的 1:N 查询结构

### 导入与映射
- **`ImportHistory`**：导入历史记录（文件名、类型、状态、快照日期、版本等）
- **`GlobalSchemaField`**：全局标准字段模型（标准字段 + 物理字段映射 `mappedSources`）

## 核心目标（To‑Be）

### 真实解析
实现**真实 DDL/SQL 文本解析**，从 DDL/SQL 中抽取字段名与注释（Comment），用于构建数据源字段列表并参与映射：

- 方式：优先用**正则表达式**解析常见 DDL 片段（`CREATE TABLE`、列定义、`COMMENT`/`--` 等）
- 输出：结构化结果（表名、列名、类型、注释、来源）

### 数据驱动
实现**XLSX 上传后解析与数据关联（Join）**，将静态 Mock 替换为数据驱动：

- 上传：前端读取 Excel（多个 sheet/多文件可扩展）
- 解析：根据 Mapping 规则统一字段命名（标准字段）并做清洗
- 关联：按业务键（如款号、楦号、底号）进行 Join，得到：
  - 生效款号集合
  - 3D 资产匹配状态集合
  - 品牌覆盖统计与趋势统计
- 状态管理：将结果写入**全局 State**，驱动 Dashboard/清单/关联查询等页面联动刷新

### 持久化
使用 **LocalStorage** 持久化关键配置与部分数据，降低刷新/重进系统的成本：

- 映射规则（Global Mapping）
- 导入历史（简版）
- 部分快照统计（如按日期的 KPI/趋势点）

## 核心逻辑定义

### 匹配公式（覆盖率）
**3D 覆盖率**定义为：

\[
\text{3D覆盖率} = \frac{\text{状态为生效 且 匹配到 3D 文件的款号数}}{\text{状态为生效的总款号数}}
\]

其中：
- **状态为生效**：当前数据模型中对应 `InventoryItem['data_status'] === 'active'`
- **匹配到 3D 文件**：至少满足“楦 3D 已匹配”或“大底 3D 已匹配”（具体口径可在实现时配置为 AND/OR）

## 待办清单（Task List）

### 任务 1：实现正则表达式 DDL 解析器
- 输入：DDL/SQL 文本（支持多表拼接）
- 输出：`{ tables: [{ tableName, columns: [{ name, type?, comment? }] }] }`
- 覆盖：`CREATE TABLE ... ( ... )`、列定义、`COMMENT`、常见分隔符与大小写

### 任务 2：安装 xlsx 库并实现前端 Excel 数据导入引擎
- 依赖：添加 `xlsx`（或同类库）
- 能力：选择文件/拖拽上传、解析 sheet、输出行数据（JSON）
- 与 Mapping 联动：将物理字段映射到标准字段，并产生可用于 Join 的结构

### 任务 3：实现基于 Mapping 规则的自动关联算法
- Join 键：以款号为主，关联楦/底（可扩展多键）
- 产出：用于 Dashboard 的 KPI、品牌覆盖、趋势数据；用于清单/查询的明细结构
- 结果写入：全局 State + LocalStorage（按策略保存快照）

