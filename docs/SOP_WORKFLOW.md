## SOP：本地代码变更与提交工作流

### 变更前自检

- 在修改任何代码前，必须先执行：

```bash
git status
```

- 要求：工作区应为 **干净状态**（无未提交改动）。如不干净，先完成提交或明确回滚。

### 原子化提交

- 每完成一个独立的功能点（例如：修复一个 Bug、增加一个 API、重构一个模块），必须执行一次本地提交。
- 原则：一个 commit 对应一个“可以独立回滚/独立审阅”的变更单元。

### Commit 规范

- Commit message 格式：

\(<类型>(模块): 简短描述\)

- 常用类型：
  - `feat`：新增功能
  - `fix`：修复缺陷
  - `refactor`：重构（不改变外部行为）
  - `perf`：性能优化
  - `docs`：文档
  - `test`：测试
  - `chore`：工程杂项

- 示例：
  - `fix(mapping): 修复主表识别逻辑`
  - `feat(api): 新增 schema 草稿保存接口`

### 提交流程（本地）

```bash
git status
git diff
git add <files...>
git commit -m "<类型>(模块): 简短描述"
git status
```

### 同步权限（强制）

- **严禁自动执行 `git push`**。
- 每次本地 commit 后，必须询问用户：
  - “本地已保存版本 **<commit_sha>**，是否同步到 GitHub 云端？”

