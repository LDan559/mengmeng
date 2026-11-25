# 蓝奏云(Alist) 到 专属夸克网盘 云端全速中转工具

本项目为独立自包含的云端高速同步项目，专为借助 GitHub Actions 免费云端算力与千兆骨干网络实现小说批量极速搬运而设计。

---

## 安全设计原则

1. **零私密信息硬编码**：代码文件与流水线中绝不硬编码任何网盘 Cookie、服务器 IP 或密码。
2. **全流程 Secrets 隔离**：所有敏感凭据统一通过 GitHub 官方的 Secrets 加密存储，GitHub 平台自动对所有日志输出执行 `***` 脱敏打码，即使仓库被设为公开（Public）也绝无泄密风险。
3. **零服务器带宽消耗**：在 Alist 蓝奏云默认 302 重定向模式下，数据直接由蓝奏云直链下载至 GitHub 虚拟主机内存并推送到夸克网盘，不占用个人服务器带宽。

---

## 项目目录结构

```text
夸克网盘云端同步工具/
├── .github/
│   └── workflows/
│       └── sync.yml            # GitHub Actions 云端工作流定义 (纯变量引用)
├── data/
│   ├── .gitkeep
│   ├── sync_checkpoint.json    # [自动生成] 断点续传记录
│   ├── synced_index.jsonl      # [自动生成] 已成功同步书籍轻量索引
│   └── failed_books.jsonl      # [自动生成] 失败待补漏书籍台账
├── standalone_sync.py          # 核心多线程自包含同步脚本
├── requirements.txt            # Python 依赖清单 (仅 requests)
├── .gitignore                  # Git 忽略配置
└── README.md                   # 本说明文档
```

---

## 快速使用指南

### 第一步：在 GitHub 上创建仓库并上传代码

1. 打开 GitHub (https://github.com/) 并登录你的账号。
2. 点击右上角 "+" -> "New repository"，仓库名称可自定（如 `quark-sync`）。
3. 仓库属性建议选择 **Private (私有)**。
4. 将本文件夹内所有文件直接推送到该仓库中。

### 第二步：配置 GitHub Secrets（关键安全步骤）

进入你的 GitHub 仓库页面：
1. 点击顶部 **Settings**。
2. 左侧菜单找到 **Secrets and variables** -> 点击 **Actions**。
3. 点击绿色按钮 **New repository secret**，依次添加以下 3 个密钥：

| Secret 名称 | 必须 | 填入内容 |
| :--- | :--- | :--- |
| `QUARK_COOKIE` | 是 | 你的专属夸克网盘完整 Cookie 字符串 |
| `ALIST_URL` | 是 | 你的 Alist 服务地址（例如 `http://xxx:5244`） |
| `ALIST_PWD` | 是 | 你的 Alist 登录管理员密码 |

*注：配置完毕后，密钥内容即被 GitHub 高强度加密，后续无需在任何界面重复输入。*

### 第三步：一键触发云端全速搬运

1. 进入仓库页面 -> 点击顶部 **Actions** 标签。
2. 在左侧工作流列表中点击 **蓝奏云小说高速搬运至夸克网盘**。
3. 点击右侧 **Run workflow** 按钮展开参数面板：
   - `max_books`：本次同步小说本数上限（默认 2000 本，建议单次 1000~3000）。
   - `concurrency`：并发线程数（默认 8 线程）。
   - `delay`：线程错峰微延时（默认 0.05 秒）。
   - `max_seconds`：最大运行时长（默认 18000 秒，即 5 小时）。
   - `retry_failed`：勾选后将启动历史失败书籍专向打捞补漏。
4. 点击绿色的 **Run workflow** 即可全自动在云端执行。

### 第四步：取回成果与断点查看

每次工作流执行完毕后，GitHub 机器人都将自动提交更新回你的仓库：
- 查看 `data/synced_index.jsonl`：下载即可直接导入小说机器人。
- 查看 `data/sync_checkpoint.json`：实时掌握已同步总数与网盘分桶状态。
- 查看 `data/failed_books.jsonl`：查看因源头损坏等不可抗力未成功的书籍明细。
