# Fork 说明 / Fork Notice

本仓库是 [usagi-org/ai-goofish-monitor](https://github.com/usagi-org/ai-goofish-monitor) 的衍生版本（fork），
重命名为 **AI Xianyu Monitor (`ai-xianyu-monitor`)** 以示区别。

原项目采用 **MIT 协议**（Copyright (c) 2025 dingyufei615），本衍生版本沿用 MIT 协议发布，
完整许可证见 [LICENSE](LICENSE)。

## 相比上游新增的修复

### 1. 定时任务偶发不执行
- **现象**：日志中偶尔出现 `Run time of job "..." was missed by ...`，整轮定时任务被跳过，下次要等数小时后。
- **根因**：APScheduler 3.x 默认 `misfire_grace_time=1s` 过严；同时 `process_service` 在 asyncio 事件循环内
  同步调用 `find_task_by_name_sync()` 查询 SQLite，数据库锁等待会短暂阻塞整个事件循环。定时回调晚于 1 秒
  触发即被判定为 missed 并丢弃（`coalesce=True` 不补跑）。
- **修复**：
  - `src/services/scheduler_service.py`：`add_job` 增加 `misfire_grace_time=None, coalesce=True, max_instances=1`，
    任务永不因轻微延迟被丢弃。
  - `src/services/process_service.py`：将阻塞的同步查询改为 `await asyncio.to_thread(self._resolve_cookie_path, ...)`，
    移出事件循环，消除对调度器的阻塞。

### 2. 日志时间戳缺失
- **现象**：同一个日志文件里混用两种输出——scraper 步骤日志带 `[ 2026-09-19 07:11:01]` 时间戳，
  但编排层、`LOG:`、`账号轮换：`、`[延迟]` 等用的是裸 `print()`，不带时间，时间线混乱。
- **修复**：`spider_v2.py` 在子进程入口 monkeypatch `builtins.print`，给所有输出统一注入时间戳；
  已带 `[ 日期 时间]` 前缀的步骤行不重复加。

## 部署

```bash
git clone https://github.com/hunsui/ai-xianyu-monitor && cd ai-xianyu-monitor
cp .env.example .env   # 按需填写配置
docker compose up -d   # 默认拉取 docker.io/hunsui/ai-xianyu-monitor:latest
```

如需从源码自行构建镜像：

```bash
docker build -t ai-xianyu-monitor:latest .
```
