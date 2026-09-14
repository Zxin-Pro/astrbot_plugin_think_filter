# astrbot_plugin_think_filter

过滤 LLM 输出中的 `<think>` 思考内容，同时兼容**非流式**与**流式**输出。

适用版本：**AstrBot v4.28.0**（`metadata.yaml` 声明 `>=4.24.0,<5`）。

## 用了哪个钩子，为什么

只用一个钩子：`@filter.on_decorating_result()`（`OnDecoratingResultEvent`，"发送消息前"）。

原因：

1. 它是发送给用户之前的**最后一道**结果处理阶段（`astrbot/core/pipeline/result_decorate/stage.py`），
   在这里改内容，不需要关心上游是哪个 provider、是否 Agent 模式。
2. 它能同时覆盖两种输出形态：
   - **非流式**：`result.chain` 里已经是完整文本，直接改写 `Plain` 组件即可。
   - **流式**：`result.result_content_type == ResultContentType.STREAMING_RESULT`，
     此时 `result.chain` 是空的，真正的数据在 `result.async_stream`（一个
     `AsyncGenerator[MessageChain, None]`）。我们在钩子里把它**包一层**有状态过滤器，
     等平台适配器逐块拉取时再过滤。

> 注意：AstrBot 在流式场景触发该钩子时会打印一条 warning
> （"Plugins that depend on the pre-send event hook may not work correctly when
> streaming output is enabled."）。那是因为大多数插件只改 `result.chain`，
> 而流式时 `chain` 为空。本插件额外处理了 `async_stream`，所以流式同样有效。

相比 `on_llm_response`：那个钩子拿不到"按 chunk 流式输出"的链路，无法处理跨 chunk 标签拆分，
因此不适合本需求。

## 已有的内置能力（避免重复造轮子）

AstrBot 自带 `provider_settings.display_reasoning_text`（默认 `false`）：

- provider 若把思考放在 `LLMResponse.reasoning_content`，AstrBot 默认**不会**发给用户，
  开启后才以 `🤔 思考: ...`（或飞书折叠面板）形式注入。
- 该开关只管"结构化 reasoning 字段"，**不处理模型把 `<think>` 直接写在正文里的情况**。

所以本插件针对的是"思考块混在正文文本里"的场景，与内置开关不冲突，可以同时使用。

## 目录结构

```
astrbot_plugin_think_filter/
├── metadata.yaml            # 插件元数据
├── _conf_schema.json        # 插件配置 schema
├── main.py                  # 插件入口（钩子在这里）
├── think_filter.py          # 过滤核心：非流式正则 + 流式状态机
├── requirements.txt         # 无第三方依赖（仅标准库）
├── ruff.toml                # lint 配置
└── tests/
    ├── test_think_filter.py         # 过滤核心单测（35 例）
    ├── test_plugin_integration.py   # 用 AstrBot API 桩跑钩子（12 例）
    └── fuzz_think_filter.py         # 随机切块一致性模糊测试
```

## 安装

1. 把 `astrbot_plugin_think_filter` 整个目录放进 AstrBot 的 `data/plugins/` 下
   （或打包成 zip 后在 WebUI 插件页上传安装）。
2. 重启 / 重载插件（WebUI 插件管理 → 重载插件）。
3. 无需安装第三方依赖。

## 配置

| 配置项 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- |
| `enabled` | bool | `true` | 是否启用 |
| `tags` | list | `["think"]` | 需要过滤的标签名，可加 `reasoning` 等 |
| `log_removed` | bool | `false` | 是否把被移除的思考内容写入日志 |
| `strip_whitespace` | bool | `true` | 过滤后是否清理首尾空白 |
| `stream_buffer_limit` | int | `256` | 流式疑似标签前缀的缓冲上限 |

## 实现要点

- **预编译正则**：`think_filter.py` 里按标签集合缓存 `re.compile` 结果，不在调用时重复编译。
- **快速路径**：文本里不含 `<think`（配置的任一标签）时直接返回原文，零正则开销。
- **流式不逐块正则**：用 `find` + 状态机（`STATE_OUTSIDE` / `STATE_INSIDE`）扫描，
  标签被切成 `</thi` + `nk>` 也能正确识别。
- **首字延迟**：只有"疑似标签前缀"（如 `<thi`、`<think type="`）会被缓冲，
  普通文本立即输出，所以正常回复的首字延迟不受影响。
- **异常兜底**：正则或状态机抛异常时返回/放行原文；钩子内部异常只记日志，不影响正常回复。
- **未闭合**：流结束时若仍在思考块内，丢弃缓冲区，等效"删除 `<think>` 到结尾"。

## 测试

```bash
cd astrbot_plugin_think_filter
python3 tests/test_think_filter.py        # 35 passed
python3 tests/test_plugin_integration.py  # 12 passed
python3 tests/fuzz_think_filter.py        # 0 mismatches
```

覆盖场景：成对标签、跨行、未闭合、大小写、带属性、额外标签、无标签、空文本、
逐字符跨 chunk 拆分、异常兜底、首字延迟、分段信号透传。
