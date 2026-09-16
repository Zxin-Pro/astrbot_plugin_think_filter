"""AstrBot 插件：拦截并移除 LLM 输出中的 <think> 思考内容。

适用版本：AstrBot v4.28.0（>= 4.24.0, < 5，见 metadata.yaml 的 astrbot_version）

实现思路：
- 非流式：在 `on_decorating_result`（发送消息前）里改写 message chain 中的 Plain 文本，
  使用预编译正则一次性剥离思考块，异常时原样放回。
- 流式：AstrBot v4.28.0 的 `STREAMING_RESULT` 会跳过 `on_decorating_result`，
  所以在 `on_llm_request` 中给当前 event 的 `send_streaming` 方法包一层，
  让平台适配器逐块发送前经过有状态过滤器（见 think_filter.StreamThinkFilter）。

安全原则（v1.1.1 起强制）：
- 流式包装器的每一行都可能出错，因此所有环节都有兜底：
  过滤失败 / 构造消息链失败时一律原样透传，绝不丢内容、绝不中断流。
- 即使过滤器完全失效，消息也只会"未过滤"，不会"不回复"。
- 可通过配置 filter_streaming=false 单独关闭流式过滤。
"""

from __future__ import annotations

from astrbot.api import AstrBotConfig
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Plain
from astrbot.api.star import Context, Star

from .think_filter import StreamThinkFilter, strip_think


def _make_text_chain(text: str):
    """把纯文本包成 MessageChain；失败返回 None（调用方自行决定兜底）。"""
    try:
        return MessageChain(chain=[Plain(text=text)])
    except Exception:
        return None


class ThinkFilterPlugin(Star):
    """移除 LLM 输出中的思考内容。"""

    def __init__(self, context: Context, config: AstrBotConfig | None = None) -> None:
        super().__init__(context)
        self.config = config or {}

    # -- 配置读取 ---------------------------------------------------------

    def _enabled(self) -> bool:
        return bool(self.config.get("enabled", True))

    def _stream_enabled(self) -> bool:
        return bool(self.config.get("filter_streaming", True))

    def _tags(self) -> list[str]:
        """读取标签配置；容错处理用户手写成逗号/空格分隔字符串的情况。"""
        tags = self.config.get("tags", ["think", "thinking"])
        if isinstance(tags, str):
            # 容错：'think,thinking' / 'think thinking' 也能用
            tags = [t for t in __import__("re").split(r"[,，;；\s]+", tags) if t]
        if not isinstance(tags, list):
            tags = ["think", "thinking"]
        result = [str(t).strip().lower() for t in tags if str(t).strip()]
        return result or ["think", "thinking"]

    def _unclosed_action(self) -> str:
        value = str(self.config.get("unclosed_action", "drop")).strip().lower()
        return value if value in ("drop", "keep") else "drop"

    def _log_removed(self) -> bool:
        return bool(self.config.get("log_removed", False))

    def _strip_whitespace(self) -> bool:
        return bool(self.config.get("strip_whitespace", True))

    def _buffer_limit(self) -> int:
        try:
            return int(self.config.get("stream_buffer_limit", 256))
        except (TypeError, ValueError):
            return 256

    async def initialize(self) -> None:
        """插件加载时打印生效的配置，方便在日志里确认配置是否真正生效。"""
        self.logger.info(
            "[think_filter] v%s 已加载 | tags=%s | 未闭合=%s | 流式过滤=%s | 移除日志=%s",
            "1.2.0",
            self._tags(),
            self._unclosed_action(),
            self._stream_enabled(),
            self._log_removed(),
        )

    # -- 非流式 -----------------------------------------------------------

    def _clean_chain(self, result) -> int:
        """就地清理非流式 message chain 中的思考内容，返回被移除的字符数。"""
        removed = 0
        for comp in result.chain:
            if not isinstance(comp, Plain):
                continue
            raw = comp.text or ""
            cleaned = strip_think(
                raw,
                tags=self._tags(),
                strip_whitespace=self._strip_whitespace(),
                unclosed_action=self._unclosed_action(),
            )
            if cleaned != raw:
                removed += len(raw) - len(cleaned)
                comp.text = cleaned
        return removed

    # -- 流式：包装器 ------------------------------------------------------

    def _build_stream_wrapper(self, source):
        """把原始异步生成器包一层有状态过滤器，返回新的异步生成器。

        安全设计：
        - 任何一块处理出错：该块原样透传，后续块降级为原样透传（过滤器停用）；
        - set_text 失败：退化为原链（宁可漏过滤，不可丢消息）；
        - 尾部残留转链失败：丢弃残留（只是疑似标签前缀，不是正文）；
        - 本包装器绝不向消费者抛出"过滤自身"的异常。
        """
        tags = self._tags()
        log_removed = self._log_removed()
        buffer_limit = self._buffer_limit()
        unclosed_action = self._unclosed_action()
        plugin_logger = self.logger

        def get_text(chain):
            """取出链中的纯文本；带 type 的链（reasoning/break/tool_call）不处理。"""
            if isinstance(chain, str):
                # 兼容直接产出字符串的流
                return chain
            try:
                if getattr(chain, "type", None):
                    return None
                text = chain.get_plain_text()
                return text if isinstance(text, str) else None
            except Exception:
                return None

        def set_text(chain, new_text):
            """构造保留元信息的新链；失败则原样返回旧链（漏过滤好过丢消息）。"""
            if isinstance(chain, str):
                return new_text
            try:
                new_chain = MessageChain(chain=[Plain(text=new_text)])
                new_chain.type = getattr(chain, "type", None)
                new_chain.use_t2i_ = getattr(chain, "use_t2i_", None)
                new_chain.use_markdown_ = getattr(chain, "use_markdown_", None)
                return new_chain
            except Exception:
                try:
                    # 退化方案：直接改写首个 Plain 的文本
                    for comp in getattr(chain, "chain", []) or []:
                        if isinstance(comp, Plain):
                            comp.text = new_text
                            return chain
                except Exception:
                    pass
                return chain

        async def wrapper():
            filt = StreamThinkFilter(
                tags=tags,
                log_removed=log_removed,
                buffer_limit=buffer_limit,
                unclosed_action=unclosed_action,
            )
            broken = False  # 过滤器是否已失效（失效后全部原样透传）
            try:
                async for chain in source:
                    if broken:
                        yield chain
                        continue
                    try:
                        text = get_text(chain)
                        if not isinstance(text, str):
                            # 非文本链（分段信号、工具状态等）原样透传
                            yield chain
                            continue
                        filtered = filt.feed(text)
                    except Exception:
                        # 过滤环节出错：本块原样放行，后续全部降级为透传
                        plugin_logger.exception("[think_filter] 流式过滤异常，已降级为原样透传")
                        broken = True
                        yield chain
                        continue

                    if filtered:
                        yield set_text(chain, filtered)

                # 流正常结束：处理尾部残留（未闭合 think 已在 flush 内丢弃）
                tail = filt.flush()
                if tail:
                    tail_chain = _make_text_chain(tail)
                    if tail_chain is not None:
                        yield tail_chain
            except Exception:
                # source 本身（AstrBot 管线）的异常：按原语义向上抛，
                # 但先把过滤器缓冲的残留输出，尽量减少内容丢失
                plugin_logger.exception("[think_filter] 流式源异常")
                try:
                    tail = filt.flush()
                    if tail:
                        tail_chain = _make_text_chain(tail)
                        if tail_chain is not None:
                            yield tail_chain
                except Exception:
                    pass
                raise

            if log_removed and filt.removed_text:
                try:
                    plugin_logger.info(
                        "[think_filter] 已移除思考内容 %d 字符",
                        len(filt.removed_text),
                    )
                except Exception:
                    pass

        return wrapper()

    # -- 钩子：LLM 请求前（流式过滤挂点） ---------------------------------

    @filter.on_llm_request()
    async def on_llm_request(self, event: AstrMessageEvent, req) -> None:
        """LLM 请求发出前，给 event 实例的 send_streaming 打补丁。

        为什么不用 on_decorating_result 处理流式：
        ResultDecorateStage 的 process() 开头就跳过 STREAMING_RESULT，
        流式进行中该钩子不会触发。详见 README。

        补丁只作用于当前 event 实例（不污染类），幂等可重入；
        任何失败都不影响后续流程（最坏情况是流式不过滤）。
        """
        if not self._enabled() or not self._stream_enabled():
            return
        try:
            if getattr(event, "_think_filter_stream_patched", False):
                return
            original = getattr(event, "send_streaming", None)
            if not callable(original):
                return
            event._think_filter_stream_patched = True
            plugin = self

            async def patched_send_streaming(generator, *args, **kwargs):
                try:
                    wrapped = plugin._build_stream_wrapper(generator)
                except Exception:
                    plugin.logger.exception("[think_filter] 构建流式过滤器失败，本次原样发送")
                    wrapped = generator
                return await original(wrapped, *args, **kwargs)

            event.send_streaming = patched_send_streaming
        except Exception:
            # 补丁失败只影响过滤，不影响机器人正常回复
            self.logger.exception("[think_filter] 流式接管失败")

    # -- 指令：查看生效配置 ------------------------------------------------

    @filter.command("think状态")
    async def think_status(self, event: AstrMessageEvent):
        """查看思考过滤器当前生效的配置（用于确认配置是否真正加载）。"""
        yield event.plain_result(
            "[think_filter] 当前生效配置\n"
            f"版本: 1.2.0\n"
            f"启用: {self._enabled()}\n"
            f"过滤标签: {', '.join(self._tags())}\n"
            f"未闭合处理: {self._unclosed_action()}\n"
            f"流式过滤: {self._stream_enabled()}\n"
            f"移除日志: {self._log_removed()}\n"
            "提示：修改配置后需「重载插件」才会生效。"
        )

    # -- 钩子：LLM 响应后（清理历史记录） ---------------------------------

    @filter.on_llm_response()
    async def on_llm_response(self, event: AstrMessageEvent, resp) -> None:
        """LLM 响应后清理 completion_text 中的思考内容。

        作用：写入会话历史前把 think 块清掉，避免模型在后续轮次
        看到自己历史里的思考内容，导致越写越多。

        注意：流式显示不经过这里（内容早已逐块发送），本钩子只影响
        存入历史的最终文本；非流式场景与 on_decorating_result 双重保险。
        """
        if not self._enabled():
            return
        try:
            text = getattr(resp, "completion_text", None)
            if not isinstance(text, str) or not text:
                return
            cleaned = strip_think(
                text,
                tags=self._tags(),
                strip_whitespace=self._strip_whitespace(),
                unclosed_action=self._unclosed_action(),
            )
            if cleaned != text:
                resp.completion_text = cleaned
                if self._log_removed():
                    self.logger.info(
                        "[think_filter] 历史记录已移除 %d 字符思考内容",
                        len(text) - len(cleaned),
                    )
        except Exception:
            self.logger.exception("[think_filter] 历史清理失败，已跳过")

    # -- 钩子：发送消息前（非流式路径） ------------------------------------

    @filter.on_decorating_result()
    async def on_decorating_result(self, event: AstrMessageEvent) -> None:
        """发送消息前的钩子：处理非流式结果。

        AstrBot v4.28.0 的 ResultDecorateStage 会跳过 STREAMING_RESULT，
        所以实时流式拦截由 on_llm_request 中的 send_streaming 补丁完成。
        本钩子负责非流式完整文本的清理。
        """
        if not self._enabled():
            return

        try:
            result = event.get_result()
            if result is None or not result.chain:
                return

            removed = self._clean_chain(result)
            if removed and self._log_removed():
                self.logger.info("[think_filter] 非流式结果已移除 %d 字符思考内容", removed)
        except Exception:
            # 钩子里任何异常都不能影响机器人正常回复
            self.logger.exception("[think_filter] 非流式过滤失败，已跳过")

    async def terminate(self) -> None:
        """插件卸载/停用时调用。"""
        self.logger.info("[think_filter] 思考内容过滤器已停用")
