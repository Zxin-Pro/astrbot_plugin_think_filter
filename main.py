"""AstrBot 插件：拦截并移除 LLM 输出中的 <think> 思考内容。

适用版本：AstrBot v4.28.0（>= 4.24.0, < 5，见 metadata.yaml 的 astrbot_version）

实现思路：
- 非流式：在 `on_decorating_result`（发送消息前）里改写 message chain 中的 Plain 文本，
  使用预编译正则一次性剥离思考块，异常时原样放回。
- 流式：AstrBot v4.28.0 的 `STREAMING_RESULT` 会跳过 `on_decorating_result`，
  所以在 `on_llm_request` 中给当前 event 的 `send_streaming` 方法包一层，
  让平台适配器逐块发送前经过有状态过滤器（见 think_filter.filter_stream）。
  这样跨 chunk 的 `</thi` + `nk>` 也能正确处理。
"""

from __future__ import annotations

from astrbot.api import AstrBotConfig
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Plain
from astrbot.api.star import Context, Star

from .think_filter import filter_stream, strip_think


def _lstrip_chunk(item):
    """清理单块的**前导**空白；返回 None 表示该块已空。"""
    try:
        if isinstance(item, str):
            cleaned = item.lstrip()
            return cleaned or None
        chain = getattr(item, "chain", None)
        if not chain:
            return item
        if isinstance(chain[0], Plain) and chain[0].text:
            chain[0].text = chain[0].text.lstrip()
        if len(chain) == 1 and isinstance(chain[0], Plain) and not chain[0].text:
            return None
        return item
    except Exception:
        return item


def _rstrip_chunk(item):
    """清理单块的**尾部**空白；返回 None 表示该块已空。"""
    try:
        if isinstance(item, str):
            cleaned = item.rstrip()
            return cleaned or None
        chain = getattr(item, "chain", None)
        if not chain:
            return item
        if isinstance(chain[-1], Plain) and chain[-1].text:
            chain[-1].text = chain[-1].text.rstrip()
        if len(chain) == 1 and isinstance(chain[0], Plain) and not chain[0].text:
            return None
        return item
    except Exception:
        return item


class ThinkFilterPlugin(Star):
    """移除 LLM 输出中的思考内容。"""

    def __init__(self, context: Context, config: AstrBotConfig | None = None) -> None:
        super().__init__(context)
        self.config = config or {}

    # -- 配置读取 ---------------------------------------------------------

    def _enabled(self) -> bool:
        return bool(self.config.get("enabled", True))

    def _tags(self) -> list[str]:
        tags = self.config.get("tags", ["think"])
        if not isinstance(tags, list):
            tags = ["think"]
        return [str(t) for t in tags if str(t).strip()] or ["think"]

    def _log_removed(self) -> bool:
        return bool(self.config.get("log_removed", False))

    def _strip_whitespace(self) -> bool:
        return bool(self.config.get("strip_whitespace", True))

    def _buffer_limit(self) -> int:
        try:
            return int(self.config.get("stream_buffer_limit", 256))
        except (TypeError, ValueError):
            return 256

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
            )
            if cleaned != raw:
                removed += len(raw) - len(cleaned)
                comp.text = cleaned
        return removed

    def _build_stream_wrapper(self, source):
        """把原始异步生成器包一层流式过滤器。

        流产出的是 MessageChain（而不是裸字符串），所以：
        - chunk_getter：从 MessageChain 里取出纯文本；
        - chunk_setter：把过滤后的文本写回 MessageChain。
        非文本链（图片、分段信号 type="break" 等）原样透传。
        """
        tags = self._tags()
        log_removed = self._log_removed()
        buffer_limit = self._buffer_limit()
        strip_ws = self._strip_whitespace()
        plugin_logger = self.logger

        def get_text(chain):
            try:
                if getattr(chain, "type", None):
                    # 带类型的链（reasoning / tool_call / break）保持原样，不做文本过滤
                    return None
                text = chain.get_plain_text()
                return text if isinstance(text, str) else None
            except Exception:
                return None

        def set_text(chain, new_text):
            """把过滤后的文本写回链对象，保留链里的非文本组件（图片等）。"""
            try:
                comps = list(getattr(chain, "chain", []) or [])
                kept = [c for c in comps if not isinstance(c, Plain)]
                replaced = False
                result_comps = []
                for comp in comps:
                    if isinstance(comp, Plain) and not replaced:
                        comp.text = new_text
                        result_comps.append(comp)
                        replaced = True
                    elif isinstance(comp, Plain):
                        continue  # 多余的 Plain 已被合并进首个
                    else:
                        result_comps.append(comp)
                if not replaced:
                    result_comps = [Plain(new_text), *kept]
                chain.chain = result_comps
                return chain
            except Exception:
                # 改写失败时退化为只输出文本，保证内容不丢
                return new_text

        def report_removed(text: str) -> None:
            plugin_logger.info(
                "[think_filter] 已移除思考内容 %d 字符：%s",
                len(text),
                text if len(text) <= 500 else text[:500] + "...",
            )

        async def wrapper():
            # 逐块过滤：注意这里传的是 chain 对象，非文本块会被 filter_stream 原样 yield。
            # 首尾空白只在"整条回复"的最前/最后一块上清理，绝不逐块 strip，
            # 否则会破坏正文内部的换行与空格。
            first = True
            pending = None  # 延后一块，用来判断它是否是最后一块
            async for item in filter_stream(
                source,
                tags=tags,
                log_removed=log_removed,
                buffer_limit=buffer_limit,
                chunk_getter=get_text,
                chunk_setter=set_text,
                on_removed=report_removed if log_removed else None,
            ):
                # filter_stream 流结束时 flush 出的尾巴是裸字符串，
                # 平台适配器要求 MessageChain，这里统一转换
                if isinstance(item, str):
                    item = MessageChain(chain=[Plain(item)])

                if first:
                    if strip_ws:
                        item = _lstrip_chunk(item)
                        if item is None:
                            continue  # 该块被清空，仍是首块
                    first = False
                    pending = item
                    continue

                if pending is not None:
                    yield pending
                pending = item

            if pending is None:
                return
            if strip_ws:
                pending = _rstrip_chunk(pending)
                if pending is None:
                    return
            yield pending

        return wrapper()

    # -- 钩子：LLM 请求前（流式过滤的真正挂点） ---------------------------

    @filter.on_llm_request()
    async def on_llm_request(self, event: AstrMessageEvent, req) -> None:
        """LLM 请求发出前，给 event 实例的 send_streaming 打补丁。

        为什么不用 on_decorating_result 处理流式：
        ResultDecorateStage 的 process() 开头就有
        ``if result.result_content_type == ResultContentType.STREAMING_RESULT: return``
        —— 流式进行中该钩子根本不会触发（只在结束后以 STREAMING_FINISH
        触发一次，那时内容早已发完）。因此包装 async_stream 的做法是无效的。

        真正的挂点是 event.send_streaming：RespondStage 在发送流式结果时调用
        ``await event.send_streaming(result.async_stream, realtime_segmenting)``，
        平台适配器拿到生成器后逐块迭代发送。这里在实例上覆盖该方法，
        把生成器先包一层有状态过滤器再交给原实现，即可拦截流式内容。

        补丁只作用于当前 event 实例（不污染类），并且用标记保证幂等，
        Agent 多步执行多次触发 on_llm_request 时不会重复包装。
        """
        if not self._enabled():
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
                wrapped = plugin._build_stream_wrapper(generator)
                return await original(wrapped, *args, **kwargs)

            event.send_streaming = patched_send_streaming
            self.logger.debug("[think_filter] 已为本次事件接管 send_streaming")
        except Exception:
            # 补丁失败不影响正常回复，非流式路径仍然有效
            self.logger.error("[think_filter] 流式接管失败", exc_info=True)

    # -- 钩子：发送消息前（非流式路径） ------------------------------------

    @filter.on_decorating_result()
    async def on_decorating_result(self, event: AstrMessageEvent) -> None:
        """发送消息前的钩子：主要处理非流式结果，并兼容旧/特殊路径。

        AstrBot v4.28.0 的 ResultDecorateStage 会跳过 STREAMING_RESULT，
        所以真正的实时流式拦截由 on_llm_request 中的 send_streaming 补丁完成。

        本钩子负责：
        - 非流式：result.chain 里是完整文本，直接改写 Plain 组件；
        - 某些不走 send_streaming 的特殊/降级路径：尽力清理完整结果；
        - STREAMING_FINISH：只做兜底清理，该结果本身不会再次发送。
        """
        if not self._enabled():
            return

        try:
            result = event.get_result()
            if result is None:
                return

            # 流式结果：包装 async_stream
            stream = getattr(result, "async_stream", None)
            if stream is not None:
                result.async_stream = self._build_stream_wrapper(stream)
                return

            # 非流式结果：直接清理文本
            if not result.chain:
                return
            removed = self._clean_chain(result)
            if removed and self._log_removed():
                self.logger.info("[think_filter] 非流式结果已移除 %d 字符思考内容", removed)
        except Exception:
            # 钩子里任何异常都不能影响机器人正常回复
            self.logger.error("[think_filter] 过滤失败，已跳过本次处理", exc_info=True)

    async def terminate(self) -> None:
        """插件卸载/停用时调用。"""
        self.logger.info("[think_filter] 思考内容过滤器已停用")
