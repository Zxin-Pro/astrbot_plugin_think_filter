"""思考内容过滤核心逻辑（与 AstrBot 解耦，方便独立测试）。

设计要点：
- 非流式：预编译正则一次性剥离成对标签与未闭合标签。
- 流式：基于 find 的有状态状态机，绝不逐 chunk 跑正则，正确处理标签跨 chunk 边界。
"""

from __future__ import annotations

import re
from collections.abc import AsyncGenerator, Callable, Iterable
from typing import Any

# ---------------------------------------------------------------------------
# 非流式：预编译正则
# ---------------------------------------------------------------------------

# 按标签集合缓存预编译结果，避免每次调用 re.compile
_PATTERN_CACHE: dict[tuple[str, ...], tuple[re.Pattern[str], re.Pattern[str]]] = {}


def _close_name_ok(name: str, norm: tuple[str, ...]) -> bool:
    """校验闭合标签名是否合法。

    宽容策略：闭合名与任一配置标签互为前缀即可（如配置 thinking 也能被
    </think> 闭合）。开闭不匹配是模型漏过滤/吞回复的重要来源，闭合方向
    宽容不会误伤正文（正常回复几乎不会包含 </think 样式的文本）。
    """
    name = name.strip().lower()
    if len(name) < 3:
        return False
    return any(name.startswith(t) or t.startswith(name) for t in norm)


def _strip_pairs(text: str, norm: tuple[str, ...], unclosed_action: str) -> str:
    """手工扫描成对标签：语义与流式状态机完全一致。

    - 起始标签严格匹配配置的标签名（\b 边界，<thinking> 不会被 <think> 误匹配）
    - 闭合标签宽容匹配（互为前缀即可），消除开闭不匹配导致的"未闭合"误判
    - 找不到合法闭合时按 unclosed_action 处理（drop=删到结尾 / keep=原样保留）
    """
    open_re = re.compile(rf"<(?:{'|'.join(norm)})\b[^>]*>", re.IGNORECASE | re.DOTALL)
    close_re = re.compile(r"</\s*([a-zA-Z0-9_-]+)\s*[^>]*>", re.IGNORECASE)

    out: list[str] = []
    pos = 0
    n = len(text)
    while pos < n:
        m = open_re.search(text, pos)
        if not m:
            out.append(text[pos:])
            break
        out.append(text[pos : m.start()])

        # 向后找第一个"合法"的闭合标签（跳过无关的 </xxx>）
        scan = m.end()
        closed_end = -1
        while True:
            cm = close_re.search(text, scan)
            if not cm:
                break
            if _close_name_ok(cm.group(1), norm):
                closed_end = cm.end()
                break
            # 非法候选：只前进 1 字符，避免吞掉嵌套其中的合法闭合
            # （如 </thi123<</THINK> 中的 </THINK>）
            scan = cm.start() + 1

        if closed_end != -1:
            pos = closed_end  # 整块丢弃
        elif unclosed_action == "drop":
            pos = n  # 未闭合：删除到结尾
        else:
            out.append(text[m.start() :])  # keep：原样保留
            pos = n
    return "".join(out)


def normalize_tags(tags: Iterable[str]) -> tuple[str, ...]:
    """规范化标签列表：去空白、转小写、去重、排序（保证缓存命中）。"""
    return tuple(sorted({t.strip().lower() for t in tags if t and t.strip()}))


def strip_think(
    text: str,
    tags: Iterable[str] = ("think", "thinking"),
    strip_whitespace: bool = True,
    unclosed_action: str = "drop",
) -> str:
    """非流式兜底：一次性移除所有思考块。

    unclosed_action:
        - "drop": 未闭合标签时删除从标签到结尾的全部内容（默认，按规范）
        - "keep": 未闭合标签时不过滤（防止模型忘闭合导致回复被吞）

    任何异常都会被捕获并返回原文，保证不会因为过滤失败导致机器人不回复。
    """
    if not isinstance(text, str) or not text:
        return text

    norm = normalize_tags(tags)
    if not norm:
        return text

    try:
        # 快速路径：不含 '<标签' 前缀时直接返回，零正则开销
        lowered = text.lower()
        if not any(f"<{t}" in lowered for t in norm):
            return text

        result = _strip_pairs(text, norm, unclosed_action)
        return result.strip() if strip_whitespace else result
    except Exception:
        return text


# ---------------------------------------------------------------------------
# 流式：有状态过滤器
# ---------------------------------------------------------------------------

STATE_OUTSIDE = 0  # 正常输出
STATE_INSIDE = 1  # 处于思考块内部，丢弃内容

# 标签名之后允许出现的字符：空白（进入属性）、'>'（闭合）、'/'（自闭合写法）
_TAG_DELIMS = frozenset(" \t\r\n>/")

# '\\b' 词边界的等价判断：Python re 里 \\w 默认匹配 Unicode 字母数字与下划线。
# 非流式正则用 '<think\\b' 判定标签名结束，因此流式侧必须用同样的规则，
# 否则 '<think，' 这类畸形输入会导致流式与非流式结果不一致。
_WORD_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")

# 闭合标签名匹配：'</' + 标签名（字母数字下划线连字符）
_CLOSE_NAME_RE = re.compile(r"</\s*([a-zA-Z0-9_-]+)", re.IGNORECASE)


def _is_word_boundary_after(text: str, pos: int) -> bool:
    """判断 text[pos] 处是否构成 \\b（前一个字符为词字符时，此处非词字符或结尾）。"""
    if pos >= len(text):
        return True  # 字符串结尾 -> 词边界
    prev = text[pos - 1]
    cur = text[pos]
    prev_word = prev.isalnum() or prev in _WORD_CHARS or prev == "_" or prev.isalpha()
    cur_word = cur.isalnum() or cur in _WORD_CHARS or cur == "_" or cur.isalpha()
    return prev_word != cur_word


class StreamThinkFilter:
    """有状态流式过滤器。

    不能对每个 chunk 直接 re.sub —— 一个标签可能被切成 ``</thi`` + ``nk>``。
    这里的做法：
    1. 维护 ``buffer`` 与 ``state``；
    2. 每次把新 chunk 拼进 buffer，用 ``find`` 扫描完整起始/结束标签；
    3. 扫描到句尾若发现"疑似标签前缀"（``<thi``、``</think``、``<think type="``），
       就把这段前缀留在 buffer 等后续 chunk；
    4. 其余内容立即输出，所以首字延迟几乎为 0（只有极小缓冲）。
    """

    def __init__(
        self,
        tags: Iterable[str] = ("think", "thinking"),
        log_removed: bool = False,
        buffer_limit: int = 256,
        unclosed_action: str = "drop",
    ) -> None:
        self.tags: tuple[str, ...] = normalize_tags(tags)
        self.log_removed = log_removed
        self.buffer_limit = max(32, int(buffer_limit))
        self.unclosed_action = unclosed_action
        self.buffer = ""
        self.state = STATE_OUTSIDE
        self.removed_parts: list[str] = []
        # 当前未闭合思考块的内容暂存（keep 模式 flush 时需要还原）
        self._current_inside: list[str] = []

    # -- 标签识别 ---------------------------------------------------------

    def _match_close_tag(self, text: str, pos: int) -> int | None:
        """匹配结束标签：宽容策略，闭合名与任一配置标签互为前缀即可。

        例如配置 thinking 时，</think>、</thinking>、</thinkin> 都能闭合；
        开闭不匹配（<thinking>...</think>）不再被误判为"未闭合"。
        闭合方向宽容不会误伤正文（正常回复几乎不含 </think 样式文本）。
        """
        m = _CLOSE_NAME_RE.match(text, pos)
        if not m:
            return None
        if not _close_name_ok(m.group(1), self.tags):
            return None
        end = text.find(">", m.end())
        if end == -1:
            return None
        return end + 1

    def _match_tag(self, text: str, pos: int, closing: bool) -> int | None:
        """text[pos] 为 '<'；若匹配到当前状态需要的完整标签则返回其结束后一位。

        - closing=False：匹配起始标签 ``<tag 属性>``
        - closing=True ：匹配结束标签 ``</tag>``
        """
        return self._match_close_tag(text, pos) if closing else self._match_open_tag(text, pos)

    def _match_open_tag(self, text: str, pos: int) -> int | None:
        """匹配起始标签 ``<tag 属性...>``（大小写不敏感）。

        标签名后按非流式正则的 ``\\b`` 语义判断词边界，
        即 '<think>'、'<think '、'<think/>'、'<think，x>' 都算命中。
        """
        probe = text[pos:].lower()
        for tag in self.tags:
            head = f"<{tag}"
            if not probe.startswith(head):
                continue
            after = pos + len(head)
            # '<thinking>' 不应被 '<think' 命中：需要词边界
            if not _is_word_boundary_after(text, after):
                continue
            end = text.find(">", after)
            if end != -1:
                return end + 1
        return None

    def _partial_prefix_len(self, text: str, pos: int) -> int:
        """判断 text[pos:] 是否为"可能构成某标签的前缀"，返回其长度（0=不是）。

        覆盖两种情况：
        - 前缀还在标签名里，如 '<thi' / '</thi'；
        - 前缀已越过标签名、进入属性区但还没遇到 '>'，如 '<think type="reasoning'。
        """
        tail = text[pos:]
        if not tail.startswith("<"):
            return 0
        if ">" in tail:
            # 已经有 '>' 说明标签必然完整（或不是我们要的标签），无需缓冲
            return 0

        lowered = tail.lower()
        best = 0
        for tag in self.tags:
            for full in (f"<{tag}", f"</{tag}"):
                if full.startswith(lowered):
                    # 前缀还在标签名中，如 '<thi'、'</thi'
                    best = max(best, len(tail))
                elif lowered.startswith(full) and ">" not in tail:
                    # 已越过标签名、还没遇到 '>'，例如 '<think type="x'、'</thinki'
                    # 结束标签按正则语义（\s*[^>]*）允许任意非 '>' 字符；
                    # 起始标签要求标签名后是词边界（与非流式 '\b' 一致）
                    nxt = lowered[len(full) : len(full) + 1]
                    if full.startswith("</") or nxt == "" or nxt in _TAG_DELIMS:
                        best = max(best, len(tail))
                    elif nxt and not (nxt.isalnum() or nxt == "_"):
                        # 非词字符 -> 构成 \b，仍可能匹配，继续缓冲
                        best = max(best, len(tail))
        return best

    # -- 对外接口 ---------------------------------------------------------

    def feed(self, chunk: str) -> str:
        """喂入一个 chunk，返回本次可安全输出的文本（可能为空串）。"""
        if not isinstance(chunk, str) or not chunk:
            return ""
        try:
            # 快速路径：正常状态、无缓冲区、chunk 无 '<' -> 直接输出，零开销
            if self.state == STATE_OUTSIDE and not self.buffer and "<" not in chunk:
                return chunk

            self.buffer += chunk
            out: list[str] = []

            while self.buffer:
                # 1) 先看开头是不是当前状态需要的完整标签
                if self.buffer[0] == "<":
                    if self.state == STATE_OUTSIDE:
                        hit = self._match_open_tag(self.buffer, 0)
                        if hit is not None:
                            # 命中起始标签 -> 进入丢弃状态，开始暂存当前块内容
                            # （keep 模式 flush 时需要还原标签本身）
                            tag_text = self.buffer[:hit]
                            self.buffer = self.buffer[hit:]
                            self.state = STATE_INSIDE
                            self._current_inside = [tag_text]
                            continue
                    else:
                        hit = self._match_close_tag(self.buffer, 0)
                        if hit is not None:
                            # 命中结束标签 -> 回到输出状态，当前块内容已入 removed_parts
                            self.buffer = self.buffer[hit:]
                            self.state = STATE_OUTSIDE
                            self._current_inside = []
                            continue
                        # 思考块内部的嵌套起始标签直接丢弃、不改变状态
                        nest = self._match_open_tag(self.buffer, 0)
                        if nest is not None:
                            self._record_removed(self.buffer[:nest])
                            self.buffer = self.buffer[nest:]
                            continue

                    # 2) 不是完整标签，看是否为疑似前缀 -> 留缓冲等后续 chunk
                    plen = self._partial_prefix_len(self.buffer, 0)
                    if plen:
                        break  # 等下一个 chunk

                    # 2.5) 思考块内的 "</名称...>"：闭合名后允许杂字符，
                    # 必须等出现 '>' 才能判定是否合法闭合（与非流式一致），
                    # 否则 "</thi请" 这类会被提前当普通内容消费掉
                    if (
                        self.state == STATE_INSIDE
                        and self.buffer.startswith("</")
                        and ">" not in self.buffer
                    ):
                        nm = _CLOSE_NAME_RE.match(self.buffer)
                        if nm:
                            name = nm.group(1).lower()
                            if any(t.startswith(name) or name.startswith(t) for t in self.tags):
                                break  # 等待 '>' 或更多字符

                    # 3) 确认不是标签，'<' 本身当普通文本处理
                    char, self.buffer = self.buffer[0], self.buffer[1:]
                    if self.state == STATE_OUTSIDE:
                        out.append(char)
                    else:
                        self._record_removed(char)
                    continue

                # 4) 普通文本：一口气搬到下一个 '<' 或缓冲区末尾
                nxt = self.buffer.find("<")
                if nxt == -1:
                    nxt = len(self.buffer)

                piece, self.buffer = self.buffer[:nxt], self.buffer[nxt:]
                if piece:
                    if self.state == STATE_OUTSIDE:
                        out.append(piece)
                    else:
                        self._record_removed(piece)

            # 防御：缓冲区异常增长时强制放行，避免内存问题
            if len(self.buffer) > self.buffer_limit:
                forced, self.buffer = self.buffer, ""
                if self.state == STATE_OUTSIDE:
                    out.append(forced)
                else:
                    self._record_removed(forced)

            return "".join(out)
        except Exception:
            # 出错时退化为原样放行，绝不中断流式输出
            raw, self.buffer = self.buffer, ""
            return raw

    def flush(self) -> str:
        """流结束时调用。

        - 仍在思考块内（未闭合标签）：默认丢弃缓冲区，等效"删到结尾"；
          若 unclosed_action == "keep"，则原样输出缓冲区（防止回复被吞）；
        - 否则缓冲区里只是疑似前缀，实际不是标签，原样输出。
        """
        residue, self.buffer = self.buffer, ""
        if self.state == STATE_INSIDE:
            if self.unclosed_action == "keep":
                # keep 模式：还原整个未闭合思考块（含已流经的内容）
                return "".join(self._current_inside) + residue
            self._record_removed(residue)
            return ""
        self._current_inside = []
        return residue

    def _record_removed(self, text: str) -> None:
        if not text:
            return
        self.removed_parts.append(text)
        if self.state == STATE_INSIDE:
            self._current_inside.append(text)

    @property
    def removed_text(self) -> str:
        return "".join(self.removed_parts)


async def filter_stream(
    source: AsyncGenerator[Any, None],
    tags: Iterable[str] = ("think",),
    log_removed: bool = False,
    buffer_limit: int = 256,
    chunk_getter: Callable[[Any], Any] | None = None,
    chunk_setter: Callable[[Any, str], Any] | None = None,
    on_removed: Callable[[str], None] | None = None,
) -> AsyncGenerator[Any, None]:
    """异步生成器包装：接收 LLM 流式响应生成器，产出过滤后的文本块。

    Args:
        source: 原始流式生成器。
        tags: 需要过滤的标签名。
        log_removed: 是否收集被移除的思考内容（配合 on_removed 记录日志）。
        buffer_limit: 单个疑似前缀的缓冲上限。
        chunk_getter: 从流产出对象中取出文本；默认对象本身就是字符串。
        chunk_setter: 把过滤后文本写回原对象；默认直接产出字符串。
        on_removed: 流结束时回调被移除的思考文本，用于日志。
    """
    filt = StreamThinkFilter(tags=tags, log_removed=log_removed, buffer_limit=buffer_limit)

    async for item in source:
        text = chunk_getter(item) if chunk_getter else item
        if not isinstance(text, str):
            yield item  # 非文本块（如分段信号）原样透传
            continue
        filtered = filt.feed(text)
        if filtered:
            yield chunk_setter(item, filtered) if chunk_setter else filtered

    tail = filt.flush()
    if tail:
        yield tail
    if on_removed is not None and filt.removed_text:
        try:
            on_removed(filt.removed_text)
        except Exception:
            pass
