"""离线测试：不依赖 AstrBot，直接验证过滤核心逻辑。

运行： python3 tests/test_think_filter.py
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from think_filter import StreamThinkFilter, filter_stream, strip_think  # noqa: E402

PASS = 0
FAIL = 0


def check(name: str, got, want) -> None:
    global PASS, FAIL
    if got == want:
        PASS += 1
        print(f"[PASS] {name}")
    else:
        FAIL += 1
        print(f"[FAIL] {name}\n   got : {got!r}\n   want: {want!r}")


# ---------------------------------------------------------------------------
# 1. 非流式
# ---------------------------------------------------------------------------
check(
    "非流式-成对标签",
    strip_think("<think>这里是思考</think>正式回复"),
    "正式回复",
)
check(
    "非流式-跨行",
    strip_think("<think>\n第一行\n第二行\n</think>\n\n正式回复"),
    "正式回复",
)
check(
    "非流式-未闭合",
    strip_think("正式前<think>思考到结尾没有闭合"),
    "正式前",
)
check("非流式-大小写", strip_think("<Think>A</THINK>正文"), "正文")
check(
    "非流式-带属性",
    strip_think('<think type="reasoning" foo="bar">A</think >正文'),
    "正文",
)
check("非流式-无标签", strip_think("普通文本 no tags"), "普通文本 no tags")
check("非流式-空串", strip_think(""), "")
check(
    "非流式-多块",
    strip_think("<think>a</think>X<think>b</think>Y"),
    "XY",
)
check(
    "非流式-不误删 thinking 标签",
    strip_think("<thinking>不该删</thinking>", tags=("think",)),
    "<thinking>不该删</thinking>",
)
check(
    "非流式-额外标签",
    strip_think("<reasoning>x</reasoning>正文", tags=["think", "reasoning"]),
    "正文",
)
check(
    "非流式-保留首尾空白开关",
    strip_think("  <think>a</think>  ", strip_whitespace=False),
    "    ",
)

# ---------------------------------------------------------------------------
# 2. 流式：逐字符（最极端的跨 chunk 拆分）
# ---------------------------------------------------------------------------


def stream_sync(text: str, tags=("think",), size: int = 1) -> str:
    f = StreamThinkFilter(tags=tags)
    out = []
    for i in range(0, len(text), size):
        out.append(f.feed(text[i : i + size]))
    out.append(f.flush())
    return "".join(out)


check(
    "流式-逐字符完整标签",
    stream_sync("<think>思考内容</think>正式回复"),
    "正式回复",
)
check(
    "流式-指定拆分 </thi + nk>",
    stream_sync("<think>abc</think>正文"),
    "正文",
)
check(
    "流式-指定拆分 <thi + nk>",
    stream_sync("<think>abc</think>正文"),
    "正文",
)
check(
    "流式-未闭合到结尾",
    stream_sync("正文<think>后面全是要丢的"),
    "正文",
)
check("流式-大小写混写", stream_sync("<THINK>a</Think>正文"), "正文")
check(
    "流式-带属性",
    stream_sync('<think type="reasoning">a</think >正文'),
    "正文",
)
check("流式-无标签", stream_sync("普通文本 no tags"), "普通文本 no tags")
check("流式-空串", stream_sync(""), "")
check(
    "流式-标签前有正常文本",
    stream_sync("你好 <think>内部</think> 世界"),
    "你好  世界",
)
check(
    "流式-不误删 thinking（仅配置 think 时）",
    stream_sync("<thinking>keep</thinking>", tags=("think",)),
    "<thinking>keep</thinking>",
)
check(
    "流式-默认配置下 thinking 标签被过滤",
    stream_sync("<thinking>x</thinking>正文", tags=("think", "thinking")),
    "正文",
)
check(
    "流式-开闭不匹配也可闭合",
    stream_sync("<thinking>abc</think>正文", tags=("thinking",)),
    "正文",
)
check(
    "非流式-开闭不匹配也可闭合",
    strip_think("<thinking>abc</think>正文", tags=("thinking",)),
    "正文",
)
check(
    "非流式-无关闭合标签不吞正文（keep）",
    strip_think(
        "<thinking>abc</div>def</thinking>回复",
        tags=("thinking",),
        unclosed_action="keep",
    ),
    "回复",
)
check(
    "流式-小块 2 字符",
    stream_sync("<think>12345</think>AB", size=2),
    "AB",
)
check(
    "流式-小块 3 字符",
    stream_sync("前<think>中</think>后", size=3),
    "前后",
)
check(
    "流式-只有 < 不是标签",
    stream_sync("a < b > c"),
    "a < b > c",
)
check(
    "流式-伪标签前缀后接其他字符",
    stream_sync("a <thxx b"),
    "a <thxx b",
)
check(
    "流式-额外标签",
    stream_sync("<reasoning>x</reasoning>正文", tags=["think", "reasoning"]),
    "正文",
)

# ---------------------------------------------------------------------------
# 3. 流式：异步生成器包装
# ---------------------------------------------------------------------------


async def gen(chunks):
    for c in chunks:
        yield c


async def collect(chunks, **kw):
    return "".join([x async for x in filter_stream(gen(chunks), **kw)])


check(
    "异步包装-跨 chunk",
    asyncio.run(collect(["<thi", "nk>", "思考", "</thi", "nk>", "你好"])),
    "你好",
)
check(
    "异步包装-未闭合",
    asyncio.run(collect(["你好", "<think", ">", "丢弃"])),
    "你好",
)
check(
    "异步包装-空块被跳过",
    asyncio.run(collect(["<think>", "a", "</think>", "", "ok"])),
    "ok",
)


# 非文本块应原样透传
async def mixed_gen():
    yield "正文"
    yield {"type": "break"}


async def collect_mixed():
    return [x async for x in filter_stream(mixed_gen())]


mixed = asyncio.run(collect_mixed())
check("异步包装-非文本块透传", mixed, ["正文", {"type": "break"}])

# ---------------------------------------------------------------------------
# 4. 异常兜底
# ---------------------------------------------------------------------------
check("异常兜底-非字符串输入", strip_think(None), None)
check("异常兜底-标签列表为空", strip_think("<think>a</think>b", tags=[]), "<think>a</think>b")


class BoomFilter(StreamThinkFilter):
    """模拟内部逻辑抛异常，验证会退化为"原样放行"。"""

    def _match_open_tag(self, *a, **kw):
        raise RuntimeError("boom")


bf = BoomFilter()
check("异常兜底-流式内部报错放行原文", bf.feed("<think>x"), "<think>x")

# ---------------------------------------------------------------------------
# 5. 首字延迟：正常文本必须立即输出，不能等后续 chunk
# ---------------------------------------------------------------------------
f = StreamThinkFilter()
first = f.feed("你")
check("延迟-首块立即输出", first, "你")
second = f.feed("好")
check("延迟-次块立即输出", second, "好")

print(f"\n==== {PASS} passed, {FAIL} failed ====")
sys.exit(1 if FAIL else 0)
