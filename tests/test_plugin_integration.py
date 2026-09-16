"""集成测试：用假的 AstrBot API 桩，直接跑 main.py 的钩子路径。

目的：在没有真实 AstrBot 环境的情况下，验证
- 非流式：on_decorating_result 会改写 message chain；
- 流式：on_llm_request 接管 send_streaming，逐块过滤且跨 chunk 正确；
- 流式过滤器任何环节出错都会降级为原样透传，绝不丢内容、绝不抛异常；
- 开关关闭时不生效。

运行： python3 tests/test_plugin_integration.py
"""

import asyncio
import os
import sys
import types

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(ROOT))

PLUGIN_NAME = os.path.basename(ROOT)


# ---------------------------------------------------------------------------
# 构造 astrbot.* 假模块，让 main.py 可以 import
# ---------------------------------------------------------------------------
def install_stubs():
    astrbot = types.ModuleType("astrbot")
    api = types.ModuleType("astrbot.api")

    class AstrBotConfig(dict):
        pass

    class _Logger:
        def info(self, *a, **kw):
            pass

        def error(self, *a, **kw):
            pass

        def warning(self, *a, **kw):
            pass

        def exception(self, *a, **kw):
            pass

        def debug(self, *a, **kw):
            pass

    api.AstrBotConfig = AstrBotConfig
    api.logger = _Logger()
    astrbot.api = api

    # astrbot.api.message_components
    mc = types.ModuleType("astrbot.api.message_components")

    class Plain:
        def __init__(self, text="", **_):
            self.text = text

    mc.Plain = Plain

    # astrbot.api.event
    event_mod = types.ModuleType("astrbot.api.event")

    class AstrMessageEvent:
        pass

    class MessageChain:
        def __init__(self, chain=None, type=None):
            self.chain = list(chain or [])
            self.type = type
            self.use_t2i_ = None
            self.use_markdown_ = None

        def get_plain_text(self):
            return "".join(getattr(c, "text", "") for c in self.chain)

    class _Filter:
        @staticmethod
        def _deco(*a, **kw):
            def deco(fn):
                return fn

            return deco

        on_decorating_result = _deco
        on_llm_request = _deco

    event_mod.AstrMessageEvent = AstrMessageEvent
    event_mod.MessageChain = MessageChain
    event_mod.filter = _Filter()

    # astrbot.api.star
    star_mod = types.ModuleType("astrbot.api.star")

    class Star:
        def __init__(self, context=None, config=None):
            self.context = context
            import logging

            self.logger = logging.getLogger("stub")

    class Context:
        pass

    star_mod.Star = Star
    star_mod.Context = Context

    sys.modules.update(
        {
            "astrbot": astrbot,
            "astrbot.api": api,
            "astrbot.api.message_components": mc,
            "astrbot.api.event": event_mod,
            "astrbot.api.star": star_mod,
        }
    )
    return Plain, MessageChain


Plain, MessageChain = install_stubs()

# 把插件目录当作包导入，模拟 data.plugins.<name>.main
import importlib  # noqa: E402

sys.path.insert(0, os.path.dirname(ROOT))
pkg = types.ModuleType(PLUGIN_NAME)
pkg.__path__ = [ROOT]
sys.modules[PLUGIN_NAME] = pkg
main = importlib.import_module(f"{PLUGIN_NAME}.main")
think_filter = importlib.import_module(f"{PLUGIN_NAME}.think_filter")

PASS = 0
FAIL = 0


def check(name, got, want):
    global PASS, FAIL
    if got == want:
        PASS += 1
        print(f"[PASS] {name}")
    else:
        FAIL += 1
        print(f"[FAIL] {name}\n   got : {got!r}\n   want: {want!r}")


class FakeResult:
    def __init__(self, chain=None):
        self.chain = chain if chain is not None else []
        self.result_content_type = None

    def get_plain_text(self):
        return "".join(getattr(c, "text", "") for c in self.chain)


class FakeEvent:
    def __init__(self, result=None):
        self._result = result
        self.sent_stream = None
        self.sent_args = None
        self.patched = False

    def get_result(self):
        return self._result

    async def send_streaming(self, generator, use_fallback=False):
        """模拟真实平台适配器：接收生成器并逐块消费。"""
        self.sent_args = use_fallback
        self.sent_stream = []
        async for item in generator:
            self.sent_stream.append(item)
        return self.sent_stream


async def run_plugin(plugin, result):
    event = FakeEvent(result)
    await plugin.on_decorating_result(event)
    return event.get_result()


async def run_stream_hook(plugin, chunks, use_fallback=False, raw=False):
    """模拟真实管线：先触发 on_llm_request，再调用 event.send_streaming。

    默认源产出 MessageChain（与生产环境一致）；raw=True 时产出裸字符串，
    用于验证兼容性。
    """
    event = FakeEvent()
    await plugin.on_llm_request(event, object())
    event.patched = getattr(event, "_think_filter_stream_patched", False)

    async def source():
        for c in chunks:
            yield c if raw else MessageChain(chain=[Plain(c)])

    await event.send_streaming(source(), use_fallback)
    return event


def stream_text(event):
    parts = []
    for item in event.sent_stream:
        if isinstance(item, str):
            parts.append(item)
        else:
            parts.append(item.get_plain_text())
    return "".join(parts)


# ---------------------------------------------------------------------------
# 非流式
# ---------------------------------------------------------------------------
p = main.ThinkFilterPlugin(None, {"enabled": True, "tags": ["think"]})

r = asyncio.run(run_plugin(p, FakeResult(chain=[Plain("<think>内部思考</think>正式回复")])))
check("非流式-移除思考块", r.chain[0].text, "正式回复")

r = asyncio.run(run_plugin(p, FakeResult(chain=[Plain("正常文本")])))
check("非流式-无标签不变", r.chain[0].text, "正常文本")

r = asyncio.run(run_plugin(p, FakeResult(chain=[Plain("A<think>B")])))
check("非流式-未闭合", r.chain[0].text, "A")

# 关闭插件
off = main.ThinkFilterPlugin(None, {"enabled": False})
r = asyncio.run(run_plugin(off, FakeResult(chain=[Plain("<think>x</think>y")])))
check("非流式-关闭开关不处理", r.chain[0].text, "<think>x</think>y")

# 自定义标签
custom = main.ThinkFilterPlugin(None, {"enabled": True, "tags": ["think", "reasoning"]})
r = asyncio.run(run_plugin(custom, FakeResult(chain=[Plain("<reasoning>x</reasoning>正文")])))
check("非流式-自定义标签", r.chain[0].text, "正文")


# ---------------------------------------------------------------------------
# 流式：真实管线挂点
# ---------------------------------------------------------------------------
ev = asyncio.run(run_stream_hook(p, ["<thi", "nk>", "思考", "</thi", "nk>", "你好"], True))
check("流式-补丁已安装", ev.patched, True)
check("流式-跨 chunk 拆分", stream_text(ev), "你好")
check("流式-透传 fallback 参数", ev.sent_args, True)
check(
    "流式-产出均为 MessageChain", all(isinstance(i, MessageChain) for i in ev.sent_stream), True
)

# 字符串源兼容：也能正确过滤
ev = asyncio.run(run_stream_hook(p, ["<thi", "nk>", "思考", "</thi", "nk>", "你好"], raw=True))
check("流式-字符串源兼容", stream_text(ev), "你好")

ev = asyncio.run(run_stream_hook(p, ["你好", "<think>", "丢弃内容"]))
check("流式-未闭合到结尾", stream_text(ev), "你好")

ev = asyncio.run(run_stream_hook(p, list("<think>abc</think>正文")))
check("流式-逐字符", stream_text(ev), "正文")

ev = asyncio.run(run_stream_hook(p, ["普通", "文本"]))
check("流式-无标签原样", stream_text(ev), "普通文本")

ev = asyncio.run(run_stream_hook(p, ["<Think type='x'>", "a", "</THINK>", "正文"]))
check("流式-大小写与属性", stream_text(ev), "正文")


# 非文本链（type 非空）必须原样透传，不能被当成正文丢掉
async def run_typed():
    event = FakeEvent()
    await p.on_llm_request(event, object())

    async def source():
        yield FakeEvent()  # 占位，真实用例在下方单独测
        yield  # 模拟 None 块
        yield MessageChain(chain=[], type="break")

    async def typed_source():
        yield MessageChain(chain=[Plain("正文")])
        yield MessageChain(chain=[], type="break")

    await event.send_streaming(typed_source())
    return event


ev = asyncio.run(run_typed())
check(
    "流式-分段信号透传",
    [(i.type, i.get_plain_text()) for i in ev.sent_stream],
    [(None, "正文"), ("break", "")],
)


# None 块透传不崩溃
async def run_none_chunk():
    event = FakeEvent()
    await p.on_llm_request(event, object())

    async def source():
        yield MessageChain(chain=[Plain("正文")])
        yield None

    await event.send_streaming(source())
    return event


ev = asyncio.run(run_none_chunk())
check(
    "流式-None 块不崩溃",
    [i.get_plain_text() if i is not None else None for i in ev.sent_stream],
    ["正文", None],
)


# 同一个事件多次触发 on_llm_request，必须幂等
async def check_idempotent_patch():
    event = FakeEvent()
    await p.on_llm_request(event, object())
    first = event.send_streaming
    await p.on_llm_request(event, object())
    return first is event.send_streaming


check("流式-补丁幂等", asyncio.run(check_idempotent_patch()), True)

# filter_streaming=false 时不应打补丁
no_stream = main.ThinkFilterPlugin(None, {"enabled": True, "filter_streaming": False})
ev = asyncio.run(run_stream_hook(no_stream, ["<think>x</think>你好"]))
check("流式-关闭流式过滤不打补丁", ev.patched, False)
check("流式-关闭流式过滤原样输出", stream_text(ev), "<think>x</think>你好")


# ---------------------------------------------------------------------------
# 兜底能力：过滤器坏了也绝不能丢消息
# ---------------------------------------------------------------------------
async def run_feed_boom():
    """feed() 中途抛异常：该块原样透传，后续块降级为透传，内容零丢失。"""
    event = FakeEvent()
    await p.on_llm_request(event, object())

    real_feed = think_filter.StreamThinkFilter.feed
    calls = {"n": 0}

    def boom_feed(self, chunk):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("feed boom")
        return real_feed(self, chunk)

    think_filter.StreamThinkFilter.feed = boom_feed
    try:

        async def source():
            yield MessageChain(chain=[Plain("第一块")])
            yield MessageChain(chain=[Plain("<think>第二块")])
            yield MessageChain(chain=[Plain("第三块")])

        await event.send_streaming(source())
    finally:
        think_filter.StreamThinkFilter.feed = real_feed
    return event


ev = asyncio.run(run_feed_boom())
check(
    "兜底-feed 异常降级透传",
    stream_text(ev),
    "第一块<think>第二块第三块",
)


async def run_source_explode():
    """流式源中途抛异常：包装器应先 flush 残留再向上抛（不吞掉管线错误）。"""
    event = FakeEvent()
    await p.on_llm_request(event, object())

    async def source():
        yield MessageChain(chain=[Plain("<think>abc</think>正文")])
        raise RuntimeError("source boom")

    raised = False
    try:
        await event.send_streaming(source())
    except RuntimeError:
        raised = True
    return event, raised


ev, raised = asyncio.run(run_source_explode())
check("流式-源异常向上传播", raised, True)
check(
    "流式-源异常前内容已送达",
    stream_text(ev),
    "正文",
)


# ---------------------------------------------------------------------------
# 异常兜底（非流式钩子不抛异常）
# ---------------------------------------------------------------------------
class BrokenPlugin(main.ThinkFilterPlugin):
    def _clean_chain(self, result):
        raise RuntimeError("boom")


bp = BrokenPlugin(None, {"enabled": True})
r = FakeResult(chain=[Plain("<think>x</think>y")])
try:
    asyncio.run(run_plugin(bp, r))
    ok = True
except Exception:
    ok = False
check("异常兜底-钩子不抛异常", ok, True)

print(f"\n==== {PASS} passed, {FAIL} failed ====")
sys.exit(1 if FAIL else 0)
