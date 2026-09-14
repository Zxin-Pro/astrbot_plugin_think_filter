"""集成测试：用假的 AstrBot API 桩，直接跑 main.py 的两个钩子路径。

目的：在没有真实 AstrBot 环境的情况下，验证
- 非流式：on_decorating_result 会改写 message chain；
- 流式：on_decorating_result 会包装 async_stream，逐块过滤且跨 chunk 正确；
- 开关关闭时不生效；
- 内部异常不影响事件（钩子不抛错）。

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

        def debug(self, *a, **kw):
            pass

    api.AstrBotConfig = AstrBotConfig
    api.logger = _Logger()
    astrbot.api = api

    # astrbot.api.message_components
    mc = types.ModuleType("astrbot.api.message_components")

    class Plain:
        def __init__(self, text=""):
            self.text = text

    mc.Plain = Plain

    # astrbot.api.event
    event_mod = types.ModuleType("astrbot.api.event")

    class AstrMessageEvent:
        pass

    class _Filter:
        @staticmethod
        def on_decorating_result(*a, **kw):
            def deco(fn):
                return fn

            return deco

    event_mod.AstrMessageEvent = AstrMessageEvent
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
    return Plain


Plain = install_stubs()

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
    def __init__(self, chain=None, stream=None):
        self.chain = chain if chain is not None else []
        self.async_stream = stream
        self.result_content_type = None

    def get_plain_text(self):
        return "".join(getattr(c, "text", "") for c in self.chain)


class FakeChain:
    """模拟 MessageChain：有 type、有 chain 列表、有 get_plain_text。"""

    def __init__(self, text=None, type_=None, comps=None):
        self.type = type_
        self.chain = comps if comps is not None else ([Plain(text)] if text else [])

    def get_plain_text(self):
        return "".join(getattr(c, "text", "") for c in self.chain)


class FakeEvent:
    def __init__(self, result):
        self._result = result

    def get_result(self):
        return self._result


async def run_plugin(plugin, result):
    event = FakeEvent(result)
    await plugin.on_decorating_result(event)
    return event.get_result()


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
# 流式
# ---------------------------------------------------------------------------
async def fake_source(chunks):
    for c in chunks:
        yield FakeChain(c)


async def collect_stream(plugin, chunks):
    r = FakeResult(stream=fake_source(chunks))
    r = await run_plugin(plugin, r)
    out = []
    async for item in r.async_stream:
        if isinstance(item, str):
            out.append(item)
        else:
            out.append(item.get_plain_text())
    return "".join(out)


check(
    "流式-跨 chunk 拆分",
    asyncio.run(collect_stream(p, ["<thi", "nk>", "思考", "</thi", "nk>", "你好"])),
    "你好",
)
check(
    "流式-未闭合到结尾",
    asyncio.run(collect_stream(p, ["你好", "<think>", "丢弃内容"])),
    "你好",
)
check(
    "流式-逐字符",
    asyncio.run(collect_stream(p, list("<think>abc</think>正文"))),
    "正文",
)
check(
    "流式-无标签原样",
    asyncio.run(collect_stream(p, ["普通", "文本"])),
    "普通文本",
)
check(
    "流式-大小写与属性",
    asyncio.run(collect_stream(p, ["<Think type='x'>", "a", "</THINK>", "正文"])),
    "正文",
)

# 关键回归：分块时不能逐块 strip，正文内部换行/空格必须保留，只清整条回复的首尾
check(
    "流式-保留正文内部换行与空格",
    asyncio.run(collect_stream(p, ["  <think>x</think>\n", "第一行  带空格\n", "第二行\n  "])),
    "第一行  带空格\n第二行",
)
check(
    "流式-首尾空白清理",
    asyncio.run(collect_stream(p, ["\n\n", "正文", "\n\n"])),
    "正文",
)


# 非文本链（type 非空）必须原样透传，不能被当成正文丢掉
async def typed_source():
    yield FakeChain("正文")
    yield FakeChain(type_="break", comps=[])


async def collect_typed():
    r = await run_plugin(p, FakeResult(stream=typed_source()))
    out = []
    async for item in r.async_stream:
        out.append(item if isinstance(item, str) else (item.type, item.get_plain_text()))
    return out


check(
    "流式-分段信号透传",
    asyncio.run(collect_typed()),
    [(None, "正文"), ("break", "")],
)


# ---------------------------------------------------------------------------
# 异常兜底
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
