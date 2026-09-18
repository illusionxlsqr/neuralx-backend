"""Unit tests for the multi-timeframe analyze_and_pick strategy.

Covers:
  * bullish HTF + bullish LTF -> CALL
  * bearish HTF + bearish LTF -> PUT
  * flat market -> no signal (None or {_reject:[...]})
  * fast_exit -> duration in seconds (15/30)
  * reasoning format: contains "confluenza X/5", HTF label, timer, payout
"""
import asyncio
import sys
import pytest

sys.path.insert(0, "/app/backend")


def _mk_candles(closes, body_dir=1):
    """Wrap closes with open/high/low so candela indecision filter passes.

    body_dir=+1 => green candle (open below close)
    body_dir=-1 => red candle (open above close)
    """
    out = []
    for c in closes:
        if body_dir >= 0:
            o = c - 0.30
            hi = c + 0.02
            lo = o - 0.02
        else:
            o = c + 0.30
            hi = o + 0.02
            lo = c - 0.02
        out.append({"open": o, "close": c, "high": hi, "low": lo})
    return out


class FakeClient:
    """Returns different candle series based on the timeframe requested.

    tf==60  -> LTF (1m entry candles)
    tf==300 -> HTF (5m master trend candles)
    """
    def __init__(self, ltf_closes, htf_closes, body_dir=1):
        self._ltf = _mk_candles(ltf_closes, body_dir)
        self._htf = _mk_candles(htf_closes, body_dir)

    async def get_candles(self, sym, tf, count):
        return self._htf if int(tf) >= 120 else self._ltf


def _reset_po():
    from server import PO
    PO["asset_cooldown"] = {}
    PO["loss_streak"] = 0
    PO["global_pause_until"] = 0.0
    PO["fast_exit"] = False


@pytest.mark.asyncio
async def test_call_on_bullish_multi_tf():
    from server import analyze_and_pick, PO, CLIENT
    _reset_po()
    ltf = [100 + i * 0.6 for i in range(80)]
    htf = [95 + i * 0.5 for i in range(60)]
    CLIENT["c"] = FakeClient(ltf, htf, body_dir=+1)
    PO["assets"] = [{"symbol": "BTCUSD_otc", "payout": 92}]
    pick = await analyze_and_pick()
    assert pick is not None, "should produce a signal on strong bullish trend"
    assert "_reject" not in pick, f"rejected: {pick}"
    assert pick["side"] == "call", f"expected call, got {pick}"
    assert "CALL (compra)" in pick["reason"]
    assert "HTF" in pick["reason"] and "/5" in pick["reason"]
    assert "timer" in pick["reason"] and "payout" in pick["reason"]


@pytest.mark.asyncio
async def test_put_on_bearish_multi_tf():
    from server import analyze_and_pick, PO, CLIENT
    _reset_po()
    ltf = [100 - i * 0.6 for i in range(80)]
    htf = [110 - i * 0.5 for i in range(60)]
    CLIENT["c"] = FakeClient(ltf, htf, body_dir=-1)
    PO["assets"] = [{"symbol": "ETHUSD_otc", "payout": 90}]
    pick = await analyze_and_pick()
    assert pick is not None, "should produce a signal on strong bearish trend"
    assert "_reject" not in pick, f"rejected: {pick}"
    assert pick["side"] == "put"
    assert "PUT (vende)" in pick["reason"]
    assert "HTF" in pick["reason"] and "/5" in pick["reason"]


@pytest.mark.asyncio
async def test_flat_market_no_signal():
    """A perfectly flat series must be rejected (HTF unclear or low volatility)."""
    from server import analyze_and_pick, PO, CLIENT
    _reset_po()
    ltf = [100.0 for _ in range(80)]
    htf = [100.0 for _ in range(60)]
    CLIENT["c"] = FakeClient(ltf, htf, body_dir=+1)
    PO["assets"] = [{"symbol": "SOLUSD_otc", "payout": 85}]
    pick = await analyze_and_pick()
    assert pick is None or pick.get("_reject") is not None, f"flat market should reject, got {pick}"


@pytest.mark.asyncio
async def test_fast_exit_seconds_timer():
    from server import analyze_and_pick, PO, CLIENT
    _reset_po()
    PO["fast_exit"] = True
    ltf = [100 + i * 0.7 for i in range(80)]
    htf = [95 + i * 0.6 for i in range(60)]
    CLIENT["c"] = FakeClient(ltf, htf, body_dir=+1)
    PO["assets"] = [{"symbol": "BTCUSD_otc", "payout": 92}]
    pick = await analyze_and_pick()
    assert pick is not None and "_reject" not in pick
    assert pick["duration"] in (15, 30), f"fast_exit should pick 15/30s, got {pick['duration']}"


@pytest.mark.asyncio
async def test_cooldown_blocks_asset():
    """Asset in cooldown must be rejected even with strong signal."""
    import time
    from server import analyze_and_pick, PO, CLIENT
    _reset_po()
    ltf = [100 + i * 0.6 for i in range(80)]
    htf = [95 + i * 0.5 for i in range(60)]
    CLIENT["c"] = FakeClient(ltf, htf, body_dir=+1)
    PO["assets"] = [{"symbol": "BTCUSD_otc", "payout": 92}]
    PO["asset_cooldown"] = {"BTCUSD_otc": time.time() + 60}
    pick = await analyze_and_pick()
    # should not produce a signal for BTCUSD_otc
    assert pick is None or pick.get("_reject") is not None or pick.get("symbol") != "BTCUSD_otc"
