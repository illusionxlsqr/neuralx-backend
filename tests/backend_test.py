"""Backend tests for NEURAL-X Pocket Option bot."""
import os
import time
import pytest
import requests

BASE_URL = os.environ.get("REACT_APP_BACKEND_URL", "").rstrip("/")
if not BASE_URL:
    # fallback: read from frontend/.env
    try:
        with open("/app/frontend/.env") as f:
            for line in f:
                if line.startswith("REACT_APP_BACKEND_URL="):
                    BASE_URL = line.split("=", 1)[1].strip().rstrip("/")
                    break
    except Exception:
        pass

API = f"{BASE_URL}/api"


@pytest.fixture(scope="module")
def s():
    sess = requests.Session()
    sess.headers.update({"Content-Type": "application/json"})
    return sess


def test_root(s):
    r = s.get(f"{API}/")
    assert r.status_code == 200
    assert "message" in r.json()


def test_state_shape(s):
    r = s.get(f"{API}/po/state")
    assert r.status_code == 200
    d = r.json()
    for k in ["status", "connected", "running", "amount", "take_profit",
              "stop_loss", "duration_mode", "assets", "reasoning", "trades",
              "current_trade", "balance", "wins", "losses"]:
        assert k in d, f"missing {k}"
    assert d["duration_mode"] == "auto"


def test_settings_persists(s):
    # save amount=1, clear tp/sl by passing 0 which is treated as None
    r = s.post(f"{API}/po/settings", json={"amount": 1, "take_profit": 0, "stop_loss": 0})
    assert r.status_code == 200
    d = r.json()
    assert d["amount"] == 1.0
    assert d["take_profit"] is None
    assert d["stop_loss"] is None
    # GET verify
    d2 = s.get(f"{API}/po/state").json()
    assert d2["amount"] == 1.0
    assert d2["take_profit"] is None
    assert d2["stop_loss"] is None


def test_settings_invalid_amount(s):
    r = s.post(f"{API}/po/settings", json={"amount": -5})
    assert r.status_code == 400


def test_connected_auto(s):
    """DEMO SSID stored server-side should have auto-connected at startup."""
    # give up to 90s for auto-connect
    for _ in range(90):
        d = s.get(f"{API}/po/state").json()
        if d["status"] == "connected":
            break
        time.sleep(1)
    d = s.get(f"{API}/po/state").json()
    assert d["status"] == "connected", f"not connected: {d.get('status')} err={d.get('error')}"
    assert d["balance"] is not None
    assert d["demo"] is True


def test_toggle_and_bot_does_not_self_shutdown(s):
    """Core bug fix: with empty TP/SL, bot must remain running for 75-90s."""
    # ensure TP/SL empty
    s.post(f"{API}/po/settings", json={"amount": 1, "take_profit": 0, "stop_loss": 0})
    # make sure connected
    d = s.get(f"{API}/po/state").json()
    if d["status"] != "connected":
        pytest.skip("not connected — skipping toggle test")
    # start
    r = s.post(f"{API}/po/toggle", json={"running": True})
    assert r.status_code == 200
    assert r.json()["running"] is True

    # wait 80s and poll
    stopped_at = None
    for i in range(16):
        time.sleep(5)
        st = s.get(f"{API}/po/state").json()
        if not st["running"]:
            stopped_at = i * 5
            break
    # stop bot for cleanup
    s.post(f"{API}/po/toggle", json={"running": False})
    assert stopped_at is None, f"bot self-stopped after ~{stopped_at}s (bug not fixed)"


def test_toggle_ignores_stale_tp_below_balance(s):
    """If TP <= balance at start, backend should clear it (not stop the bot)."""
    d = s.get(f"{API}/po/state").json()
    if d["status"] != "connected" or d["balance"] is None:
        pytest.skip("not connected")
    bal = d["balance"]
    # set a TP well BELOW balance
    tp_invalid = max(1.0, bal - 5)
    s.post(f"{API}/po/settings", json={"amount": 1, "take_profit": tp_invalid, "stop_loss": 0})
    d2 = s.get(f"{API}/po/state").json()
    assert d2["take_profit"] == tp_invalid
    r = s.post(f"{API}/po/toggle", json={"running": True})
    assert r.status_code == 200
    d3 = r.json()
    assert d3["running"] is True, "bot should be running"
    assert d3["take_profit"] is None, "backend must have cleared invalid TP"
    # cleanup
    s.post(f"{API}/po/toggle", json={"running": False})


def test_assets_populated(s):
    d = s.get(f"{API}/po/state").json()
    if d["status"] != "connected":
        pytest.skip("not connected")
    assert isinstance(d["assets"], list)
    # may be empty if API discovery still running; give it a chance
    if len(d["assets"]) == 0:
        time.sleep(5)
        d = s.get(f"{API}/po/state").json()
    assert len(d["assets"]) > 0, "no crypto assets auto-detected"
    a = d["assets"][0]
    assert "symbol" in a and "payout" in a


def test_zzz_connect_invalid_ssid_returns_400(s):
    """MUST BE LAST — invalid connect currently wipes existing valid client (side-effect bug)."""
    r = s.post(f"{API}/po/connect", json={"ssid": "not-a-valid-ssid"})
    assert r.status_code == 400, f"expected 400, got {r.status_code} {r.text}"
