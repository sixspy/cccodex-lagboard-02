"""双时钟取严裁决写路径测试。

口径（钉死于双时钟裁决簿，当天不得更换）：
  甲路径：取严结果与目录输出态同库事务落库，失败整笔回退。
  取严滞后 = max(源戳滞后, 摄入戳滞后)，宽松口径禁用。

失败测先行：
  - test_strict_write_failure_rolls_back      取严失败测：半截写库即红
  - test_concurrent_same_source_contention    双路抢写失败测：缺双时钟争用留痕即红
"""
import sqlite3
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

import app
from app import app as web, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    init_db()
    yield


def login(c):
    r = c.post("/api/login", json={"username": "admin", "password": "123456"})
    assert r.status_code == 200
    return {"Authorization": "Bearer " + r.json()["token"]}


def stale_msg(minutes_src=180, minutes_ing=175, source="订单源", **extra):
    now = datetime.now().replace(microsecond=0)
    msg = {
        "source": source,
        "source_clock": (now - timedelta(minutes=minutes_src)).isoformat(),
        "ingest_clock": (now - timedelta(minutes=minutes_ing)).isoformat(),
    }
    msg.update(extra)
    return msg


def arrivals_of(c, h, source="订单源"):
    return [a for a in c.get("/api/arrivals", headers=h).json()["items"] if a["source"] == source]


def output_mode(c, h, source="订单源"):
    items = c.get("/api/sources", headers=h).json()["items"]
    return {s["name"]: s["output_mode"] for s in items}[source]


def events(c, h):
    return c.get("/api/dual-clock/policy", headers=h).json()["events"]


# ---------- 取严失败测：半截写库即红 ----------

def test_strict_write_failure_rolls_back(tmp_path):
    """取严结果与目录输出态同库事务落库；目录更新必败时整笔回退。"""
    c = TestClient(web)
    h = login(c)
    assert output_mode(c, h) == "可写"
    n0 = len(arrivals_of(c, h))
    # 在库层装触发器：取严到达行写入后，目录输出态更新必败 → 制造半截写库场景
    conn = sqlite3.connect(tmp_path / "t.db")
    conn.execute(
        "CREATE TRIGGER fail_catalog_update BEFORE UPDATE ON sources "
        "BEGIN SELECT RAISE(ABORT,'forced catalog failure'); END"
    )
    conn.commit()
    conn.close()

    r = c.post("/api/arrivals", headers=h, json=stale_msg())
    assert r.status_code == 500
    # 整笔回退：到达行不得半截落库，目录输出态不得仍旧被改一半
    assert len(arrivals_of(c, h)) == n0, "取严到达行半截落库"
    assert output_mode(c, h) == "可写", "目录输出态半截更新"
    # 留痕：取严失败回退
    assert any(e["event"] == "取严失败回退" and e["source"] == "订单源" for e in events(c, h))


# ---------- 双路抢写失败测：缺双时钟争用留痕即红 ----------

def test_concurrent_same_source_contention():
    """两路采集同时推同一源须排队，后到只记双时钟争用，不得写入。"""
    c = TestClient(web)
    h = login(c)
    n0 = len(arrivals_of(c, h))
    mode0 = output_mode(c, h)
    # 第一路采集占用订单源写锁（模拟同源写入进行中）
    lock = app.SOURCE_LOCKS["订单源"]
    assert lock.acquire(blocking=False)
    try:
        r = c.post("/api/arrivals", headers=h, json=stale_msg())
    finally:
        lock.release()
    assert r.status_code == 409
    assert "双时钟争用" in r.json()["detail"]
    # 后到只留痕：不得写到达行、不得动目录输出态
    assert len(arrivals_of(c, h)) == n0
    assert output_mode(c, h) == mode0
    evs = [e for e in events(c, h) if e["event"] == "双时钟争用" and e["source"] == "订单源"]
    assert len(evs) == 1, "缺双时钟争用留痕"


# ---------- 取严口径与裁决簿 ----------

def test_lenient_pass_no_longer_allowed():
    """复现夹具同款报文：两戳互差仅5分钟但均滞后约3小时，取严后必须冻结。"""
    c = TestClient(web)
    h = login(c)
    r = c.post("/api/arrivals", headers=h, json=stale_msg(180, 175))
    assert r.status_code == 200
    b = r.json()
    assert b["arbitration"] == "取严"
    assert b["level"] == "违约" and b["output_mode"] == "冻结"
    assert output_mode(c, h) == "冻结", "取严写成功却目录输出态仍旧"


def test_strict_takes_max_of_two_clocks():
    """取严 = max(源戳滞后, 摄入戳滞后)，无论哪一侧更严都取严。"""
    c = TestClient(web)
    h = login(c)
    # 源戳更严：源滞后≈25分、摄入滞后≈5分 → 取严≈25分 → 告警/降级只读（SLA=15）
    r = c.post("/api/arrivals", headers=h, json=stale_msg(25, 5))
    b = r.json()
    assert b["lag_minutes"] == max(b["lag_source_minutes"], b["lag_ingest_minutes"])
    assert b["lag_source_minutes"] >= 24 and b["lag_ingest_minutes"] >= 4
    assert b["level"] == "告警" and b["output_mode"] == "降级只读"
    # 摄入戳更严：源滞后≈5分、摄入滞后≈25分 → 同样取严≈25分
    r2 = c.post("/api/arrivals", headers=h, json=stale_msg(5, 25, source="支付源"))
    b2 = r2.json()
    assert b2["lag_minutes"] == max(b2["lag_source_minutes"], b2["lag_ingest_minutes"])
    assert b2["level"] == "告警" and b2["output_mode"] == "降级只读"


def test_ledger_pinned_and_deviation_rejected():
    """裁决簿钉死甲路径，当天不得更换；偏离裁决簿即驳回并记双钟偏了。"""
    c = TestClient(web)
    h = login(c)
    pol = c.get("/api/dual-clock/policy", headers=h).json()
    in_force = [p for p in pol["paths"] if p["in_force"]]
    assert len(in_force) == 1, "甲乙两套不得并存"
    assert in_force[0]["key"] == "甲"
    assert pol["ledger"]["pinned_date"] == datetime.now().date().isoformat()
    # 当天更换裁决簿 → 驳回 + 双钟偏了
    r = c.post("/api/dual-clock/policy", headers=h, json={"path": "乙"})
    assert r.status_code == 409
    # 写路径偏离裁决簿（报文自称走乙路径）→ 驳回 + 双钟偏了，不得落库
    r2 = c.post("/api/arrivals", headers=h, json=stale_msg(arbitration_path="乙"))
    assert r2.status_code == 409
    evs = [e for e in events(c, h) if e["event"] == "双钟偏了"]
    assert len(evs) >= 2
    assert len(arrivals_of(c, h)) == 0


# ---------- 五处读数一致 ----------

def test_five_readings_consistent():
    """时钟核对页、到达列表、取严策略页、水位页、源目录五处读数彼此一致。"""
    c = TestClient(web)
    h = login(c)
    c.post("/api/arrivals", headers=h, json=stale_msg(180, 175))

    arr = arrivals_of(c, h)[0]
    cc = {r["source"]: r for r in c.get("/api/dual-clock/clock-check", headers=h).json()["items"]}["订单源"]
    wl = {r["source"]: r for r in c.get("/api/dual-clock/water-level", headers=h).json()["items"]}["订单源"]
    src = {s["name"]: s for s in c.get("/api/sources", headers=h).json()["items"]}["订单源"]
    pol = c.get("/api/dual-clock/policy", headers=h).json()

    assert arr["lag_minutes"] == cc["lag_minutes"] == wl["lag_minutes"]
    assert arr["lag_source_minutes"] == cc["lag_source_minutes"]
    assert arr["lag_ingest_minutes"] == cc["lag_ingest_minutes"]
    assert arr["level"] == cc["level"] == wl["level"] == "违约"
    assert src["output_mode"] == cc["output_mode"] == wl["output_mode"] == "冻结"
    assert [p["key"] for p in pol["paths"] if p["in_force"]] == ["甲"]
