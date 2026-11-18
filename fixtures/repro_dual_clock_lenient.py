#!/usr/bin/env python3
"""复现夹具：两套戳不一致，目录仍按宽松戳放行。

夹具报文（同源采集，一条报文同时带两套戳）：
  - 源侧发出戳 source_clock：约 180 分钟前（数据早已超过 15 分钟 SLA）
  - 摄入侧落库戳 ingest_clock：约 175 分钟前
  两戳互相只差 5 分钟（"两套戳不一致"：数据实际已严重滞后，
  但两戳之间的差很小）。

修复前（宽松口径）：目录只看两戳差 5 分钟 ≤ SLA 15 分钟 → 正常/可写，
  到达列表里两戳都已滞后约 3 小时，源目录却放行 —— 两边互相打架。
取严口径（目标）：取严滞后 = max(源戳滞后, 摄入戳滞后) ≈ 180 分钟
  → 违约/冻结，且与目录输出态同库事务落库。

用法：.venv/bin/python fixtures/repro_dual_clock_lenient.py
退出码恒为 0；输出最后一行标明 REPRODUCED（复现）或 FIXED（已修复）。
"""
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

import app  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402


def main() -> int:
    tmpdb = Path(tempfile.mkdtemp(prefix="dualclock-repro-")) / "repro.db"
    app.DB_PATH = tmpdb
    app.init_db()
    c = TestClient(app.app if hasattr(app, "app") else app)  # app 模块即 FastAPI 实例所在模块
    token = c.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    h = {"Authorization": "Bearer " + token}

    now = datetime.now().replace(microsecond=0)
    msg = {
        "source": "订单源",
        "source_clock": (now - timedelta(minutes=180)).isoformat(),
        "ingest_clock": (now - timedelta(minutes=175)).isoformat(),
    }
    before = {s["name"]: s["output_mode"] for s in c.get("/api/sources", headers=h).json()["items"]}
    r = c.post("/api/arrivals", headers=h, json=msg)
    after = {s["name"]: s["output_mode"] for s in c.get("/api/sources", headers=h).json()["items"]}
    body = r.json()

    print("== 双时钟宽松放行复现夹具 ==")
    print(f"夹具报文: 源={msg['source']}")
    print(f"  源侧发出戳   : {msg['source_clock']}  (滞后约 180 分钟)")
    print(f"  摄入侧落库戳 : {msg['ingest_clock']}  (滞后约 175 分钟)")
    print(f"  两戳差       : 5 分钟  (SLA=15 分钟)")
    print(f"写入前目录输出态: {before['订单源']}")
    print(f"写入回执: {body}")
    print(f"写入后目录输出态: {after['订单源']}")
    print(f"到达列表最新行两戳均已滞后约 3 小时，目录输出态={after['订单源']}")

    if after["订单源"] == "可写":
        print("REPRODUCED: 两套戳不一致（均滞后约180分钟、互差5分钟），目录仍按宽松戳放行(可写)；"
              "取严口径应为 违约/冻结 —— 到达列表与源目录互相打架")
    else:
        print(f"FIXED: 取严口径生效，目录输出态={after['订单源']}（两戳取严后不再放行）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
