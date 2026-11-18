
import hashlib, secrets, sqlite3, threading
from collections import defaultdict
from pathlib import Path
from fastapi import Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))

class LoginIn(BaseModel):
    username: str
    password: str

def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u

from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "源到达SLA"
SKELETON = "多源到达时钟对照 SLA 分级，超限则冻结或降级输出，补齐后恢复并对账"

# ---- 双时钟取严裁决 ----
# 裁决口径二选一，择定后记入双时钟裁决簿钉死，当天不得更换；甲乙不得混用。
ARBITRATION_PATHS = {
    "甲": "同库事务落库：取严结果与目录输出态同事务写入，失败整笔回退",
    "乙": "草稿条件更新：先写取严草稿核对双戳，再条件更新正式到达并改目录输出态",
}
PINNED_PATH = "甲"  # 本仓择定甲路径（同库事务），钉死
STRICT_RULE = ("双时钟取严：取严滞后=max(源侧发出戳滞后, 摄入侧落库戳滞后)，"
               "按取严滞后定级并与目录输出态同库事务落库；宽松口径禁用")
# 同源写锁：两路采集同时推同一源须排队，后到只记双时钟争用
SOURCE_LOCKS: defaultdict[str, threading.Lock] = defaultdict(threading.Lock)

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def ensure_arrival_columns(conn):
    """arrivals 增加双时钟取严列；历史行按宽松口径回填标记。"""
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(arrivals)").fetchall()}
    if "lag_source_minutes" not in cols:
        conn.execute("ALTER TABLE arrivals ADD COLUMN lag_source_minutes INTEGER")
    if "lag_ingest_minutes" not in cols:
        conn.execute("ALTER TABLE arrivals ADD COLUMN lag_ingest_minutes INTEGER")
    if "arbitration" not in cols:
        conn.execute("ALTER TABLE arrivals ADD COLUMN arbitration TEXT")
    conn.execute("""UPDATE arrivals SET lag_source_minutes=lag_minutes,
                    lag_ingest_minutes=lag_minutes, arbitration='历史宽松'
                    WHERE arbitration IS NULL""")

def pin_ledger(conn):
    """双时钟裁决簿：择定口径记入并钉死，当天不得更换。"""
    today = datetime.now().date().isoformat()
    row = conn.execute("SELECT id FROM dual_clock_ledger WHERE pinned_date=?", (today,)).fetchone()
    if not row:
        conn.execute(
            "INSERT INTO dual_clock_ledger(pinned_date,path,path_label,rule,created_at) VALUES (?,?,?,?,?)",
            (today, PINNED_PATH, ARBITRATION_PATHS[PINNED_PATH], STRICT_RULE,
             datetime.now().isoformat(timespec="seconds")))

def current_ledger(conn):
    return conn.execute(
        "SELECT * FROM dual_clock_ledger ORDER BY pinned_date DESC, id DESC LIMIT 1").fetchone()

def log_event(source, event, detail):
    """双时钟留痕（独立事务，不随取严写路径回退）。"""
    with connect() as c:
        c.execute("INSERT INTO dual_clock_events(source,event,detail,created_at) VALUES (?,?,?,?)",
                  (source, event, detail, datetime.now().isoformat(timespec="seconds")))

def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS sources(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT UNIQUE NOT NULL,
          sla_minutes INTEGER NOT NULL,
          output_mode TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS arrivals(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source TEXT NOT NULL,
          source_clock TEXT NOT NULL,
          ingest_clock TEXT NOT NULL,
          lag_minutes INTEGER NOT NULL,
          level TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS catchups(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source TEXT NOT NULL,
          status TEXT NOT NULL,
          restored_mode TEXT NOT NULL,
          note TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS dual_clock_ledger(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          pinned_date TEXT UNIQUE NOT NULL,
          path TEXT NOT NULL,
          path_label TEXT NOT NULL,
          rule TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS dual_clock_events(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source TEXT,
          event TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        """)
        ensure_arrival_columns(conn)
        if conn.execute("SELECT COUNT(*) c FROM sources").fetchone()["c"] == 0:
            conn.executemany("INSERT INTO sources(name,sla_minutes,output_mode) VALUES (?,?,?)", [
                ("订单源", 15, "可写"), ("支付源", 20, "降级只读"), ("物流源", 30, "冻结"),
            ])
            conn.execute("INSERT INTO arrivals(source,source_clock,ingest_clock,lag_minutes,level) VALUES (?,?,?,?,?)",
                         ("支付源","2026-11-16T08:00:00","2026-11-16T08:35:00",35,"告警"))
            conn.execute("INSERT INTO arrivals(source,source_clock,ingest_clock,lag_minutes,level) VALUES (?,?,?,?,?)",
                         ("物流源","2026-11-16T07:00:00","2026-11-16T09:10:00",130,"违约"))
        conn.execute("""UPDATE arrivals SET lag_source_minutes=lag_minutes,
                        lag_ingest_minutes=lag_minutes, arbitration='历史宽松'
                        WHERE arbitration IS NULL""")
        pin_ledger(conn)

def level_of(lag, sla):
    if lag <= sla: return "正常"
    if lag <= sla * 2: return "告警"
    return "违约"

def mode_of(level):
    return {"正常":"可写", "告警":"降级只读", "违约":"冻结"}[level]

@app.on_event("startup")
def _s(): init_db()

@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM sources").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "sources": n, "db": str(DB_PATH)}

@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row: raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok]=row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/sources")
def sources(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM sources ORDER BY id").fetchall()]}

class ArrivalIn(BaseModel):
    source: str
    source_clock: str
    ingest_clock: str
    arbitration_path: str | None = None  # 报文自称的写路径；偏离裁决簿即驳回

def parse_clock(t: str) -> datetime:
    return datetime.fromisoformat(t)

def strict_lags(source_clock: str, ingest_clock: str) -> tuple[int, int, int]:
    """双时钟取严：分别按源戳、摄入戳对裁决时刻计滞后，取两者较严（较大）。"""
    src_dt, ing_dt = parse_clock(source_clock), parse_clock(ingest_clock)
    tz = src_dt.tzinfo or ing_dt.tzinfo
    if tz:
        src_dt = src_dt.replace(tzinfo=tz) if src_dt.tzinfo is None else src_dt
        ing_dt = ing_dt.replace(tzinfo=tz) if ing_dt.tzinfo is None else ing_dt
    now = datetime.now(tz=tz)
    lag_src = max(0, int((now - src_dt).total_seconds() // 60))
    lag_ing = max(0, int((now - ing_dt).total_seconds() // 60))
    return lag_src, lag_ing, max(lag_src, lag_ing)

@app.post("/api/arrivals")
def add_arrival(body: ArrivalIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        led = current_ledger(c)
    # 偏离双时钟裁决簿即驳回并记双钟偏了
    if body.arbitration_path and body.arbitration_path != led["path"]:
        log_event(body.source, "双钟偏了",
                  f"报文自称走{body.arbitration_path}路径，裁决簿已钉死{led['path']}路径，驳回")
        raise HTTPException(409, "双钟偏了：写路径偏离双时钟裁决簿，已驳回")
    # 两路采集同时推同一源须排队，后到只记双时钟争用
    lock = SOURCE_LOCKS[body.source]
    if not lock.acquire(blocking=False):
        log_event(body.source, "双时钟争用", "两路采集同时推同一源，后到仅留痕不写入")
        raise HTTPException(409, "双时钟争用：同源写入进行中，后到仅留痕")
    try:
        try:
            lag_src, lag_ing, lag = strict_lags(body.source_clock, body.ingest_clock)
        except ValueError:
            raise HTTPException(400, "时钟格式错误")
        # 甲路径：取严结果与目录输出态同库事务落库，失败整笔回退
        try:
            with connect() as c:
                src = c.execute("SELECT * FROM sources WHERE name=?", (body.source,)).fetchone()
                if not src: raise HTTPException(404, "未知源")
                lv = level_of(lag, src["sla_minutes"])
                mode = mode_of(lv)
                c.execute("""INSERT INTO arrivals(source,source_clock,ingest_clock,lag_minutes,level,
                             lag_source_minutes,lag_ingest_minutes,arbitration)
                             VALUES (?,?,?,?,?,?,?,?)""",
                          (body.source, body.source_clock, body.ingest_clock, lag, lv,
                           lag_src, lag_ing, "取严"))
                c.execute("UPDATE sources SET output_mode=? WHERE name=?", (mode, body.source))
        except HTTPException:
            raise
        except Exception as e:
            log_event(body.source, "取严失败回退", f"{type(e).__name__}: {e}")
            raise HTTPException(500, "取严落库失败，整笔回退")
        return {"lag_source_minutes": lag_src, "lag_ingest_minutes": lag_ing,
                "lag_minutes": lag, "level": lv, "output_mode": mode,
                "arbitration": "取严", "path": led["path"]}
    finally:
        lock.release()

@app.get("/api/arrivals")
def arrivals(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM arrivals ORDER BY id DESC").fetchall()]}

@app.post("/api/catchup/{source}")
def catchup(source: str, authorization: str | None = Header(default=None)):
    require_user(authorization)
    now = datetime.now().isoformat(timespec="seconds")
    with connect() as c:
        src = c.execute("SELECT * FROM sources WHERE name=?", (source,)).fetchone()
        if not src: raise HTTPException(404, "未知源")
        c.execute("UPDATE sources SET output_mode='可写' WHERE name=?", (source,))
        c.execute("INSERT INTO catchups(source,status,restored_mode,note,created_at) VALUES (?,?,?,?,?)",
                  (source, "成功", "可写", "补齐后恢复可写", now))
        return {"ok": True, "output_mode": "可写"}

@app.get("/api/catchups")
def catchups(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM catchups ORDER BY id DESC").fetchall()]}

# ---- 双时钟取严：裁决簿 / 时钟核对 / 水位 ----

LATEST_ARRIVAL_JOIN = """
    SELECT s.name AS source, s.sla_minutes, s.output_mode,
           a.source_clock, a.ingest_clock,
           a.lag_source_minutes, a.lag_ingest_minutes,
           a.lag_minutes, a.level, a.arbitration
    FROM sources s
    LEFT JOIN arrivals a ON a.id = (SELECT MAX(id) FROM arrivals WHERE source = s.name)
    ORDER BY s.id
"""

class PolicyIn(BaseModel):
    path: str
    rule: str | None = None

@app.get("/api/dual-clock/policy")
def dual_clock_policy(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        led = current_ledger(c)
        evs = [dict(r) for r in c.execute(
            "SELECT * FROM dual_clock_events ORDER BY id DESC LIMIT 50").fetchall()]
    return {
        "ledger": dict(led) if led else None,
        "paths": [{"key": k, "label": v, "in_force": bool(led) and led["path"] == k}
                  for k, v in ARBITRATION_PATHS.items()],
        "events": evs,
    }

@app.post("/api/dual-clock/policy")
def change_dual_clock_policy(body: PolicyIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        led = current_ledger(c)
    log_event(None, "双钟偏了",
              f"试图将裁决簿更换为{body.path}路径，簿已钉死{led['path']}路径({led['pinned_date']})，当天不得更换，驳回")
    raise HTTPException(409, "双钟偏了：裁决簿已钉死，当天不得更换")

@app.get("/api/dual-clock/clock-check")
def dual_clock_check(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items": [dict(r) for r in c.execute(LATEST_ARRIVAL_JOIN).fetchall()]}

@app.get("/api/dual-clock/water-level")
def dual_clock_water_level(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = [dict(r) for r in c.execute(LATEST_ARRIVAL_JOIN).fetchall()]
    for r in rows:
        lag = r["lag_minutes"]
        r["water_ratio"] = round(lag / r["sla_minutes"], 2) if lag is not None else None
    return {"items": rows}

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")
app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
