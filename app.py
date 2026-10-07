#!/usr/bin/env python3
"""共享厨房过敏原交叉污染排程系统

核心规则：
1. 同一台器具的已排入批次时段不得重叠，冲突批次拒绝排入并保留原因；
2. 器具上前一批次留下的残留过敏原中，凡不在本批次配方内的，均构成
   交叉污染风险，切换前必须存在一条清洁记录满足：
   - 清洁时间在前一批次结束之后、本批次开始之前；
   - 清洁有效期（valid_until）覆盖本批次开始时间；
   - 清洁范围覆盖全部新增残留源（或标记为“全部”）；
   缺少清洁 / 清洁失效 / 覆盖不足时，批次被阻塞并保留阻塞原因；
3. 被阻塞批次不占用器具、不产生残留；调整顺序后重新计算即可；
4. 器具停用检修：管理员登记器具、停用起止时间与原因，可修改或取消；
   同一器具的有效停用时段不得重叠，起止无效、器具不存在或记录已取消时
   拒绝变更。批次使用任一器具的时段与有效停用时段相交即阻塞（标明器具
   与冲突时段），端点相接不算相交；受阻批次不占用器具、不留残留。
5. 批量调整试算：排程员一次提交多个批次的新时段/器具与包含全部现有批次的
   目标顺序，系统按现有规则试算完整排程并逐批对比调整前后状态；试算不改动
   实时排程、导出与交接版。确认时若排程依据（批次/器具/清洁/停用/配方）自
   试算后已变化，拒绝过期方案并提示重新试算；否则在同一事务内一次性保存
   全部调整（失败不部分写入），含阻塞批次的方案允许保存，交接版发布规则不变。
6. 清洁补救试算：对当前因清洁缺失/失效/覆盖不足受阻的批次，排程员选择批次
   并输入正整数分钟的清洁时长，系统逐台找出“前一已排入批次结束 → 目标开始”
   区间内扣除停用与占用后、可连续执行清洁的最晚时窗（端点相接不算占用）；
   建议的单条清洁覆盖该器具全部新增残留过敏原，完成时间作为清洁时间，有效期
   覆盖目标开始。任一相关器具时窗不足，或目标还因占用/停用受阻，或加入虚拟
   清洁重算后目标仍阻塞，均判定不可仅靠清洁解决。试算只用虚拟清洁在内存中
   重算完整排程（含其他批次的级联变化），不写入清洁、实时排程或交接版；
   无效批次与非正整数时长一律拒绝。
7. 生产执行：操作员按最新交接版逐批开工——只有批次仍存在、实时排程已排入，
   且顺序、计划时段、器具、过敏原、风险来源与所用清洁均与该版快照一致时
   才记录实际开工时间，否则说明差异且不记开工；同一批次不得重复开工
   （execution.batch_id 唯一约束兜底）。进行中批次可登记实际完工时间，
   早于开工或重复完工均拒绝；执行记录保存所据版本与开完工时间，批次日后
   删除或改排仍可查看。进行中批次的排程依据必须保持不变：批次、器具、
   清洁、停用与批量调整等写操作在事务内对比操作前后的计算结果，会改变
   进行中批次依据的整次拒绝并指出批次；开工、完工与相关写入均在
   BEGIN IMMEDIATE 写事务内判定，避免并发穿透。
"""
import csv
import hashlib
import io
import json
import os
import sqlite3
from datetime import datetime, timedelta

from flask import (Flask, Response, flash, g, redirect, render_template,
                   request, url_for)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DB_PATH", os.path.join(BASE_DIR, "data", "scheduler.db"))
PORT = int(os.environ.get("PORT", "8000"))
APP_URL = os.environ.get("APP_URL", f"http://localhost:{PORT}")

ALLERGENS = ["花生", "坚果", "牛奶", "鸡蛋", "麸质", "大豆",
             "鱼类", "贝类", "芝麻", "芹菜", "芥末", "亚硫酸盐"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS equipment (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS batch (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    allergens TEXT NOT NULL DEFAULT '',
    planned_start TEXT NOT NULL,
    planned_end TEXT NOT NULL,
    seq INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS batch_equipment (
    batch_id INTEGER NOT NULL REFERENCES batch(id) ON DELETE CASCADE,
    equipment_id INTEGER NOT NULL REFERENCES equipment(id) ON DELETE CASCADE,
    PRIMARY KEY (batch_id, equipment_id)
);
CREATE TABLE IF NOT EXISTS cleaning (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    equipment_id INTEGER NOT NULL REFERENCES equipment(id) ON DELETE CASCADE,
    cleaned_at TEXT NOT NULL,
    valid_until TEXT NOT NULL,
    covers TEXT NOT NULL DEFAULT '*'
);
CREATE TABLE IF NOT EXISTS allergen_revision (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id INTEGER NOT NULL REFERENCES batch(id) ON DELETE CASCADE,
    old_allergens TEXT NOT NULL DEFAULT '',
    new_allergens TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL,
    changed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS equipment_downtime (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    equipment_id INTEGER NOT NULL REFERENCES equipment(id) ON DELETE CASCADE,
    start_at TEXT NOT NULL,
    end_at TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT '',
    cancelled_at TEXT
);
CREATE TABLE IF NOT EXISTS cleaning_deletion (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cleaning_id INTEGER NOT NULL,
    equipment_id INTEGER,
    equipment_name TEXT NOT NULL DEFAULT '',
    cleaned_at TEXT NOT NULL DEFAULT '',
    valid_until TEXT NOT NULL DEFAULT '',
    covers TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL,
    deleted_at TEXT NOT NULL,
    affected TEXT NOT NULL DEFAULT '[]'
);
CREATE TABLE IF NOT EXISTS release_version (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    version INTEGER NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    batch_count INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS release_item (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    release_id INTEGER NOT NULL REFERENCES release_version(id) ON DELETE CASCADE,
    position INTEGER NOT NULL,
    batch_id INTEGER,
    batch_name TEXT NOT NULL DEFAULT '',
    allergens TEXT NOT NULL DEFAULT '',
    equipment_names TEXT NOT NULL DEFAULT '',
    planned_start TEXT NOT NULL DEFAULT '',
    planned_end TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'accepted',
    risks TEXT NOT NULL DEFAULT '[]',
    revision_note TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS execution (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    release_id INTEGER NOT NULL REFERENCES release_version(id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    batch_id INTEGER UNIQUE,
    batch_name TEXT NOT NULL DEFAULT '',
    position INTEGER NOT NULL DEFAULT 0,
    allergens TEXT NOT NULL DEFAULT '',
    equipment_names TEXT NOT NULL DEFAULT '',
    planned_start TEXT NOT NULL DEFAULT '',
    planned_end TEXT NOT NULL DEFAULT '',
    actual_start TEXT NOT NULL,
    actual_end TEXT,
    started_at TEXT NOT NULL DEFAULT '',
    finished_at TEXT
);
"""

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "allergen-scheduler-dev-key")


# ---------------------------------------------------------------- 数据库

def get_db():
    if "db" not in g:
        # timeout：发布交接版等写事务遇锁时最多等待 10 秒，避免并发下立即报错
        g.db = sqlite3.connect(DB_PATH, timeout=10)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON")
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    dirpath = os.path.dirname(DB_PATH)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    seed_if_empty(conn)
    conn.close()


def seed_if_empty(conn):
    """首次启动写入演示数据：覆盖 已排入/已缓解/清洁失效/覆盖不足/时段冲突 各情形。"""
    if conn.execute("SELECT COUNT(*) AS c FROM equipment").fetchone()["c"]:
        return
    today = datetime.now().date()

    def t(h, m=0):
        return datetime(today.year, today.month, today.day, h, m).isoformat(timespec="minutes")

    for name in ["台面A", "台面B", "搅拌机", "烤箱"]:
        conn.execute("INSERT INTO equipment(name) VALUES (?)", (name,))
    eq = {r["name"]: r["id"] for r in conn.execute("SELECT * FROM equipment")}

    batches = [
        ("牛奶燕麦糊", "牛奶,麸质", "台面A", t(8), t(9)),
        ("花生能量棒", "花生,大豆", "台面A", t(9, 30), t(10, 30)),
        ("芝麻饼干", "芝麻,麸质", "台面A", t(11), t(12)),
        ("清蒸鲈鱼", "鱼类", "台面B", t(8, 30), t(9, 30)),
        ("贝类意面", "贝类,麸质", "台面B", t(9), t(10)),
        ("香蕉奶昔", "牛奶", "搅拌机", t(8), t(8, 30)),
        ("杏仁奶", "坚果", "搅拌机", t(9), t(9, 30)),
    ]
    for i, (name, alg, eqname, s, e) in enumerate(batches, 1):
        cur = conn.execute(
            "INSERT INTO batch(name, allergens, planned_start, planned_end, seq)"
            " VALUES (?,?,?,?,?)", (name, alg, s, e, i))
        conn.execute("INSERT INTO batch_equipment(batch_id, equipment_id) VALUES (?,?)",
                     (cur.lastrowid, eq[eqname]))

    cleanings = [
        ("台面A", t(9), t(12), "牛奶,麸质"),      # 覆盖 B1→B2 的切换
        ("台面A", t(10, 30), t(11, 30), "花生"),   # 只覆盖花生，未覆盖大豆 → B3 阻塞
        ("搅拌机", t(8, 30), t(8, 45), "*"),        # 09:00 前已失效 → B7 阻塞
    ]
    for eqname, c, v, covers in cleanings:
        conn.execute(
            "INSERT INTO cleaning(equipment_id, cleaned_at, valid_until, covers)"
            " VALUES (?,?,?,?)", (eq[eqname], c, v, covers))

    # 器具停用检修：烤箱 13:00–17:00 停用（当前无批次使用，可在「器具与清洁」
    # 登记/编辑/取消停用记录；批次时段与有效停用时段相交时会被阻塞）
    conn.execute(
        "INSERT INTO equipment_downtime(equipment_id, start_at, end_at, reason,"
        " created_at) VALUES (?,?,?,?,?)",
        (eq["烤箱"], t(13), t(17), "加热管检修，定期保养",
         datetime.now().isoformat(timespec="minutes")))
    conn.commit()


# ---------------------------------------------------------------- 工具

def parse_dt(s):
    try:
        return datetime.fromisoformat(s)
    except (TypeError, ValueError):
        return None


def split_allergens(s):
    return {a.strip() for a in (s or "").split(",") if a.strip()}


def join_alg(lst):
    return "、".join(lst) if lst else "无"


def risk_source_text(risk):
    return (f"器具「{risk['equipment']}」残留 {join_alg(risk['residual'])}"
            f"（来自批次「{risk['from_batch']}」）")


def fmt_dt_min(value):
    """把 datetime 或 ISO 字符串统一格式化为 YYYY-MM-DD HH:MM（仅用于展示）。"""
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat(timespec="minutes").replace("T", " ")
    return str(value).replace("T", " ")


def downtime_block_text(eq_name, start_at, end_at, reason):
    return (f"器具「{eq_name}」停用检修（{fmt_dt_min(start_at)} ~ "
            f"{fmt_dt_min(end_at)}，原因：{reason}），与本批次时段相交，"
            f"检修期间不得安排生产")


def risk_action_text(risk):
    if risk["level"] == "none":
        return "残留过敏原均在本批次配方内，无交叉污染风险"
    if risk["level"] == "mitigated":
        c = risk["cleaning"]
        covers = "全部过敏原" if c["covers"].strip() == "*" else c["covers"]
        return (f"新增风险 {join_alg(risk['new_risks'])}，已由清洁 #{c['id']} 覆盖"
                f"（{c['cleaned_at']} 清洁，有效至 {c['valid_until']}，范围：{covers}）")
    return risk["block_reason"]


def revision_note_text(rev):
    """看板与导出中展示的一行式“最近一次配方变更”说明。"""
    if not rev:
        return ""
    old = join_alg(sorted(split_allergens(rev["old_allergens"])))
    new = join_alg(sorted(split_allergens(rev["new_allergens"])))
    when = (rev["changed_at"] or "").replace("T", " ")
    return (f"配方过敏原最近变更（{when}）：{old} → {new}；"
            f"原因：{rev['reason']}")


def trial_risk_action_text(risk):
    """清洁补救试算结果中风险处置的展示文案（识别只存在于试算中的虚拟清洁）。"""
    if risk["level"] != "mitigated":
        return risk_action_text(risk)
    c = risk["cleaning"]
    if not c.get("virtual"):
        return risk_action_text(risk)
    covers = cleaning_cover_text(c.get("covers"))
    return (f"新增风险 {join_alg(risk['new_risks'])}，可由本试算建议的虚拟清洁覆盖"
            f"（{fmt_dt_min(c['cleaned_at'])} 完成清洁，有效期至 "
            f"{fmt_dt_min(c['valid_until'])}，范围：{covers}）；"
            f"该清洁仅存在于本次试算，未写入系统")


# ---------------------------------------------------------------- 排程引擎

def active_downtimes(conn):
    """读取各器具当前有效的停用时段（未取消且起止时间可解析），按开始时间排序。

    检修期间不得安排生产：批次使用某器具的时段与任一有效停用时段相交即阻塞；
    端点相接（一个结束恰好等于另一个开始）不算相交。
    返回 equipment_id -> [停用记录（含 start_dt/end_dt）]。
    """
    downtimes = {}
    for r in conn.execute(
            "SELECT dt.*, e.name AS equipment_name FROM equipment_downtime dt"
            " JOIN equipment e ON e.id = dt.equipment_id"
            " WHERE dt.cancelled_at IS NULL ORDER BY dt.start_at, dt.id"):
        d = dict(r)
        d["start_dt"] = parse_dt(d["start_at"])
        d["end_dt"] = parse_dt(d["end_at"])
        if d["start_dt"] is None or d["end_dt"] is None:
            continue
        downtimes.setdefault(d["equipment_id"], []).append(d)
    return downtimes


def compute_schedule(conn, exclude_cleaning_id=None, batch_rows=None,
                     equip_of=None, virtual_cleanings=None):
    """按当前顺序（seq, 计划开始时间）逐个尝试排入，返回每批次的结果。

    exclude_cleaning_id 给出的清洁记录在本次计算中视为不存在（规则本身不变），
    用于删除清洁前预演：找出因该记录失效而被阻塞的批次。
    batch_rows / equip_of 给出时，用其代替数据库中的批次与器具关联进行计算
    （清洁、停用、器具主数据仍取自数据库当前值），用于批量调整试算，
    不改动任何已存数据。
    virtual_cleanings 给出时，追加若干只存在于本次计算中的“虚拟清洁”
    （字典含 equipment_id/cleaned_at/valid_until/covers/virtual=True），
    数据库不做任何写入；用于清洁补救试算。
    """
    eq_names = {r["id"]: r["name"] for r in conn.execute("SELECT * FROM equipment")}
    if equip_of is None:
        equip_of = {}
        for r in conn.execute("SELECT * FROM batch_equipment"):
            equip_of.setdefault(r["batch_id"], []).append(r["equipment_id"])
    downtimes = active_downtimes(conn)
    cleanings = {}
    for r in conn.execute("SELECT * FROM cleaning ORDER BY cleaned_at, id"):
        if exclude_cleaning_id is not None and r["id"] == exclude_cleaning_id:
            continue
        c = dict(r)
        c["cleaned_dt"] = parse_dt(c["cleaned_at"])
        c["valid_dt"] = parse_dt(c["valid_until"])
        cleanings.setdefault(c["equipment_id"], []).append(c)
    for c in virtual_cleanings or []:
        c = dict(c)
        c.setdefault("id", None)
        c.setdefault("virtual", True)
        c["cleaned_dt"] = parse_dt(c["cleaned_at"])
        c["valid_dt"] = parse_dt(c["valid_until"])
        cleanings.setdefault(c["equipment_id"], []).append(c)
    # 追加虚拟清洁后各器具内需重新按清洁时间排序（无 id 的虚拟记录用 id=0 兜底）
    for eq_id in cleanings:
        cleanings[eq_id].sort(
            key=lambda c: (c["cleaned_dt"] or datetime.min, c["id"] or 0))
    latest_rev = latest_revisions(conn)

    usage = {}  # equipment_id -> [已排入批次的占用记录]
    results = []
    if batch_rows is None:
        rows = conn.execute(
            "SELECT * FROM batch ORDER BY seq, planned_start, id").fetchall()
    else:
        rows = sorted(batch_rows,
                      key=lambda r: (r["seq"], r["planned_start"], r["id"]))
    for row in rows:
        start = parse_dt(row["planned_start"])
        end = parse_dt(row["planned_end"])
        b_allergens = split_allergens(row["allergens"])
        eqs = sorted(equip_of.get(row["id"], []))
        blocks, risks = [], []

        if start is None or end is None:
            blocks.append("时段格式无效，请使用 YYYY-MM-DDTHH:MM")
        elif end <= start:
            blocks.append("时段无效：结束时间必须晚于开始时间")
        elif not eqs:
            blocks.append("未指定台面/器具，无法排入")
        else:
            for eq in eqs:
                eq_name = eq_names.get(eq, f"#{eq}")
                # 器具检修停用：批次时段与有效停用时段相交即阻塞（端点相接不算相交），
                # 该器具上不再做占用/残留判定（受阻批次本就不占用器具、不留残留）
                dt_conflicts = [d for d in downtimes.get(eq, [])
                                if start < d["end_dt"] and d["start_dt"] < end]
                if dt_conflicts:
                    for d in dt_conflicts:
                        blocks.append(downtime_block_text(
                            eq_name, d["start_dt"], d["end_dt"],
                            d["reason"] or "—"))
                    continue
                usages = usage.get(eq, [])
                conflicts = [u for u in usages
                             if u["start"] < end and start < u["end"]]
                if conflicts:
                    names = "、".join(f"「{u['batch_name']}」" for u in conflicts)
                    blocks.append(
                        f"器具「{eq_name}」时段冲突：与已排入批次 {names} 时间重叠")
                    continue
                prevs = [u for u in usages if u["end"] <= start]
                if not prevs:
                    continue  # 该器具上无前置批次，无残留
                prev = max(prevs, key=lambda u: u["end"])
                residual = sorted(prev["allergens"])
                new_risks = sorted(prev["allergens"] - b_allergens)
                risk = {"equipment": eq_name, "equipment_id": eq,
                        "from_batch": prev["batch_name"],
                        "prev_end": prev["end"],
                        "residual": residual, "new_risks": new_risks,
                        "cleaning": None, "level": "none", "block_reason": None}
                if not new_risks:
                    risks.append(risk)
                    continue
                # 切换过敏原：需要覆盖全部残留源的有效清洁
                cand = [c for c in cleanings.get(eq, [])
                        if c["cleaned_dt"] and c["valid_dt"]
                        and prev["end"] <= c["cleaned_dt"] <= start]
                valid = [c for c in cand if c["valid_dt"] >= start]
                covering = [c for c in valid
                            if c["covers"].strip() == "*"
                            or set(new_risks) <= split_allergens(c["covers"])]
                src = join_alg(new_risks)
                if covering:
                    risk["level"] = "mitigated"
                    risk["cleaning"] = covering[-1]
                else:
                    risk["level"] = "blocked"
                    if not cand:
                        reason = (f"器具「{eq_name}」残留过敏原 {src}"
                                  f"（来自批次「{prev['batch_name']}」）："
                                  f"切换过敏原前缺少覆盖全部残留源的清洁记录")
                    elif not valid:
                        reason = (f"器具「{eq_name}」残留过敏原 {src}"
                                  f"（来自批次「{prev['batch_name']}」）："
                                  f"清洁已失效（有效期早于本批次开始时间）")
                    else:
                        reason = (f"器具「{eq_name}」残留过敏原 {src}"
                                  f"（来自批次「{prev['batch_name']}」）："
                                  f"现有清洁未覆盖全部残留源")
                    risk["block_reason"] = reason
                    blocks.append(reason)
                risks.append(risk)

        status = "blocked" if blocks else "accepted"
        if status == "accepted":  # 只有排入成功的批次才占用器具、留下残留
            for eq in eqs:
                usage.setdefault(eq, []).append({
                    "start": start, "end": end,
                    "batch_name": row["name"], "allergens": b_allergens})
        results.append({
            "id": row["id"], "name": row["name"],
            "allergens": sorted(b_allergens),
            "equipment_names": [eq_names.get(e, f"#{e}") for e in eqs],
            "start": start, "end": end,
            "status": status, "risks": risks, "blocks": blocks,
            "latest_revision": latest_rev.get(row["id"]),
        })
    return results


def latest_revisions(conn):
    """每个批次最近一次过敏原修订（按记录 id 最大者，即最后保存的那次）。"""
    latest = {}
    for r in conn.execute(
            "SELECT * FROM allergen_revision ar"
            " WHERE ar.id = (SELECT MAX(id) FROM allergen_revision"
            "                WHERE batch_id = ar.batch_id)"):
        latest[r["batch_id"]] = dict(r)
    return latest


def normalize_seq(conn):
    """把批次顺序规范化为 1..N（不写事务，由调用方在事务内统一提交）。"""
    rows = conn.execute(
        "SELECT id FROM batch ORDER BY seq, planned_start, id").fetchall()
    for i, r in enumerate(rows, 1):
        conn.execute("UPDATE batch SET seq=? WHERE id=?", (i, r["id"]))


def cleaning_cover_text(covers):
    return "全部过敏原" if (covers or "").strip() == "*" else (covers or "").replace(",", "、")


def affected_of_cleaning(conn, cid):
    """删除清洁记录前按当前排程预演。

    返回 (cleaning, affected)：cleaning 为该记录行（不存在则为 None）；
    affected 为因该记录失效而由“已排入”变为“阻塞”的批次列表，
    每项包含批次信息、新阻塞原因，以及原先由本清洁缓解的风险说明。
    预演只把该记录视为不存在，排程规则仍由 compute_schedule 统一执行，
    因此级联影响（前序批次失占器具导致后续批次失去残留依据等）也会自然体现。
    """
    cleaning = conn.execute(
        "SELECT c.*, e.name AS equipment_name FROM cleaning c"
        " JOIN equipment e ON e.id = c.equipment_id WHERE c.id=?",
        (cid,)).fetchone()
    if cleaning is None:
        return None, []
    before = {r["id"]: r for r in compute_schedule(conn)}
    after = {r["id"]: r for r in compute_schedule(conn, exclude_cleaning_id=cid)}
    affected = []
    for bid, a in after.items():
        b = before.get(bid)
        if b is None or b["status"] != "accepted" or a["status"] != "blocked":
            continue
        mitigated = []
        for risk in b["risks"]:
            if risk["level"] == "mitigated" and risk["cleaning"] \
                    and risk["cleaning"]["id"] == cid:
                mitigated.append({
                    "equipment": risk["equipment"],
                    "from_batch": risk["from_batch"],
                    "new_risks": risk["new_risks"],
                    "summary": (f"器具「{risk['equipment']}」上来自批次"
                                f"「{risk['from_batch']}」的新增风险"
                                f" {join_alg(risk['new_risks'])} 原由本清洁覆盖"),
                })
        affected.append({
            "batch_id": a["id"],
            "batch_name": a["name"],
            "equipment_names": a["equipment_names"],
            "prev_status": "已排入",
            "new_status": "已阻塞",
            "block_reasons": a["blocks"],
            "mitigated_here": mitigated,
        })
    return cleaning, affected


def cleaning_deletion_rows(conn):
    """读取清洁删除审计记录，affected 快照解析为列表供页面/导出使用。"""
    rows = []
    for r in conn.execute(
            "SELECT cd.*, e.name AS equipment_name_now FROM cleaning_deletion cd"
            " LEFT JOIN equipment e ON e.id = cd.equipment_id"
            " ORDER BY cd.id DESC"):
        d = dict(r)
        try:
            d["affected_list"] = json.loads(d.get("affected") or "[]")
        except (TypeError, ValueError):
            d["affected_list"] = []
        rows.append(d)
    return rows


# ------------------------------------------------------------ 器具停用检修

def downtime_rows(conn, include_cancelled=True):
    """读取停用记录（含器具名与取消状态），默认按登记时间倒序（新记录在前）。"""
    sql = ("SELECT dt.*, e.name AS equipment_name FROM equipment_downtime dt"
           " JOIN equipment e ON e.id = dt.equipment_id")
    if not include_cancelled:
        sql += " WHERE dt.cancelled_at IS NULL"
    sql += " ORDER BY dt.id DESC"
    return [dict(r) for r in conn.execute(sql)]


def validate_downtime(conn, equipment_id, start_str, end_str, reason,
                      exclude_id=None):
    """校验停用登记/修改，返回 (equipment_row, start_dt, end_dt, error)。

    校验项：器具存在；停用原因非空；起止时间可解析、结束晚于开始；
    与同一器具的其他有效（未取消）停用时段不得重叠（端点相接允许）。
    exclude_id 给出当前修改的记录自身，不参与重叠比较。
    """
    try:
        eid = int(equipment_id)
    except (TypeError, ValueError):
        return None, None, None, "请选择有效的器具"
    equipment = conn.execute(
        "SELECT * FROM equipment WHERE id=?", (eid,)).fetchone()
    if equipment is None:
        return None, None, None, "器具不存在或已删除，无法登记停用"
    if not reason:
        return None, None, None, "请填写停用原因（如：加热管检修）"
    start_dt = parse_dt(start_str)
    end_dt = parse_dt(end_str)
    if start_dt is None or end_dt is None:
        return None, None, None, "停用起止时间无效，请使用 YYYY-MM-DDTHH:MM"
    if end_dt <= start_dt:
        return None, None, None, "停用时段无效：结束时间必须晚于开始时间"
    other = conn.execute(
        "SELECT * FROM equipment_downtime WHERE equipment_id=?"
        " AND cancelled_at IS NULL" + (" AND id<>?" if exclude_id else ""),
        (eid, exclude_id) if exclude_id else (eid,)).fetchall()
    for r in other:
        s, e = parse_dt(r["start_at"]), parse_dt(r["end_at"])
        if s is not None and e is not None and start_dt < e and s < end_dt:
            return None, None, None, (
                f"同一器具的有效停用时段不得重叠：与停用记录 #{r['id']}"
                f"（{fmt_dt_min(s)} ~ {fmt_dt_min(e)}）时间重叠")
    return equipment, start_dt, end_dt, None



# ---------------------------------------------------------------- 页面路由

@app.context_processor
def inject_globals():
    return {"app_url": APP_URL, "port": PORT, "allergen_options": ALLERGENS,
            "risk_source_text": risk_source_text,
            "risk_action_text": risk_action_text,
            "trial_risk_action_text": trial_risk_action_text,
            "revision_note_text": revision_note_text,
            "join_alg": join_alg}


@app.route("/")
def index():
    conn = get_db()
    results = compute_schedule(conn)
    accepted = sum(1 for r in results if r["status"] == "accepted")
    latest = latest_release(conn)
    items = release_items(conn, latest["id"]) if latest else []
    diff, removed_ids = (diff_against_release(results, items)
                         if latest else ({}, set()))
    executions = execution_by_batch(conn)
    exec_stats = None
    if latest:
        exec_stats = {"done": 0, "doing": 0, "todo": 0}
        for it in items:
            ex = executions.get(it["batch_id"])
            if ex is None:
                exec_stats["todo"] += 1
            elif ex["actual_end"]:
                exec_stats["done"] += 1
            else:
                exec_stats["doing"] += 1
    return render_template("index.html", results=results,
                           accepted=accepted,
                           blocked_count=len(results) - accepted,
                           active_downtime_rows=downtime_rows(
                               conn, include_cancelled=False),
                           cleaning_deletions=cleaning_deletion_rows(conn),
                           latest_release=latest, release_items=items,
                           release_diff=diff, removed_ids=removed_ids,
                           executions=executions, exec_stats=exec_stats)


@app.post("/recalculate")
def recalculate():
    flash("已按当前顺序、器具占用与清洁有效期重新计算排程", "ok")
    return redirect(url_for("index"))


@app.route("/batches", methods=["GET", "POST"])
def batches():
    conn = get_db()
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        allergens = request.form.getlist("allergens")
        equipment = request.form.getlist("equipment")
        start = request.form.get("planned_start", "")
        end = request.form.get("planned_end", "")
        if not name or not start or not end:
            flash("请填写批次名称与预计时段", "error")
            return redirect(url_for("batches"))
        try:
            conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError:
            flash("系统正忙，请稍后重试录入批次", "error")
            return redirect(url_for("batches"))
        try:
            before = inprogress_guard_snapshot(conn)
            seq = conn.execute(
                "SELECT COALESCE(MAX(seq),0)+1 AS s FROM batch").fetchone()["s"]
            cur = conn.execute(
                "INSERT INTO batch(name, allergens, planned_start, planned_end, seq)"
                " VALUES (?,?,?,?,?)",
                (name, ",".join(allergens), start, end, seq))
            for eq in equipment:
                conn.execute("INSERT INTO batch_equipment VALUES (?,?)",
                             (cur.lastrowid, int(eq)))
            violated = inprogress_violations(conn, before)
            if violated:
                conn.rollback()
                flash(guard_reject_text(violated, prefix="未录入"), "error")
                return redirect(url_for("batches"))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        flash(f"批次「{name}」已录入，当前排第 {seq} 位", "ok")
        return redirect(url_for("batches"))
    rows = conn.execute("SELECT * FROM batch ORDER BY seq").fetchall()
    eq_names = {r["id"]: r["name"]
                for r in conn.execute("SELECT * FROM equipment")}
    equip_of = {}
    for r in conn.execute("SELECT * FROM batch_equipment"):
        equip_of.setdefault(r["batch_id"], []).append(eq_names.get(r["equipment_id"]))
    equipment = conn.execute("SELECT * FROM equipment ORDER BY id").fetchall()
    revisions = conn.execute(
        "SELECT ar.*, b.name AS batch_name FROM allergen_revision ar"
        " JOIN batch b ON b.id = ar.batch_id"
        " ORDER BY ar.id DESC").fetchall()
    return render_template("batches.html", rows=rows, equipment=equipment,
                           equip_of=equip_of, revisions=revisions)


@app.post("/batches/<int:bid>/allergens")
def revise_allergens(bid):
    """修改批次配方过敏原：必须填写变更原因与时间，保存前后集合的修订记录。

    只更新 batch.allergens 并追加修订行；批次顺序、器具关联与清洁记录不受影响，
    重新计算排程时自然使用最新集合。
    """
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError:
        flash("系统正忙，请稍后重试修改过敏原", "error")
        return redirect(url_for("batches"))
    try:
        row = conn.execute("SELECT * FROM batch WHERE id=?", (bid,)).fetchone()
        if row is None:
            conn.rollback()
            flash("批次不存在或已删除", "error")
            return redirect(url_for("batches"))
        reason = request.form.get("reason", "").strip()
        changed_at = request.form.get("changed_at", "").strip()
        new_list = [a.strip() for a in request.form.getlist("allergens") if a.strip()]
        old_set, new_set = split_allergens(row["allergens"]), set(new_list)
        error = None
        if not reason:
            error = "未保存：修改过敏原必须填写变更原因"
        elif not changed_at or parse_dt(changed_at) is None:
            error = "未保存：请填写有效的变更时间"
        elif new_set == old_set:
            error = "未保存：过敏原集合无实际变化"
        if error:
            conn.rollback()
            flash(error, "error")
            return redirect(url_for("batches"))
        before = inprogress_guard_snapshot(conn)
        new_str = ",".join(new_list)
        conn.execute("UPDATE batch SET allergens=? WHERE id=?", (new_str, bid))
        conn.execute(
            "INSERT INTO allergen_revision"
            "(batch_id, old_allergens, new_allergens, reason, changed_at)"
            " VALUES (?,?,?,?,?)",
            (bid, row["allergens"], new_str, reason, changed_at))
        violated = inprogress_violations(conn, before)
        if violated:
            conn.rollback()
            flash(guard_reject_text(violated), "error")
            return redirect(url_for("batches"))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    flash(f"批次「{row['name']}」过敏原已更新"
          f"（{join_alg(sorted(old_set))} → {join_alg(sorted(new_set))}），"
          f"修订记录已保存，重新计算排程将使用最新集合", "ok")
    return redirect(url_for("batches"))


@app.post("/batches/<int:bid>/delete")
def delete_batch(bid):
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError:
        flash("系统正忙，请稍后重试删除批次", "error")
        return redirect(url_for("batches"))
    try:
        row = conn.execute("SELECT * FROM batch WHERE id=?", (bid,)).fetchone()
        if row is None:
            conn.rollback()
            flash("批次不存在或已删除", "error")
            return redirect(url_for("batches"))
        before = inprogress_guard_snapshot(conn)
        conn.execute("DELETE FROM batch WHERE id=?", (bid,))
        normalize_seq(conn)
        violated = inprogress_violations(conn, before)
        if violated:
            conn.rollback()
            flash(guard_reject_text(violated, prefix="未删除"), "error")
            return redirect(url_for("batches"))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    flash("批次已删除", "ok")
    return redirect(url_for("batches"))


@app.post("/batches/<int:bid>/move/<direction>")
def move_batch(bid, direction):
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError:
        flash("系统正忙，请稍后重试调整顺序", "error")
        return redirect(url_for("index"))
    try:
        before = inprogress_guard_snapshot(conn)
        normalize_seq(conn)
        rows = conn.execute("SELECT id, seq FROM batch ORDER BY seq").fetchall()
        ids = [r["id"] for r in rows]
        moved = False
        if bid in ids:
            i = ids.index(bid)
            j = i - 1 if direction == "up" else i + 1
            if 0 <= j < len(ids):
                a, b = rows[i], rows[j]
                conn.execute("UPDATE batch SET seq=? WHERE id=?",
                             (b["seq"], a["id"]))
                conn.execute("UPDATE batch SET seq=? WHERE id=?",
                             (a["seq"], b["id"]))
                moved = True
        if moved:
            violated = inprogress_violations(conn, before)
            if violated:
                conn.rollback()
                flash(guard_reject_text(violated, prefix="未调整顺序"), "error")
                return redirect(url_for("index"))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    if moved:
        flash("已调整顺序并重新计算排程", "ok")
    return redirect(url_for("index"))


@app.route("/equipment", methods=["GET", "POST"])
def equipment_page():
    conn = get_db()
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        if not name:
            return redirect(url_for("equipment_page"))
        try:
            conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError:
            flash("系统正忙，请稍后重试添加器具", "error")
            return redirect(url_for("equipment_page"))
        try:
            before = inprogress_guard_snapshot(conn)
            try:
                conn.execute("INSERT INTO equipment(name) VALUES (?)", (name,))
            except sqlite3.IntegrityError:
                conn.rollback()
                flash(f"器具「{name}」已存在", "error")
                return redirect(url_for("equipment_page"))
            violated = inprogress_violations(conn, before)
            if violated:
                conn.rollback()
                flash(guard_reject_text(violated, prefix="未添加器具"), "error")
                return redirect(url_for("equipment_page"))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        flash(f"器具「{name}」已添加", "ok")
        return redirect(url_for("equipment_page"))
    equipment = conn.execute("SELECT * FROM equipment ORDER BY id").fetchall()
    cleanings = conn.execute(
        "SELECT c.*, e.name AS equipment_name FROM cleaning c"
        " JOIN equipment e ON e.id = c.equipment_id"
        " ORDER BY c.cleaned_at DESC, c.id DESC").fetchall()
    return render_template("equipment.html", equipment=equipment,
                           cleanings=cleanings,
                           downtimes=downtime_rows(conn))


@app.post("/equipment/<int:eid>/delete")
def delete_equipment(eid):
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError:
        flash("系统正忙，请稍后重试删除器具", "error")
        return redirect(url_for("equipment_page"))
    try:
        row = conn.execute("SELECT * FROM equipment WHERE id=?", (eid,)).fetchone()
        if row is None:
            conn.rollback()
            flash("器具不存在或已删除", "error")
            return redirect(url_for("equipment_page"))
        before = inprogress_guard_snapshot(conn)
        conn.execute("DELETE FROM equipment WHERE id=?", (eid,))
        violated = inprogress_violations(conn, before)
        if violated:
            conn.rollback()
            flash(guard_reject_text(violated, prefix="未删除"), "error")
            return redirect(url_for("equipment_page"))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    flash("器具已删除（其清洁记录与批次关联一并移除）", "ok")
    return redirect(url_for("equipment_page"))


@app.post("/cleanings")
def add_cleaning():
    conn = get_db()
    equipment_id = request.form.get("equipment_id")
    cleaned_at = request.form.get("cleaned_at", "")
    valid_until = request.form.get("valid_until", "")
    covers = request.form.getlist("covers")
    cover_all = request.form.get("cover_all")
    if not (equipment_id and cleaned_at and valid_until):
        flash("请完整填写清洁记录（器具、清洁时间、有效至）", "error")
        return redirect(url_for("equipment_page"))
    covers_str = "*" if cover_all else ",".join(covers)
    if not covers_str:
        flash("请选择清洁覆盖的过敏原，或勾选“全部过敏原”", "error")
        return redirect(url_for("equipment_page"))
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError:
        flash("系统正忙，请稍后重试登记清洁", "error")
        return redirect(url_for("equipment_page"))
    try:
        before = inprogress_guard_snapshot(conn)
        conn.execute(
            "INSERT INTO cleaning(equipment_id, cleaned_at, valid_until, covers)"
            " VALUES (?,?,?,?)",
            (equipment_id, cleaned_at, valid_until, covers_str))
        violated = inprogress_violations(conn, before)
        if violated:
            conn.rollback()
            flash(guard_reject_text(violated, prefix="未登记清洁"), "error")
            return redirect(url_for("equipment_page"))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    flash("清洁记录已登记", "ok")
    return redirect(url_for("equipment_page"))


@app.route("/cleanings/<int:cid>/delete", methods=["GET", "POST"])
def delete_cleaning(cid):
    """删除清洁记录：

    GET  按当前排程预演，列出因该记录失效而由“已排入/风险已缓解”变为
         阻塞的批次及原因，要求操作者填写删除原因后确认；
    POST 校验记录仍存在、删除原因非空，随后在同一事务内保存删除时间、
         原因与受影响批次快照并删除记录。记录不存在或原因为空时拒绝，
         不改动任何数据。取消（返回）同样不产生改动。
    """
    conn = get_db()
    if request.method == "POST":
        reason = request.form.get("reason", "").strip()
        try:
            conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError:
            flash("系统正忙，请稍后重试删除清洁记录", "error")
            return redirect(url_for("equipment_page"))
        try:
            cleaning = conn.execute(
                "SELECT c.*, e.name AS equipment_name FROM cleaning c"
                " JOIN equipment e ON e.id = c.equipment_id WHERE c.id=?",
                (cid,)).fetchone()
            if cleaning is None:
                conn.rollback()
                flash("清洁记录不存在或已被删除，未改动任何数据", "error")
                return redirect(url_for("equipment_page"))
            if not reason:
                conn.rollback()
                # 拒绝操作：保留原数据，回到确认页并附原因必填提示
                cleaning_preview, affected = affected_of_cleaning(conn, cid)
                flash("未删除：请填写删除原因并确认", "error")
                return render_template(
                    "cleaning_delete.html", cleaning=cleaning_preview,
                    affected=affected, reason_value=reason), 400
            before = inprogress_guard_snapshot(conn)
            # 确认前再按当前排程计算一次受影响批次并保存快照
            _, affected = affected_of_cleaning(conn, cid)
            deleted_at = datetime.now().isoformat(timespec="minutes")
            conn.execute(
                "INSERT INTO cleaning_deletion(cleaning_id, equipment_id,"
                " equipment_name, cleaned_at, valid_until, covers, reason,"
                " deleted_at, affected) VALUES (?,?,?,?,?,?,?,?,?)",
                (cleaning["id"], cleaning["equipment_id"],
                 cleaning["equipment_name"], cleaning["cleaned_at"],
                 cleaning["valid_until"], cleaning["covers"], reason,
                 deleted_at, json.dumps(affected, ensure_ascii=False)))
            conn.execute("DELETE FROM cleaning WHERE id=?", (cid,))
            violated = inprogress_violations(conn, before)
            if violated:
                conn.rollback()
                flash(guard_reject_text(violated, prefix="未删除"), "error")
                return redirect(url_for("equipment_page"))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        if affected:
            names = "、".join(f"「{a['batch_name']}」" for a in affected)
            flash(f"清洁记录已删除并留档；以下批次因该记录失效转为阻塞：{names}。"
                  f"看板已按现有规则重新计算", "ok")
        else:
            flash("清洁记录已删除并留档；当前排程无批次因此变为阻塞", "ok")
        return redirect(url_for("index"))

    cleaning, affected = affected_of_cleaning(conn, cid)
    if cleaning is None:
        flash("清洁记录不存在或已被删除，未改动任何数据", "error")
        return redirect(url_for("equipment_page"))
    return render_template("cleaning_delete.html", cleaning=cleaning,
                           affected=affected, reason_value="")


# ------------------------------------------------------------ 器具停用路由

@app.post("/downtimes")
def add_downtime():
    """管理员登记器具停用检修：器具、停用起止时间与原因。

    器具不存在、起止无效、原因为空或与同一器具其他有效停用时段重叠时
    拒绝登记；排程看板随后按有效停用时段阻塞相交批次。
    """
    conn = get_db()
    equipment_id = request.form.get("equipment_id", "")
    start_str = (request.form.get("start_at", "") or "").strip()
    end_str = (request.form.get("end_at", "") or "").strip()
    reason = (request.form.get("reason", "") or "").strip()
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError:
        flash("系统正忙，请稍后重试登记停用", "error")
        return redirect(url_for("equipment_page"))
    try:
        equipment, start_dt, end_dt, error = validate_downtime(
            conn, equipment_id, start_str, end_str, reason)
        if error:
            conn.rollback()
            flash(f"未登记停用：{error}", "error")
            return redirect(url_for("equipment_page"))
        before = inprogress_guard_snapshot(conn)
        conn.execute(
            "INSERT INTO equipment_downtime(equipment_id, start_at, end_at,"
            " reason, created_at) VALUES (?,?,?,?,?)",
            (equipment["id"], start_dt.isoformat(timespec="minutes"),
             end_dt.isoformat(timespec="minutes"), reason,
             datetime.now().isoformat(timespec="minutes")))
        violated = inprogress_violations(conn, before)
        if violated:
            conn.rollback()
            flash(guard_reject_text(violated, prefix="未登记停用"), "error")
            return redirect(url_for("equipment_page"))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    flash(f"器具「{equipment['name']}」停用检修时段已登记"
          f"（{fmt_dt_min(start_dt)} ~ {fmt_dt_min(end_dt)}），"
          f"排程已按最新停用状态计算", "ok")
    return redirect(url_for("equipment_page"))


@app.post("/downtimes/<int:did>/edit")
def edit_downtime(did):
    """修改停用记录的器具、起止时间与原因；已取消的记录拒绝变更。"""
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError:
        flash("系统正忙，请稍后重试修改停用记录", "error")
        return redirect(url_for("equipment_page"))
    try:
        row = conn.execute("SELECT * FROM equipment_downtime WHERE id=?",
                           (did,)).fetchone()
        if row is None:
            conn.rollback()
            flash("停用记录不存在或已删除，未改动任何数据", "error")
            return redirect(url_for("equipment_page"))
        if row["cancelled_at"] is not None:
            conn.rollback()
            flash("停用记录已取消，不能再修改；请重新登记停用时段", "error")
            return redirect(url_for("equipment_page"))
        equipment_id = request.form.get("equipment_id", "")
        start_str = (request.form.get("start_at", "") or "").strip()
        end_str = (request.form.get("end_at", "") or "").strip()
        reason = (request.form.get("reason", "") or "").strip()
        equipment, start_dt, end_dt, error = validate_downtime(
            conn, equipment_id, start_str, end_str, reason, exclude_id=did)
        if error:
            conn.rollback()
            flash(f"未保存修改：{error}", "error")
            return redirect(url_for("equipment_page"))
        before = inprogress_guard_snapshot(conn)
        conn.execute(
            "UPDATE equipment_downtime SET equipment_id=?, start_at=?,"
            " end_at=?, reason=? WHERE id=?",
            (equipment["id"], start_dt.isoformat(timespec="minutes"),
             end_dt.isoformat(timespec="minutes"), reason, did))
        violated = inprogress_violations(conn, before)
        if violated:
            conn.rollback()
            flash(guard_reject_text(violated, prefix="未保存修改"), "error")
            return redirect(url_for("equipment_page"))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    flash(f"停用记录 #{did} 已更新，排程已按最新停用状态计算", "ok")
    return redirect(url_for("equipment_page"))


@app.post("/downtimes/<int:did>/cancel")
def cancel_downtime(did):
    """取消停用记录：取消后不再阻塞排程；记录本身保留并标注取消状态。

    记录不存在或已取消时拒绝变更；取消操作不改动停用原因与历史登记内容。
    """
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError:
        flash("系统正忙，请稍后重试取消停用", "error")
        return redirect(url_for("equipment_page"))
    try:
        row = conn.execute("SELECT * FROM equipment_downtime WHERE id=?",
                           (did,)).fetchone()
        if row is None:
            conn.rollback()
            flash("停用记录不存在或已删除，未改动任何数据", "error")
            return redirect(url_for("equipment_page"))
        if row["cancelled_at"] is not None:
            conn.rollback()
            flash("该停用记录此前已取消，无需重复取消", "error")
            return redirect(url_for("equipment_page"))
        before = inprogress_guard_snapshot(conn)
        conn.execute(
            "UPDATE equipment_downtime SET cancelled_at=? WHERE id=?",
            (datetime.now().isoformat(timespec="minutes"), did))
        violated = inprogress_violations(conn, before)
        if violated:
            conn.rollback()
            flash(guard_reject_text(violated, prefix="未取消停用"), "error")
            return redirect(url_for("equipment_page"))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    flash(f"停用记录 #{did} 已取消，该时段不再阻塞排程", "ok")
    return redirect(url_for("equipment_page"))


# ---------------------------------------------------------- 批量调整试算

# 排程依据指纹覆盖的表：批次（含配方过敏原、时段、顺序）、批次-器具关联、
# 器具、清洁记录、器具停用。确认方案时重算指纹并与试算时比对，
# 任何一项变化都说明方案所依据的数据已过期。
BASIS_QUERIES = (
    ("batch", "SELECT id, name, allergens, planned_start, planned_end, seq"
              " FROM batch ORDER BY id"),
    ("batch_equipment", "SELECT batch_id, equipment_id FROM batch_equipment"
                        " ORDER BY batch_id, equipment_id"),
    ("equipment", "SELECT id, name FROM equipment ORDER BY id"),
    ("cleaning", "SELECT id, equipment_id, cleaned_at, valid_until, covers"
                 " FROM cleaning ORDER BY id"),
    ("equipment_downtime", "SELECT id, equipment_id, start_at, end_at, reason,"
                           " created_at, cancelled_at"
                           " FROM equipment_downtime ORDER BY id"),
)


def schedule_basis_fingerprint(conn):
    """当前排程依据（批次/器具/清洁/停用/配方）的指纹，用于检测过期方案。"""
    h = hashlib.sha256()
    for table, sql in BASIS_QUERIES:
        h.update(table.encode("utf-8"))
        for row in conn.execute(sql):
            h.update(repr(tuple(row)).encode("utf-8"))
    return h.hexdigest()


def parse_plan_form(form):
    """解析批量调整表单，返回 (order_ids, adjustments, error)。

    order_ids 为目标顺序中的批次 ID 列表；adjustments 为
    {批次ID: {"start", "end", "equipment"}}（仅勾选“调整”的批次）。
    此处只做格式解析，存在性与业务校验由 validate_plan 完成。
    """
    raw = (form.get("order") or "").replace("，", ",").replace("\n", ",")
    order_ids = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            order_ids.append(int(token))
        except ValueError:
            return None, None, (f"目标顺序包含无法识别的内容「{token}」，"
                                f"请填写批次 ID 并以逗号分隔")
    adjustments = {}
    for bid_str in form.getlist("adjust"):
        try:
            bid = int(bid_str)
        except ValueError:
            return None, None, "调整对象包含无效的批次 ID"
        start = (form.get(f"start_{bid}") or "").strip()
        end = (form.get(f"end_{bid}") or "").strip()
        eqs = []
        for e in form.getlist(f"eq_{bid}"):
            try:
                eqs.append(int(e))
            except ValueError:
                return None, None, f"批次 #{bid} 的器具选择无效"
        adjustments[bid] = {"start": start, "end": end,
                            "equipment": sorted(set(eqs))}
    return order_ids, adjustments, None


def validate_plan(conn, order_ids, adjustments):
    """校验调整方案，返回错误说明（None 表示通过）。

    拒绝情形：没有任何批次；目标顺序重复、缺失或包含不存在的批次；
    调整对象不存在；新时段无法解析或结束不晚于开始；所选器具不存在。
    """
    batches = {r["id"]: r for r in conn.execute("SELECT * FROM batch ORDER BY id")}
    if not batches:
        return "当前没有任何批次，无可调整的排程"
    dups, seen = [], set()
    for bid in order_ids:
        if bid in seen and bid not in dups:
            dups.append(bid)
        seen.add(bid)
    if dups:
        return "目标顺序存在重复批次：" + "、".join(f"#{b}" for b in dups)
    unknown = [b for b in order_ids if b not in batches]
    if unknown:
        return "目标顺序包含不存在的批次：" + "、".join(f"#{b}" for b in unknown)
    missing = [b for b in batches if b not in seen]
    if missing:
        names = "、".join(f"「{batches[b]['name']}」#{b}" for b in missing)
        return f"目标顺序必须包含全部现有批次，缺失：{names}"
    for bid in adjustments:
        if bid not in batches:
            return f"调整对象包含不存在的批次 #{bid}"
    eq_ids = {r["id"] for r in conn.execute("SELECT id FROM equipment")}
    for bid, adj in adjustments.items():
        name = batches[bid]["name"]
        for eid in adj["equipment"]:
            if eid not in eq_ids:
                return f"批次「{name}」选择的器具 #{eid} 不存在或已删除"
        start, end = parse_dt(adj["start"]), parse_dt(adj["end"])
        if start is None or end is None:
            return f"批次「{name}」的新时段无效，请使用 YYYY-MM-DDTHH:MM"
        if end <= start:
            return f"批次「{name}」的新时段无效：结束时间必须晚于开始时间"
    return None


def trial_schedule(conn, order_ids, adjustments):
    """按调整方案试算完整排程（不写入任何数据）。

    order_ids 为包含全部批次的目标顺序；adjustments 给出被调整批次的新时段
    与新器具集合，其余批次保持现有时段与器具。清洁、停用与器具主数据取当前值。
    """
    batches = {r["id"]: dict(r) for r in conn.execute("SELECT * FROM batch")}
    equip_of = {}
    for r in conn.execute("SELECT * FROM batch_equipment"):
        equip_of.setdefault(r["batch_id"], []).append(r["equipment_id"])
    rows = []
    for i, bid in enumerate(order_ids, 1):
        b = dict(batches[bid])
        b["seq"] = i
        adj = adjustments.get(bid)
        if adj is not None:
            b["planned_start"] = adj["start"]
            b["planned_end"] = adj["end"]
            equip_of[bid] = list(adj["equipment"])
        rows.append(b)
    return compute_schedule(conn, batch_rows=rows, equip_of=equip_of)


def reschedule_form_context(conn, prefill=None):
    """批量调整表单所需的数据；prefill 给出时按用户已填内容回显。"""
    batches = conn.execute(
        "SELECT * FROM batch ORDER BY seq, planned_start, id").fetchall()
    equipment = conn.execute("SELECT * FROM equipment ORDER BY id").fetchall()
    eq_name = {e["id"]: e["name"] for e in equipment}
    equip_of = {}
    for r in conn.execute("SELECT * FROM batch_equipment"):
        equip_of.setdefault(r["batch_id"], []).append(r["equipment_id"])
    equip_names_of = {bid: [eq_name.get(i, f"#{i}") for i in ids]
                      for bid, ids in equip_of.items()}
    order_str = ((prefill.get("order") or "") if prefill else "") \
        or ",".join(str(b["id"]) for b in batches)
    return dict(batches=batches, equipment=equipment, equip_of=equip_of,
                equip_names_of=equip_names_of, order_str=order_str,
                prefill=prefill)


@app.route("/reschedule")
def reschedule_page():
    return render_template("reschedule.html",
                           **reschedule_form_context(get_db()))


@app.post("/reschedule/preview")
def reschedule_preview():
    """试算调整方案：只计算与展示，不写入任何数据。

    输入非法（批次/器具不存在、目标顺序缺失或重复、时段无效）时拒绝试算，
    重新展示表单并保留已填内容；通过则按目标顺序与调整内容试算完整排程，
    逐批对比调整前后的状态、风险或阻塞原因，并附上排程依据指纹供确认时
    检测方案是否过期。
    """
    conn = get_db()
    order_ids, adjustments, error = parse_plan_form(request.form)
    if error is None:
        error = validate_plan(conn, order_ids, adjustments)
    if error is not None:
        flash(f"试算被拒绝：{error}；未改动任何数据", "error")
        return render_template(
            "reschedule.html",
            **reschedule_form_context(conn, prefill=request.form)), 400
    # 先取指纹再试算：若试算后数据被并发修改，确认时指纹比对会判为过期，
    # 不会把基于旧数据的方案误当有效方案保存
    basis = schedule_basis_fingerprint(conn)
    trial = trial_schedule(conn, order_ids, adjustments)
    live = compute_schedule(conn)
    before_by_id = {r["id"]: r for r in live}
    live_pos = {r["id"]: i for i, r in enumerate(live, 1)}
    status_text = {"accepted": "已排入", "blocked": "已拒绝"}
    rows = []
    for i, t in enumerate(trial, 1):
        b = before_by_id[t["id"]]
        changes = []
        if i != live_pos.get(t["id"]):
            changes.append(f"顺序：第 {live_pos.get(t['id'])} 位 → 第 {i} 位")
        if (b["start"], b["end"]) != (t["start"], t["end"]):
            changes.append("时段已调整")
        if b["equipment_names"] != t["equipment_names"]:
            changes.append("器具已调整")
        if b["status"] != t["status"]:
            changes.append(f"状态：{status_text[b['status']]} → "
                           f"{status_text[t['status']]}")
        rows.append({"before": b, "after": t, "changes": changes,
                     "adjusted": t["id"] in adjustments})
    accepted = sum(1 for t in trial if t["status"] == "accepted")
    return render_template(
        "reschedule_preview.html", rows=rows,
        trial_accepted=accepted, trial_blocked=len(trial) - accepted,
        basis=basis, order_str=",".join(str(b) for b in order_ids),
        adjustments=adjustments)


@app.post("/reschedule/confirm")
def reschedule_confirm():
    """确认保存试算方案：过期拒绝，否则在同一事务内一次性保存全部调整。

    先比对排程依据指纹：试算后批次、器具、清洁、停用或配方任一变化，
    方案即过期，拒绝保存并提示重新试算；指纹一致则重新校验方案后在
    BEGIN IMMEDIATE 事务内更新批次时段/器具关联与全部批次顺序——
    任何失败都回滚，不会部分写入。若调整会改变进行中（已开工未完工）
    批次的排程依据计算结果，整次拒绝并指出批次。含阻塞批次的方案允许保存，
    交接版发布仍按原规则要求全部批次可排入。
    """
    conn = get_db()
    order_ids, adjustments, error = parse_plan_form(request.form)
    if error is not None:
        flash(f"未保存：{error}", "error")
        return redirect(url_for("reschedule_page"))
    basis = request.form.get("basis", "")
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError:
        flash("系统正忙，请稍后重试确认保存", "error")
        return redirect(url_for("reschedule_page"))
    try:
        before = inprogress_guard_snapshot(conn)
        if not basis or basis != schedule_basis_fingerprint(conn):
            conn.rollback()
            flash("未保存：试算后排程依据（批次、器具、清洁、停用或配方）"
                  "已发生变化，该方案已过期，请重新试算", "error")
            return redirect(url_for("reschedule_page"))
        error = validate_plan(conn, order_ids, adjustments)
        if error is not None:
            conn.rollback()
            flash(f"未保存：{error}", "error")
            return redirect(url_for("reschedule_page"))
        for bid, adj in adjustments.items():
            start = parse_dt(adj["start"]).isoformat(timespec="minutes")
            end = parse_dt(adj["end"]).isoformat(timespec="minutes")
            conn.execute(
                "UPDATE batch SET planned_start=?, planned_end=? WHERE id=?",
                (start, end, bid))
            conn.execute("DELETE FROM batch_equipment WHERE batch_id=?", (bid,))
            for eid in adj["equipment"]:
                conn.execute(
                    "INSERT INTO batch_equipment(batch_id, equipment_id)"
                    " VALUES (?,?)", (bid, eid))
        for i, bid in enumerate(order_ids, 1):
            conn.execute("UPDATE batch SET seq=? WHERE id=?", (i, bid))
        violated = inprogress_violations(conn, before)
        if violated:
            conn.rollback()
            flash(guard_reject_text(violated, prefix="未保存"), "error")
            return redirect(url_for("reschedule_page"))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    results = compute_schedule(conn)
    blocked = sum(1 for r in results if r["status"] != "accepted")
    msg = (f"已一次性保存全部调整：{len(adjustments)} 个批次更新时段/器具，"
           f"{len(order_ids)} 个批次按目标顺序重排，实时排程已更新——"
           f"已排入 {len(results) - blocked} 个、阻塞 {blocked} 个")
    if blocked:
        msg += "；含阻塞批次的方案已照常保存，发布交接版仍要求全部批次可排入"
    flash(msg, "ok")
    return redirect(url_for("index"))


# ---------------------------------------------------------- 清洁补救试算

def cleaning_blocked_targets(results):
    """当前因清洁缺失 / 失效 / 覆盖不足而受阻的批次（补救试算的可选目标）。

    返回 {batch_id: {"row": 排程结果, "risks": [清洁类风险]}}；
    只有 blocked 且至少一条风险为 blocked 级的批次才可选——
    占用冲突、器具停用等非清洁原因造成的阻塞不在候选内。
    """
    targets = {}
    for r in results:
        if r["status"] != "blocked":
            continue
        risks = [risk for risk in r["risks"] if risk["level"] == "blocked"]
        if risks:
            targets[r["id"]] = {"row": r, "risks": risks}
    return targets


def accepted_usage(conn, results):
    """按当前排程结果重建各器具上“已排入”批次的占用记录，供时窗扣除使用。

    被阻塞批次不占用器具，因此只取 accepted 批次；
    每项含起止、批次名与过敏原集合，结构与 compute_schedule 内部 usage 一致。
    """
    accepted = {r["id"] for r in results if r["status"] == "accepted"}
    usage = {}
    rows = conn.execute(
        "SELECT b.id AS bid, b.name AS name, b.allergens AS allergens,"
        " b.planned_start AS planned_start, b.planned_end AS planned_end,"
        " be.equipment_id AS equipment_id FROM batch b"
        " JOIN batch_equipment be ON be.batch_id = b.id").fetchall()
    for r in rows:
        if r["bid"] not in accepted:
            continue
        start, end = parse_dt(r["planned_start"]), parse_dt(r["planned_end"])
        if start is None or end is None:
            continue
        usage.setdefault(r["equipment_id"], []).append({
            "start": start, "end": end, "batch_name": r["name"],
            "allergens": split_allergens(r["allergens"])})
    return usage


def free_segments_between(horizon_start, horizon_end, downtimes, usages):
    """计算开区间 (horizon_start, horizon_end) 扣除停用与占用后的连续空闲段。

    端点相接不算相交（与排程规则一致）：阻挡区间只在与开区间严格相交时扣除。
    返回 (blockers, segments)：
    - blockers：相交的阻挡区间，按开始时间排序，每项含 kind/start/end/label；
    - segments：扣除后剩余的空闲时段 [(start, end) ...]，按开始时间升序；
      阻挡区间两两重叠时做防御性合并，保证空闲段计算正确。
    """
    blockers = []
    for d in downtimes or []:
        if d["start_dt"] < horizon_end and horizon_start < d["end_dt"]:
            blockers.append({
                "kind": "downtime", "start": d["start_dt"], "end": d["end_dt"],
                "label": downtime_block_text(
                    d["equipment_name"], d["start_dt"], d["end_dt"],
                    d.get("reason") or "—")})
    for u in usages or []:
        if u["start"] < horizon_end and horizon_start < u["end"]:
            blockers.append({
                "kind": "usage", "start": u["start"], "end": u["end"],
                "label": (f"已排入批次「{u['batch_name']}」占用"
                          f"（{fmt_dt_min(u['start'])} ~ "
                          f"{fmt_dt_min(u['end'])}），清洁不得与生产重叠")})
    blockers.sort(key=lambda b: (b["start"], b["end"]))
    # 合并相互重叠的阻挡区间
    merged = []
    for b in blockers:
        if merged and b["start"] <= merged[-1]["end"]:
            if b["end"] > merged[-1]["end"]:
                merged[-1]["end"] = b["end"]
                merged[-1]["label"] += "；" + b["label"]
        else:
            merged.append(dict(b))
    segments = [(horizon_start, horizon_end)]
    for b in merged:
        rest = []
        for seg_start, seg_end in segments:
            if b["end"] <= seg_start or b["start"] >= seg_end:
                rest.append((seg_start, seg_end))  # 不相交
                continue
            if seg_start < b["start"]:
                rest.append((seg_start, b["start"]))
            if b["end"] < seg_end:
                rest.append((b["end"], seg_end))
        segments = rest
    return merged, segments


def cleaning_remedy_plan(conn, live, target_id, duration):
    """为目标批次构造“补一条清洁”的补救试算方案（不写入任何数据）。

    逐台处理目标因清洁受阻的器具：取当前排程中该器具上“前一已排入批次结束 →
    目标开始”的区间，按现有停用与占用扣除后找能容纳 duration 分钟的最晚
    连续时窗；建议清洁覆盖该器具全部新增残留过敏原，清洁完成时间取时窗末端，
    有效期恰好覆盖目标开始。任一相关器具无足够时窗，或目标还因占用 / 停用等
    原因受阻时，用全部可行时窗生成虚拟清洁重算完整排程——目标仍阻塞则不可解。
    返回方案字典（error 非 None 时表示输入被拒绝）。
    """
    target = next((r for r in live if r["id"] == target_id), None)
    if target is None:
        return {"error": f"批次 #{target_id} 不存在或已删除，无法试算"}
    cleaning_risks = [risk for risk in target["risks"]
                      if risk["level"] == "blocked"]
    if target["status"] != "blocked" or not cleaning_risks:
        return {"error": f"批次「{target['name']}」当前并非因清洁缺失 / 失效 /"
                         f" 覆盖不足受阻，无需清洁补救试算"}

    start = target["start"]
    other_blocks = [reason for reason in target["blocks"]
                    if reason not in {risk["block_reason"]
                                      for risk in cleaning_risks}]
    downtimes = active_downtimes(conn)
    usage = accepted_usage(conn, live)

    plans, virtual_cleanings = [], []
    for risk in cleaning_risks:
        eq_id = risk["equipment_id"]
        horizon_start, horizon_end = risk["prev_end"], start
        blockers, segments = free_segments_between(
            horizon_start, horizon_end,
            downtimes.get(eq_id, []), usage.get(eq_id, []))
        # 最晚时窗：从最靠后的空闲段开始选，时窗末端贴齐该段末端
        window_start = window_end = None
        for seg_start, seg_end in reversed(segments):
            if seg_end - seg_start >= timedelta(minutes=duration):
                window_end = seg_end
                window_start = seg_end - timedelta(minutes=duration)
                break
        plan = {
            "equipment_id": eq_id,
            "equipment": risk["equipment"],
            "from_batch": risk["from_batch"],
            "new_risks": risk["new_risks"],
            "covers": ",".join(risk["new_risks"]),
            "block_reason": risk["block_reason"],
            "horizon_start": horizon_start, "horizon_end": horizon_end,
            "horizon_start_text": fmt_dt_min(horizon_start),
            "horizon_end_text": fmt_dt_min(horizon_end),
            "blockers": blockers,
            "segments": [{"start": s, "end": e,
                          "start_text": fmt_dt_min(s),
                          "end_text": fmt_dt_min(e),
                          "minutes": int((e - s).total_seconds() // 60)}
                         for s, e in segments],
            "feasible": window_start is not None,
            "window_start": window_start, "window_end": window_end,
            "window_start_text": fmt_dt_min(window_start),
            "window_end_text": fmt_dt_min(window_end),
        }
        if not plan["feasible"]:
            longest = max((int((e - s).total_seconds() // 60)
                           for s, e in segments), default=0)
            reason = (f"前一已排入批次结束（{plan['horizon_start_text']}）至"
                      f"目标开始（{plan['horizon_end_text']}）之间最长连续空闲"
                      f"仅 {longest} 分钟，不足清洁所需 {duration} 分钟")
            if blockers:
                reason += "（区间内存在：" + "；".join(
                    b["label"] for b in blockers) + "）"
            plan["reason"] = reason
        else:
            plan["reason"] = (
                f"可在 {plan['window_start_text']} ~ {plan['window_end_text']}"
                f" 连续执行 {duration} 分钟清洁（最晚可用时窗），"
                f"完成时间 {plan['window_end_text']} 作为清洁时间，"
                f"有效期覆盖至目标开始 {plan['horizon_end_text']}")
            virtual_cleanings.append({
                "equipment_id": eq_id,
                "cleaned_at": window_end.isoformat(timespec="minutes"),
                "valid_until": start.isoformat(timespec="minutes"),
                "covers": plan["covers"], "virtual": True, "id": None,
            })
        plans.append(plan)

    trial = compute_schedule(conn, virtual_cleanings=virtual_cleanings)
    target_after = next(r for r in trial if r["id"] == target_id)
    solvable = target_after["status"] == "accepted"
    infeasible = [p for p in plans if not p["feasible"]]
    if solvable:
        verdict = ("仅靠补登清洁即可解决：按下表最晚时窗为每台相关器具补一条"
                   "清洁后，目标批次可排入（仍需在「器具与清洁」正式登记后才会"
                   "写入实时排程；本试算不写入任何数据）")
    else:
        reasons = []
        if infeasible:
            reasons.append("相关器具无足够连续清洁时窗："
                           + "；".join(f"「{p['equipment']}」{p['reason']}"
                                       for p in infeasible))
        if other_blocks:
            reasons.append("目标批次还存在非清洁阻塞：" + "；".join(other_blocks))
        if target_after["status"] != "accepted" and target_after["blocks"]:
            new_reasons = [b for b in target_after["blocks"]
                           if b not in target["blocks"]]
            if new_reasons:
                reasons.append("加入虚拟清洁重算后目标仍受阻："
                               + "；".join(new_reasons))
            elif not reasons:
                reasons.append("加入虚拟清洁重算后目标仍受阻："
                               + "；".join(target_after["blocks"]))
        verdict = "不可仅靠清洁解决——" + "；".join(reasons)
    return {"error": None, "target": target, "target_after": target_after,
            "duration": duration, "plans": plans,
            "other_blocks": other_blocks,
            "virtual_cleanings": virtual_cleanings,
            "trial": trial, "live": live, "solvable": solvable,
            "verdict": verdict}


def remedy_form_context(conn, prefill=None):
    """清洁补救试算表单所需数据：当前排程与清洁类受阻候选批次。"""
    live = compute_schedule(conn)
    targets = cleaning_blocked_targets(live)
    selected = (prefill or {}).get("target_id", "")
    duration = (prefill or {}).get("duration", "")
    return {"live": live, "targets": targets,
            "selected": selected, "duration_value": duration}


@app.route("/cleaning-remedy")
def cleaning_remedy_page():
    return render_template("cleaning_remedy.html",
                           **remedy_form_context(get_db()))


@app.post("/cleaning-remedy/preview")
def cleaning_remedy_preview():
    """清洁补救试算：选择清洁类受阻批次并输入正整数分钟数，查看补清洁的效果。

    无效批次（不存在 / 当前并非清洁受阻）与非正整数时长一律拒绝；
    试算只用虚拟清洁在内存中重算完整排程，不写入清洁记录、实时排程或交接版。
    """
    conn = get_db()
    live = compute_schedule(conn)
    targets = cleaning_blocked_targets(live)
    raw_target = (request.form.get("target_id", "") or "").strip()
    raw_duration = (request.form.get("duration", "") or "").strip()
    try:
        target_id = int(raw_target)
    except ValueError:
        target_id = None
    try:
        duration = int(raw_duration)
    except ValueError:
        duration = None
    if target_id is None:
        flash("试算被拒绝：请选择一个有效的批次", "error")
        return render_template(
            "cleaning_remedy.html",
            **remedy_form_context(conn, prefill=request.form)), 400
    if duration is None:
        flash("试算被拒绝：清洁时长必须为正整数分钟数（如 15、30），"
              "未改动任何数据", "error")
        return render_template(
            "cleaning_remedy.html",
            **remedy_form_context(conn, prefill=request.form)), 400
    if duration <= 0:
        flash("试算被拒绝：清洁时长必须为正整数（大于 0 的分钟数），"
              "未改动任何数据", "error")
        return render_template(
            "cleaning_remedy.html",
            **remedy_form_context(conn, prefill=request.form)), 400
    if target_id not in targets:
        flash(f"试算被拒绝：批次 #{target_id} 不存在，或当前并非因清洁缺失 /"
              f" 失效 / 覆盖不足受阻；仅可选择列表中因清洁问题受阻的批次，"
              f"未改动任何数据", "error")
        return render_template(
            "cleaning_remedy.html",
            **remedy_form_context(conn, prefill=request.form)), 400
    plan = cleaning_remedy_plan(conn, live, target_id, duration)
    if plan.get("error"):
        flash(f"试算被拒绝：{plan['error']}；未改动任何数据", "error")
        return render_template(
            "cleaning_remedy.html",
            **remedy_form_context(conn, prefill=request.form)), 400
    live_by_id = {r["id"]: r for r in live}
    rows = []
    for i, t in enumerate(plan["trial"], 1):
        b = live_by_id[t["id"]]
        changes = []
        if b["status"] != t["status"]:
            changes.append(f"状态：{'已排入' if b['status'] == 'accepted' else '已拒绝'}"
                           f" → {'已排入' if t['status'] == 'accepted' else '已拒绝'}")
        if b["blocks"] != t["blocks"]:
            changes.append("阻塞原因 / 风险处置发生变化")
        rows.append({"before": b, "after": t, "changes": changes,
                     "is_target": t["id"] == target_id,
                     "position": i})
    accepted = sum(1 for r in plan["trial"] if r["status"] == "accepted")
    return render_template(
        "cleaning_remedy_preview.html", plan=plan, rows=rows,
        trial_accepted=accepted, trial_blocked=len(plan["trial"]) - accepted,
        target_id=target_id, duration=duration)


# ---------------------------------------------------------------- 导出

def export_rows(results):
    """把排程结果展开为可导出的行。"""
    rows = []
    for i, r in enumerate(results, 1):
        note = revision_note_text(r.get("latest_revision"))
        base = {
            "order": i,
            "batch": r["name"],
            "allergens": r["allergens"],
            "equipment": r["equipment_names"],
            "planned_start": r["start"].isoformat(timespec="minutes") if r["start"] else "",
            "planned_end": r["end"].isoformat(timespec="minutes") if r["end"] else "",
            "status": "已排入" if r["status"] == "accepted" else "已拒绝",
        }

        def with_note(text):
            return f"{text}；{note}" if note else text

        if r["risks"]:
            for risk in r["risks"]:
                row = dict(base)
                row["risk_source"] = risk_source_text(risk)
                row["action"] = with_note(risk_action_text(risk))
                rows.append(row)
        else:
            row = dict(base)
            row["risk_source"] = "无前置残留" if r["status"] == "accepted" else "—"
            row["action"] = with_note(
                "；".join(r["blocks"]) if r["blocks"] else "无交叉污染风险")
            rows.append(row)
        # 阻塞批次若同时带有风险评估结果（某些器具上的残留、另一些器具上的
        # 时段冲突/停用检修），把未写进风险行的阻塞原因（如器具停用）补成独立行，
        # 保证“已拒绝”原因（含器具与冲突时段）在导出中完整可见
        if r["status"] == "blocked" and r["risks"] and r["blocks"]:
            covered = {risk["block_reason"] for risk in r["risks"]
                       if risk["level"] == "blocked"}
            for reason in r["blocks"]:
                if reason in covered:
                    continue
                row = dict(base)
                row["risk_source"] = "—"
                row["action"] = with_note(f"⛔ {reason}")
                rows.append(row)
    return rows


@app.route("/export.csv")
def export_csv():
    results = compute_schedule(get_db())
    out = io.StringIO()
    out.write("﻿")  # BOM，便于 Excel 识别中文
    w = csv.writer(out)
    w.writerow(["顺序", "批次", "本批次过敏原", "使用器具", "计划开始", "计划结束",
                "状态", "风险来源", "处置 / 阻塞原因"])
    for row in export_rows(results):
        w.writerow([row["order"], row["batch"], join_alg(row["allergens"]),
                    "、".join(row["equipment"]), row["planned_start"],
                    row["planned_end"], row["status"],
                    row["risk_source"], row["action"]])
    return Response(out.getvalue(), mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition":
                             "attachment; filename=schedule_risk.csv"})


@app.route("/export.json")
def export_json():
    conn = get_db()
    results = compute_schedule(conn)
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "app_url": APP_URL,
        "schedule": export_rows(results),
        "equipment_downtimes": downtime_export_rows(conn),
        "cleaning_deletions": cleaning_deletion_export_rows(conn),
    }
    return Response(json.dumps(payload, ensure_ascii=False, indent=2),
                    mimetype="application/json; charset=utf-8",
                    headers={"Content-Disposition":
                             "attachment; filename=schedule_risk.json"})


# ---------------------------------------------------- 器具停用检修导出

def downtime_export_rows(conn):
    """把停用记录展开为可导出的结构（反映最新停用/取消状态）。"""
    rows = []
    for d in downtime_rows(conn):
        rows.append({
            "downtime_id": d["id"],
            "equipment_id": d["equipment_id"],
            "equipment": d["equipment_name"],
            "start_at": d["start_at"],
            "end_at": d["end_at"],
            "reason": d["reason"],
            "created_at": d["created_at"],
            "cancelled": bool(d["cancelled_at"]),
            "cancelled_at": d["cancelled_at"] or "",
            "status": "已取消" if d["cancelled_at"] else "有效",
        })
    return rows


@app.route("/export/downtimes.csv")
def export_downtimes_csv():
    out = io.StringIO()
    out.write("﻿")  # BOM，便于 Excel 识别中文
    w = csv.writer(out)
    w.writerow(["停用序号", "器具", "停用开始", "停用结束", "停用原因",
                "登记时间", "状态", "取消时间"])
    for d in downtime_export_rows(get_db()):
        w.writerow([d["downtime_id"], d["equipment"],
                    d["start_at"].replace("T", " "),
                    d["end_at"].replace("T", " "), d["reason"],
                    (d["created_at"] or "").replace("T", " "),
                    d["status"],
                    (d["cancelled_at"] or "").replace("T", " ")])
    return Response(out.getvalue(), mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition":
                             "attachment; filename=equipment_downtimes.csv"})


@app.route("/export/downtimes.json")
def export_downtimes_json():
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "app_url": APP_URL,
        "equipment_downtimes": downtime_export_rows(get_db()),
    }
    return Response(json.dumps(payload, ensure_ascii=False, indent=2),
                    mimetype="application/json; charset=utf-8",
                    headers={"Content-Disposition":
                             "attachment; filename=equipment_downtimes.json"})


# ---------------------------------------------------- 清洁删除审计导出

def cleaning_deletion_export_rows(conn):
    """把清洁删除审计记录展开为可导出的结构（含受影响批次快照）。"""
    rows = []
    for d in cleaning_deletion_rows(conn):
        rows.append({
            "deletion_id": d["id"],
            "cleaning_id": d["cleaning_id"],
            "equipment": d["equipment_name"],
            "cleaned_at": d["cleaned_at"],
            "valid_until": d["valid_until"],
            "covers": cleaning_cover_text(d["covers"]),
            "deleted_at": d["deleted_at"],
            "reason": d["reason"],
            "affected_count": len(d["affected_list"]),
            "affected_batches": [
                {"batch_id": a.get("batch_id"),
                 "batch": a.get("batch_name"),
                 "equipment": "、".join(a.get("equipment_names") or []),
                 "prev_status": a.get("prev_status"),
                 "new_status": a.get("new_status"),
                 "block_reasons": a.get("block_reasons", [])}
                for a in d["affected_list"]],
        })
    return rows


@app.route("/export/cleaning-deletions.csv")
def export_cleaning_deletions_csv():
    out = io.StringIO()
    out.write("﻿")  # BOM，便于 Excel 识别中文
    w = csv.writer(out)
    w.writerow(["删除序号", "清洁记录ID", "器具", "清洁时间", "有效至", "覆盖范围",
                "删除时间", "删除原因", "受影响批次数",
                "受影响批次（状态变化）", "删除后阻塞原因"])
    for d in cleaning_deletion_export_rows(get_db()):
        if d["affected_batches"]:
            names = "、".join(
                f"{a['batch']}（{a['prev_status']}→{a['new_status']}）"
                for a in d["affected_batches"])
            reasons = " | ".join(
                f"{a['batch']}：{'；'.join(a['block_reasons'])}"
                for a in d["affected_batches"])
        else:
            names, reasons = "无", "无"
        w.writerow([d["deletion_id"], d["cleaning_id"], d["equipment"],
                    d["cleaned_at"].replace("T", " "),
                    d["valid_until"].replace("T", " "), d["covers"],
                    d["deleted_at"].replace("T", " "), d["reason"],
                    d["affected_count"], names, reasons])
    return Response(out.getvalue(), mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition":
                             "attachment; filename=cleaning_deletions.csv"})


@app.route("/export/cleaning-deletions.json")
def export_cleaning_deletions_json():
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "app_url": APP_URL,
        "cleaning_deletions": cleaning_deletion_export_rows(get_db()),
    }
    return Response(json.dumps(payload, ensure_ascii=False, indent=2),
                    mimetype="application/json; charset=utf-8",
                    headers={"Content-Disposition":
                             "attachment; filename=cleaning_deletions.json"})


# ---------------------------------------------------------------- 生产交接版

def risk_snapshot(risk):
    """把排程结果中的一条风险记录转为可 JSON 保存的快照（含所用清洁）。"""
    snap = {
        "equipment": risk["equipment"],
        "from_batch": risk["from_batch"],
        "residual": list(risk["residual"]),
        "new_risks": list(risk["new_risks"]),
        "level": risk["level"],
        "block_reason": risk["block_reason"],
        "cleaning": None,
    }
    c = risk.get("cleaning")
    if c:
        snap["cleaning"] = {
            "id": c["id"],
            "cleaned_at": c["cleaned_at"],
            "valid_until": c["valid_until"],
            "covers": c["covers"],
        }
    return snap


def latest_release(conn):
    return conn.execute(
        "SELECT * FROM release_version ORDER BY version DESC LIMIT 1").fetchone()


def release_items(conn, release_id):
    """读取某交接版的批次快照（按发布时顺序），risks 解析为列表。"""
    items = []
    for r in conn.execute(
            "SELECT * FROM release_item WHERE release_id=? ORDER BY position",
            (release_id,)):
        d = dict(r)
        try:
            d["risks"] = json.loads(d.get("risks") or "[]")
        except (TypeError, ValueError):
            d["risks"] = []
        items.append(d)
    return items


def risk_fingerprint(risks):
    """风险来源 / 所用清洁内容的规范化指纹，用于对比交接版与实时排程。"""
    parts = []
    for risk in risks or []:
        c = risk.get("cleaning") or {}
        parts.append("|".join([
            risk.get("equipment", ""),
            risk.get("from_batch", ""),
            ",".join(risk.get("residual") or []),
            ",".join(risk.get("new_risks") or []),
            risk.get("level") or "",
            str(c.get("id") or ""),
            c.get("cleaned_at") or "",
            c.get("valid_until") or "",
            c.get("covers") or "",
        ]))
    return parts


def diff_against_release(results, items):
    """把实时排程结果与交接版快照按批次对比。

    返回 (diff, removed_ids)：
    diff[batch_id] = {"state": "added"|"changed"|"same", "changes": [说明...]}；
    removed_ids 为交接版中已不在当前排程里的批次 id（已移除）。
    """
    by_bid = {it["batch_id"]: it for it in items}
    cur_ids = {r["id"] for r in results}
    status_text = {"accepted": "已排入", "blocked": "已拒绝"}
    diff = {}
    for i, r in enumerate(results, 1):
        it = by_bid.get(r["id"])
        if it is None:
            diff[r["id"]] = {"state": "added",
                             "changes": ["交接版发布后新增的批次"]}
            continue
        changes = []
        if r["name"] != it["batch_name"]:
            changes.append(f"名称：{it['batch_name']} → {r['name']}")
        cur_alg = "、".join(r["allergens"])
        if cur_alg != (it["allergens"] or ""):
            changes.append(f"过敏原：{it['allergens'] or '无'} → {cur_alg or '无'}")
        cur_eq = "、".join(r["equipment_names"])
        if cur_eq != (it["equipment_names"] or ""):
            changes.append(f"器具：{it['equipment_names'] or '—'} → {cur_eq or '—'}")
        cur_start = r["start"].isoformat(timespec="minutes") if r["start"] else ""
        cur_end = r["end"].isoformat(timespec="minutes") if r["end"] else ""
        if cur_start != it["planned_start"] or cur_end != it["planned_end"]:
            fmt = lambda s: (s or "").replace("T", " ") or "—"  # noqa: E731
            changes.append(f"时段：{fmt(it['planned_start'])} ~ {fmt(it['planned_end'])}"
                           f" → {fmt(cur_start)} ~ {fmt(cur_end)}")
        if r["status"] != it["status"]:
            changes.append(f"状态：{status_text.get(it['status'], it['status'])}"
                           f" → {status_text.get(r['status'], r['status'])}")
        if i != it["position"]:
            changes.append(f"顺序：第 {it['position']} 位 → 第 {i} 位")
        if risk_fingerprint(r["risks"]) != risk_fingerprint(it["risks"]):
            changes.append("风险来源 / 所用清洁发生变化")
        diff[r["id"]] = {"state": "changed" if changes else "same",
                         "changes": changes}
    removed_ids = {it["batch_id"] for it in items if it["batch_id"] not in cur_ids}
    return diff, removed_ids


@app.post("/releases/publish")
def publish_release():
    """把当前排程发布为可追溯的生产交接版。

    在 BEGIN IMMEDIATE 事务内按现有器具占用、过敏原与清洁规则重新计算，
    并在同一事务内写入连续版本号与按顺序排列的批次快照：计算所依据的
    批次/器具/清洁数据与落库内容处于同一事务，并发修改只能排在事务之前
    或之后，不会造成版本内容与判定依据不一致；版本号在写锁保护下取
    MAX(version)+1，唯一约束兜底，保证连续不重复。
    没有批次或存在阻塞批次时回滚，不生成版本。
    """
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError:
        flash("系统正忙，请稍后重试发布交接版", "error")
        return redirect(url_for("index"))
    try:
        results = compute_schedule(conn)
        if not results:
            conn.rollback()
            flash("未发布交接版：当前没有任何批次，无可交接的排程内容", "error")
            return redirect(url_for("index"))
        blocked = [r for r in results if r["status"] != "accepted"]
        if blocked:
            conn.rollback()
            parts = []
            for r in blocked:
                reason = "；".join(r["blocks"]) if r["blocks"] else "未能排入"
                parts.append(f"「{r['name']}」（{reason}）")
            flash(f"未发布交接版：{len(blocked)} 个批次处于阻塞状态——"
                  + "、".join(parts)
                  + "。请先调整顺序、补登清洁、修订批次，或取消/调整器具停用时段后再发布。",
                  "error")
            return redirect(url_for("index"))
        version = conn.execute(
            "SELECT COALESCE(MAX(version),0)+1 AS v FROM release_version"
        ).fetchone()["v"]
        created_at = datetime.now().isoformat(timespec="seconds")
        cur = conn.execute(
            "INSERT INTO release_version(version, created_at, batch_count)"
            " VALUES (?,?,?)", (version, created_at, len(results)))
        release_id = cur.lastrowid
        for i, r in enumerate(results, 1):
            conn.execute(
                "INSERT INTO release_item(release_id, position, batch_id,"
                " batch_name, allergens, equipment_names, planned_start,"
                " planned_end, status, risks, revision_note)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (release_id, i, r["id"], r["name"],
                 "、".join(r["allergens"]),
                 "、".join(r["equipment_names"]),
                 r["start"].isoformat(timespec="minutes") if r["start"] else "",
                 r["end"].isoformat(timespec="minutes") if r["end"] else "",
                 r["status"],
                 json.dumps([risk_snapshot(risk) for risk in r["risks"]],
                            ensure_ascii=False),
                 revision_note_text(r.get("latest_revision"))))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    flash(f"交接版 v{version} 已发布：{len(results)} 个批次（发布时全部已排入）"
          f"已按当前规则快照存档，历史版本不随后续修改变化", "ok")
    return redirect(url_for("release_detail", version=version))


@app.route("/releases")
def releases():
    rows = get_db().execute(
        "SELECT * FROM release_version ORDER BY version DESC").fetchall()
    return render_template("releases.html", releases=rows)


@app.route("/releases/<int:version>")
def release_detail(version):
    conn = get_db()
    rel = conn.execute(
        "SELECT * FROM release_version WHERE version=?", (version,)).fetchone()
    if rel is None:
        flash(f"交接版 v{version} 不存在", "error")
        return redirect(url_for("releases"))
    executions = {e["batch_id"]: dict(e) for e in conn.execute(
        "SELECT * FROM execution WHERE release_id=?", (rel["id"],))}
    return render_template("release_detail.html", rel=rel,
                           items=release_items(conn, rel["id"]),
                           executions=executions)


def release_export_rows(items):
    """把交接版快照展开为可导出的行（列结构与实时导出一致）。"""
    rows = []
    for it in items:
        base = {
            "order": it["position"],
            "batch": it["batch_name"],
            "allergens": it["allergens"],
            "equipment": it["equipment_names"],
            "planned_start": it["planned_start"],
            "planned_end": it["planned_end"],
            "status": "已排入" if it["status"] == "accepted" else "已拒绝",
        }
        note = it.get("revision_note") or ""
        if it["risks"]:
            for risk in it["risks"]:
                row = dict(base)
                row["risk_source"] = risk_source_text(risk)
                row["action"] = risk_action_text(risk)
                if note:
                    row["action"] = f"{row['action']}；{note}"
                rows.append(row)
        else:
            base["risk_source"] = "无前置残留" if it["status"] == "accepted" else "—"
            base["action"] = note or "无交叉污染风险"
            rows.append(base)
    return rows


def get_release_or_redirect(conn, version):
    return conn.execute(
        "SELECT * FROM release_version WHERE version=?", (version,)).fetchone()


@app.route("/releases/<int:version>.csv")
def release_export_csv(version):
    conn = get_db()
    rel = get_release_or_redirect(conn, version)
    if rel is None:
        flash(f"交接版 v{version} 不存在", "error")
        return redirect(url_for("releases"))
    out = io.StringIO()
    out.write("﻿")  # BOM，便于 Excel 识别中文
    w = csv.writer(out)
    w.writerow(["顺序", "批次", "本批次过敏原", "使用器具", "计划开始", "计划结束",
                "状态", "风险来源", "处置 / 阻塞原因"])
    for row in release_export_rows(release_items(conn, rel["id"])):
        w.writerow([row["order"], row["batch"], row["allergens"],
                    row["equipment"], row["planned_start"],
                    row["planned_end"], row["status"],
                    row["risk_source"], row["action"]])
    return Response(out.getvalue(), mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition":
                             f"attachment; filename=handover_v{version}.csv"})


@app.route("/releases/<int:version>.json")
def release_export_json(version):
    conn = get_db()
    rel = get_release_or_redirect(conn, version)
    if rel is None:
        flash(f"交接版 v{version} 不存在", "error")
        return redirect(url_for("releases"))
    items = release_items(conn, rel["id"])
    payload = {
        "version": rel["version"],
        "created_at": rel["created_at"],
        "batch_count": rel["batch_count"],
        "app_url": APP_URL,
        "schedule": release_export_rows(items),
        "snapshot": [{
            "position": it["position"],
            "batch_id": it["batch_id"],
            "batch": it["batch_name"],
            "allergens": [a for a in it["allergens"].split("、") if a],
            "equipment": [e for e in it["equipment_names"].split("、") if e],
            "planned_start": it["planned_start"],
            "planned_end": it["planned_end"],
            "status": it["status"],
            "risks": it["risks"],
            "revision_note": it["revision_note"],
        } for it in items],
    }
    return Response(json.dumps(payload, ensure_ascii=False, indent=2),
                    mimetype="application/json; charset=utf-8",
                    headers={"Content-Disposition":
                             f"attachment; filename=handover_v{version}.json"})


# ---------------------------------------------------------------- 生产执行

def execution_by_batch(conn):
    """全部执行记录按批次索引：{batch_id: 记录}（每批次至多一条，唯一约束保证）。"""
    return {r["batch_id"]: dict(r)
            for r in conn.execute("SELECT * FROM execution")}


def live_basis_map(conn):
    """实时排程中每个批次当前的开工依据：顺序、排程状态、计划时段、器具、
    过敏原、风险来源与所用清洁（与开工校验、交接版快照用同一套字段）。"""
    basis = {}
    for i, r in enumerate(compute_schedule(conn), 1):
        basis[r["id"]] = (
            i, r["status"],
            r["start"].isoformat(timespec="minutes") if r["start"] else "",
            r["end"].isoformat(timespec="minutes") if r["end"] else "",
            "、".join(r["equipment_names"]),
            "、".join(r["allergens"]),
            tuple(risk_fingerprint(r["risks"])),
        )
    return basis


def inprogress_guard_snapshot(conn):
    """进行中（已开工未完工）批次当前的排程依据：{batch_id: basis | None}。

    写操作在事务内先取本快照、写入后再取一次对比：任何使进行中批次计算
    结果发生变化的批次 / 器具 / 清洁 / 停用 / 批量调整操作都会被整次拒绝；
    None 表示批次已不在实时排程中（如被删除），同样视为依据被改变。
    """
    live = live_basis_map(conn)
    return {ex["batch_id"]: live.get(ex["batch_id"])
            for ex in conn.execute(
                "SELECT batch_id FROM execution WHERE actual_end IS NULL")}


def inprogress_violations(conn, before):
    """对比操作前快照与当前状态，返回排程依据被改变的进行中批次 [(id, 名称)]。"""
    if not before:
        return []
    after = inprogress_guard_snapshot(conn)
    names = {r["batch_id"]: r["batch_name"] for r in conn.execute(
        "SELECT batch_id, batch_name FROM execution")}
    return [(bid, names.get(bid, f"#{bid}"))
            for bid, old in before.items() if after.get(bid) != old]


def guard_reject_text(violated, prefix="未保存"):
    """进行中批次排程依据被改变时的整次拒绝说明（指出具体批次）。"""
    names = "、".join(f"「{name}」#{bid}" for bid, name in violated)
    return (f"{prefix}：操作会改变进行中批次 {names} 的排程依据"
            f"（顺序 / 计划时段 / 器具 / 过敏原 / 风险来源与所用清洁），"
            f"进行中批次的排程依据必须保持不变，已整次拒绝，未改动任何数据")


def start_drift_changes(result, position, item):
    """把某批次的实时排程结果与交接版快照逐项对比（开工校验与页面提示）。

    result 为 None 表示批次已删除。返回差异说明列表，空列表表示一致、可开工。
    """
    if result is None:
        return ["批次已删除，不在当前批次列表中"]
    changes = []
    if result["status"] != "accepted":
        reason = "；".join(result["blocks"]) if result["blocks"] else "未能排入"
        changes.append(f"实时排程状态：已排入 → 已拒绝（{reason}）")
    if position != item["position"]:
        changes.append(f"顺序：交接版第 {item['position']} 位 → 当前第 {position} 位")
    cur_start = result["start"].isoformat(timespec="minutes") \
        if result["start"] else ""
    cur_end = result["end"].isoformat(timespec="minutes") if result["end"] else ""
    if cur_start != (item["planned_start"] or "") \
            or cur_end != (item["planned_end"] or ""):
        fmt = lambda s: (s or "").replace("T", " ") or "—"  # noqa: E731
        changes.append(f"计划时段：{fmt(item['planned_start'])} ~ "
                       f"{fmt(item['planned_end'])} → {fmt(cur_start)} ~ {fmt(cur_end)}")
    cur_eq = "、".join(result["equipment_names"])
    if cur_eq != (item["equipment_names"] or ""):
        changes.append(f"器具：{item['equipment_names'] or '—'} → {cur_eq or '—'}")
    cur_alg = "、".join(result["allergens"])
    if cur_alg != (item["allergens"] or ""):
        changes.append(f"过敏原：{item['allergens'] or '无'} → {cur_alg or '无'}")
    if risk_fingerprint(result["risks"]) != risk_fingerprint(item["risks"]):
        changes.append("风险来源 / 所用清洁与交接版快照不一致")
    return changes


@app.route("/execution")
def execution_page():
    """生产执行页：按最新交接版逐批开工 / 完工，并列出全部执行记录。"""
    conn = get_db()
    latest = latest_release(conn)
    items = release_items(conn, latest["id"]) if latest else []
    executions = execution_by_batch(conn)
    live = compute_schedule(conn)
    pos = {r["id"]: i for i, r in enumerate(live, 1)}
    by_id = {r["id"]: r for r in live}
    rows = []
    for it in items:
        ex = executions.get(it["batch_id"])
        changes = start_drift_changes(by_id.get(it["batch_id"]),
                                      pos.get(it["batch_id"]), it)
        rows.append({"item": it, "execution": ex, "changes": changes,
                     "startable": ex is None and not changes})
    history = [dict(r) for r in conn.execute(
        "SELECT * FROM execution ORDER BY id DESC")]
    current = {r["id"]: r for r in conn.execute("SELECT * FROM batch")}
    for e in history:
        cur = current.get(e["batch_id"])
        e["batch_gone"] = cur is None
        e["rescheduled"] = bool(cur) and (
            parse_dt(cur["planned_start"]) != parse_dt(e["planned_start"])
            or parse_dt(cur["planned_end"]) != parse_dt(e["planned_end"]))
    return render_template("execution.html", latest=latest, rows=rows,
                           history=history)


@app.post("/execution/start/<int:bid>")
def start_execution(bid):
    """登记开工：仅当批次仍在最新交接版中、批次仍存在、实时排程已排入，
    且顺序 / 计划时段 / 器具 / 过敏原 / 风险来源与所用清洁均与该版快照一致时
    才记录实际开工时间；否则说明差异且不记开工。同一批次不得重复开工。

    判定与写入在同一 BEGIN IMMEDIATE 事务内完成（写锁保护），并发开工或并发
    修改排程依据只能排在事务之前或之后；execution.batch_id 的唯一约束兜底，
    避免并发穿透造成重复开工或依据不一致。
    """
    conn = get_db()
    actual_start = (request.form.get("actual_start", "") or "").strip()
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError:
        flash("系统正忙，请稍后重试登记开工", "error")
        return redirect(url_for("execution_page"))
    try:
        latest = latest_release(conn)
        if latest is None:
            conn.rollback()
            flash("未记开工：尚未发布交接版，请先在排程看板发布交接版", "error")
            return redirect(url_for("execution_page"))
        item = conn.execute(
            "SELECT * FROM release_item WHERE release_id=? AND batch_id=?",
            (latest["id"], bid)).fetchone()
        if item is None:
            conn.rollback()
            flash(f"未记开工：批次 #{bid} 不在最新交接版 v{latest['version']} 中"
                  f"（交接版发布后新增的批次不能按该版开工）", "error")
            return redirect(url_for("execution_page"))
        item = dict(item)
        try:
            item["risks"] = json.loads(item.get("risks") or "[]")
        except (TypeError, ValueError):
            item["risks"] = []
        start_dt = parse_dt(actual_start)
        if start_dt is None:
            conn.rollback()
            flash("未记开工：请填写有效的实际开工时间", "error")
            return redirect(url_for("execution_page"))
        existing = conn.execute(
            "SELECT * FROM execution WHERE batch_id=?", (bid,)).fetchone()
        if existing is not None:
            conn.rollback()
            flash(f"未记开工：批次「{existing['batch_name']}」已登记开工"
                  f"（交接版 v{existing['version']}，实际开工 "
                  f"{fmt_dt_min(existing['actual_start'])}），"
                  f"同一批次不得重复开工", "error")
            return redirect(url_for("execution_page"))
        live = compute_schedule(conn)
        pos = {r["id"]: i for i, r in enumerate(live, 1)}
        by_id = {r["id"]: r for r in live}
        changes = start_drift_changes(by_id.get(bid), pos.get(bid), item)
        if changes:
            conn.rollback()
            flash(f"未记开工：批次「{item['batch_name']}」与交接版 "
                  f"v{latest['version']} 快照不一致——" + "；".join(changes)
                  + "。请先使实时排程与交接版一致（或重新发布交接版）后再开工",
                  "error")
            return redirect(url_for("execution_page"))
        conn.execute(
            "INSERT INTO execution(release_id, version, batch_id, batch_name,"
            " position, allergens, equipment_names, planned_start, planned_end,"
            " actual_start, started_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (latest["id"], latest["version"], bid, item["batch_name"],
             item["position"], item["allergens"], item["equipment_names"],
             item["planned_start"], item["planned_end"],
             start_dt.isoformat(timespec="minutes"),
             datetime.now().isoformat(timespec="minutes")))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.rollback()
        flash("未记开工：该批次已存在执行记录，同一批次不得重复开工", "error")
        return redirect(url_for("execution_page"))
    except Exception:
        conn.rollback()
        raise
    flash(f"批次「{item['batch_name']}」已按交接版 v{latest['version']} 登记开工"
          f"（实际开工 {fmt_dt_min(actual_start)}），执行记录已保存", "ok")
    return redirect(url_for("execution_page"))


@app.post("/execution/<int:exid>/finish")
def finish_execution(exid):
    """登记完工：仅进行中（已开工未完工）批次可登记；完工时间早于开工时间
    或重复完工均拒绝。判定与更新在同一事务内完成，避免并发穿透。"""
    conn = get_db()
    actual_end = (request.form.get("actual_end", "") or "").strip()
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError:
        flash("系统正忙，请稍后重试登记完工", "error")
        return redirect(url_for("execution_page"))
    try:
        ex = conn.execute("SELECT * FROM execution WHERE id=?",
                          (exid,)).fetchone()
        if ex is None:
            conn.rollback()
            flash("未记完工：执行记录不存在", "error")
            return redirect(url_for("execution_page"))
        if ex["actual_end"]:
            conn.rollback()
            flash(f"未记完工：批次「{ex['batch_name']}」已于 "
                  f"{fmt_dt_min(ex['actual_end'])} 登记完工，不得重复完工", "error")
            return redirect(url_for("execution_page"))
        end_dt = parse_dt(actual_end)
        if end_dt is None:
            conn.rollback()
            flash("未记完工：请填写有效的实际完工时间", "error")
            return redirect(url_for("execution_page"))
        start_dt = parse_dt(ex["actual_start"])
        if start_dt is not None and end_dt < start_dt:
            conn.rollback()
            flash(f"未记完工：完工时间 {fmt_dt_min(actual_end)} 早于开工时间 "
                  f"{fmt_dt_min(ex['actual_start'])}，请核对后再填写", "error")
            return redirect(url_for("execution_page"))
        conn.execute(
            "UPDATE execution SET actual_end=?, finished_at=? WHERE id=?",
            (end_dt.isoformat(timespec="minutes"),
             datetime.now().isoformat(timespec="minutes"), exid))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    flash(f"批次「{ex['batch_name']}」已登记完工"
          f"（{fmt_dt_min(ex['actual_start'])} ~ {fmt_dt_min(actual_end)}），"
          f"执行记录已保存", "ok")
    return redirect(url_for("execution_page"))


# ---------------------------------------------------------------- 启动

init_db()

if __name__ == "__main__":
    print(f" * 共享厨房过敏原排程系统")
    print(f" * 访问地址: {APP_URL}  (监听端口 {PORT})")
    app.run(host="0.0.0.0", port=PORT)
