#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dota 对局查询 - 极简本地代理

为什么需要它(而不是纯前端直接 fetch Valve):
  1) CORS: api.steampowered.com 不返回跨域头,浏览器里直接 fetch 会被拦。
  2) key 安全: 代理让 key 留在本机后端,不写进网页,避免泄露。
前端仍是纯 HTML(index.html),后端只是个不到 200 行、零依赖的转发器,方便你迭代。

运行:
  python server.py
然后浏览器打开 http://localhost:8000
必须通过环境变量提供 key:  set DOTA_API_KEY=xxxx  (Windows) / export DOTA_API_KEY=xxxx (Linux)
"""
import os
import json
import time
import sqlite3
import urllib.request
import urllib.parse
import urllib.error
import http.server

KEY = os.environ.get("DOTA_API_KEY")
if not KEY:
    raise SystemExit("请设置环境变量 DOTA_API_KEY(Steam Web API key),不再使用硬编码默认值。")
PORT = int(os.environ.get("PORT", "8000"))
HOST = os.environ.get("HOST", "0.0.0.0")   # 容器里需绑 0.0.0.0 才能被外部访问
BASE = "https://api.steampowered.com"
STEAM64_BASE = 76561197960265728   # account_id(32位) + 这个 = steamid64
ANON = 4294967295                  # 0xFFFFFFFF = 匿名玩家的 account_id
HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("DOTA_DB_PATH", os.path.join(HERE, "dota.db"))

MIN_INTERVAL = float(os.environ.get("DOTA_MIN_INTERVAL", "1.1"))  # 每次 Valve 调用最小间隔(秒)
_last_call = [0.0]

_hero_cache = None


OPENDOTA = "https://api.opendota.com/api"
OPENDOTA_KEY = os.environ.get("OPENDOTA_API_KEY")   # 可选,填了配额更高


def _get_json(url, _retry=0):
    """统一的限速 + 429 退避 HTTP GET(Valve 和 OpenDota 共用)。"""
    wait = MIN_INTERVAL - (time.time() - _last_call[0])
    if wait > 0:
        time.sleep(wait)
    _last_call[0] = time.time()
    req = urllib.request.Request(url, headers={"User-Agent": "dota-local/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 429 and _retry < 4:
            back = 3 * (2 ** _retry)          # 3, 6, 12, 24 秒
            print("429 限流,%d 秒后重试(第 %d 次)" % (back, _retry + 1))
            time.sleep(back)
            return _get_json(url, _retry + 1)
        # 522/超时/网关类:多为对端临时故障(尤其 OpenDota /search),退避重试几次
        if e.code in (500, 502, 503, 504, 522, 524) and _retry < 3:
            back = 2 * (_retry + 1)           # 2, 4, 6 秒
            print("上游 %s,%d 秒后重试(第 %d 次)" % (e.code, back, _retry + 1))
            time.sleep(back)
            return _get_json(url, _retry + 1)
        raise
    except urllib.error.URLError as e:          # 连接超时/无响应
        if _retry < 2:
            time.sleep(2 * (_retry + 1))
            return _get_json(url, _retry + 1)
        raise


def api_get(path, params):
    """Valve WebAPI 调用(自动带 key)。"""
    p = dict(params)
    p["key"] = KEY
    return _get_json(BASE + path + "?" + urllib.parse.urlencode(p))


def od_get(path, params=None):
    """OpenDota 调用(免 key;设了 OPENDOTA_API_KEY 则带上)。"""
    params = dict(params or {})
    if OPENDOTA_KEY:
        params["api_key"] = OPENDOTA_KEY
    q = ("?" + urllib.parse.urlencode(params)) if params else ""
    return _get_json(OPENDOTA + path + q)


# ---------------- SQLite 索引库(路线A) ----------------
def get_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS matches(
        match_id   INTEGER PRIMARY KEY,
        seq_num    INTEGER,
        start_time INTEGER,
        data       TEXT)""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_seq ON matches(seq_num)")
    con.execute("CREATE TABLE IF NOT EXISTS od_cache(match_id INTEGER PRIMARY KEY, data TEXT)")
    return con


def db_get_od(match_id):
    con = get_db()
    row = con.execute("SELECT data FROM od_cache WHERE match_id=?", (int(match_id),)).fetchone()
    con.close()
    return json.loads(row[0]) if row else None


def db_put_od(match_id, data):
    con = get_db()
    con.execute("INSERT OR REPLACE INTO od_cache(match_id, data) VALUES(?,?)",
                (int(match_id), json.dumps(data, ensure_ascii=False)))
    con.commit()
    con.close()


def fetch_match_opendota(match_id):
    """按 match_id 从 OpenDota 取完整详情(它就是按 match_id 索引的)。
    结果缓存进 SQLite:同一局只调一次 OpenDota,之后离线秒回,避开配额。"""
    cached = db_get_od(match_id)
    if cached is not None:
        return {"result": cached, "_source": "od-cache"}
    try:
        d = od_get("/matches/%d" % int(match_id))
    except Exception as e:
        return {"error": "OpenDota 查询失败: %s" % e}
    if not d or not d.get("match_id"):
        return {"error": "OpenDota 未收录该对局(可能太新还没入库,或 id 无效)"}
    db_put_od(match_id, d)
    return {"result": d, "_source": "od"}


def db_get_match(match_id):
    con = get_db()
    row = con.execute("SELECT data FROM matches WHERE match_id=?", (int(match_id),)).fetchone()
    con.close()
    return json.loads(row[0]) if row else None


def db_put_matches(matches):
    """批量写入(match 对象来自 BySequenceNum,本身就是完整详情)。"""
    con = get_db()
    con.executemany(
        "INSERT OR IGNORE INTO matches(match_id, seq_num, start_time, data) VALUES(?,?,?,?)",
        [(m["match_id"], m.get("match_seq_num"), m.get("start_time"), json.dumps(m, ensure_ascii=False))
         for m in matches])
    con.commit()
    n = con.total_changes
    con.close()
    return n


def get_heroes():
    """hero_id -> {name, short, img}。缓存一次,大版本更新后重启即可刷新。"""
    global _hero_cache
    if _hero_cache is None:
        _hero_cache = {}
        try:
            d = api_get("/IEconDOTA2_570/GetHeroes/v1/", {"language": "zh"})
            for h in d.get("result", {}).get("heroes", []):
                short = h["name"].replace("npc_dota_hero_", "")
                _hero_cache[str(h["id"])] = {
                    "name": h.get("localized_name") or h["name"],
                    "short": short,
                    "img": "https://cdn.cloudflare.steamstatic.com/apps/dota2/images/dota_react/heroes/%s.png" % short,
                }
        except Exception as e:
            print("GetHeroes 失败(英雄名会退化成 id):", e)
    return _hero_cache


_items_cache = None


def get_items():
    """item_id -> {name, img}。用 OpenDota 常量(Valve 的 GetGameItems 已废弃/404)。"""
    global _items_cache
    if _items_cache is None:
        _items_cache = {}
        try:
            d = od_get("/constants/item_ids")   # {"1":"blink", "127":"blade_mail", ...}
            for iid, short in (d or {}).items():
                pretty = short.replace("recipe_", "配方:").replace("_", " ").strip().title()
                _items_cache[str(iid)] = {
                    "name": pretty,
                    "img": "https://cdn.cloudflare.steamstatic.com/apps/dota2/images/dota_react/items/%s.png" % short,
                }
        except Exception as e:
            print("OpenDota item_ids 失败(装备图标会退化):", e)
    return _items_cache


def fetch_match(match_id):
    """按 match_id 查:GetMatchDetails,Valve 偶发 500,重试 3 次。"""
    last = None
    for _ in range(3):
        try:
            d = api_get("/IDOTA2Match_570/GetMatchDetails/v1/", {"match_id": match_id})
            return d
        except urllib.error.HTTPError as e:
            last = "HTTP %s" % e.code
        except Exception as e:
            last = str(e)
    return {"error": "GetMatchDetails 失败: %s。这是 Valve 端偶发 500,可多试几次,或改用 seq_num 模式。" % last}


def fetch_history(account_id, count=25, hero_id=None):
    """按 account_id 查最近对局(Valve 原生)。传 hero_id 则只返回该英雄的对局。
    每条带 match_id + match_seq_num,用来点开详情。"""
    aid = int(account_id)
    if aid > STEAM64_BASE:          # 用户填了 64 位 steamid,转成 32 位
        aid -= STEAM64_BASE
    params = {"account_id": aid, "matches_requested": count}
    if hero_id:
        params["hero_id"] = int(hero_id)     # 原生按英雄筛选
    try:
        d = api_get("/IDOTA2Match_570/GetMatchHistory/v1/", params)
        res = d.get("result", {})
        if res.get("status") != 1:
            return {"error": "GetMatchHistory status=%s(该号可能没开“公开对战数据”,Dota设置里勾选后才可查)" % res.get("status")}
        heroes = get_heroes()
        for m in res.get("matches", []) or []:
            for p in m.get("players", []) or []:
                p["_hero"] = heroes.get(str(p.get("hero_id")), {"name": "hero_id %s" % p.get("hero_id"), "img": ""})
        return {"result": res, "queried_account_id": aid, "hero_id": hero_id}
    except Exception as e:
        return {"error": "GetMatchHistory 失败: %s" % e}


def fetch_player_opendota(account_id):
    """OpenDota 玩家资料 + 常玩英雄(带胜率)。一次拿到,省去自己翻页统计。"""
    aid = int(account_id)
    if aid > STEAM64_BASE:
        aid -= STEAM64_BASE
    try:
        prof = od_get("/players/%d" % aid) or {}
        heroes = od_get("/players/%d/heroes" % aid) or []
    except Exception as e:
        return {"error": "OpenDota 玩家查询失败: %s" % e}
    return {
        "account_id": aid,
        "profile": prof.get("profile") or {},
        "rank_tier": prof.get("rank_tier"),
        "mmr_estimate": prof.get("computed_mmr") or (prof.get("mmr_estimate") or {}).get("estimate"),
        "heroes": _process_heroes(heroes, get_heroes()),
    }


def search_players(q):
    """按游戏昵称搜玩家(OpenDota /search;只索引玩过 Dota 的人,天然确认“玩过 Dota”)。
    按 last_match_time(最后一把)从近到远排序。Valve 没有按昵称搜人的接口,只能走这里。"""
    try:
        arr = od_get("/search", {"q": q}) or []
    except urllib.error.HTTPError as e:
        if e.code in (500, 502, 503, 504, 522, 524):
            return {"error": "OpenDota 搜索服务暂时不可用(HTTP %s,对端超时/维护),稍后再试。这不是名字问题。" % e.code}
        return {"error": "OpenDota 搜索失败: HTTP %s" % e.code}
    except Exception as e:
        return {"error": "OpenDota 搜索失败: %s" % e}
    # ISO 时间字符串按字典序即按时间序;无时间的排最后
    arr.sort(key=lambda x: (x.get("last_match_time") or ""), reverse=True)
    return {"query": q, "results": arr}


def _process_heroes(raw, hmap, topn=15):
    """OpenDota heroes 原始列表 -> 精简 top 列表(带中文名/图标/胜率)。"""
    lst = []
    for h in raw or []:
        g = h.get("games") or 0
        if g <= 0:
            continue
        hid = str(h.get("hero_id"))
        info = hmap.get(hid, {"name": "hero_id " + hid, "img": ""})
        w = h.get("win") or 0
        lst.append({"hero_id": h.get("hero_id"), "games": g, "win": w,
                    "winrate": round(100.0 * w / g, 1), "name": info["name"], "img": info["img"]})
    lst.sort(key=lambda x: -x["games"])
    return lst[:topn]


def _lobby_ok(m, filt):
    if filt == "ranked":
        return m.get("lobby_type") == 7
    if filt == "turbo":
        return m.get("game_mode") == 23
    if filt == "normal":
        return m.get("lobby_type") == 0 and m.get("game_mode") != 23
    return True


def fetch_player_heroes(account_id, filt="all", min_rank=None):
    """常玩英雄,可按 全部/排位/匹配/快速 过滤,并可加“对局平均段位下限”min_rank。
    normal(匹配)= lobby_type 0 减去 turbo(game_mode 23)。
    设了 min_rank 时,/heroes 不支持段位过滤,改从完整 /matches 按 average_rank 现场聚合。"""
    aid = int(account_id)
    if aid > STEAM64_BASE:
        aid -= STEAM64_BASE
    hmap = get_heroes()

    if min_rank is not None:
        try:
            matches = od_get("/players/%d/matches" % aid) or []   # 全部对局(每条带 average_rank)
        except Exception as e:
            return {"error": "OpenDota 战绩失败: %s" % e}
        agg = {}
        for m in matches:
            ar = m.get("average_rank")
            if ar is None or ar < min_rank:
                continue
            if not _lobby_ok(m, filt):
                continue
            h = m.get("hero_id")
            radiant = int(m.get("player_slot", 0)) < 128
            win = 1 if (radiant == bool(m.get("radiant_win"))) else 0
            a = agg.setdefault(h, {"hero_id": h, "games": 0, "win": 0})
            a["games"] += 1
            a["win"] += win
        return {"account_id": aid, "filter": filt, "min_rank": min_rank,
                "heroes": _process_heroes(list(agg.values()), hmap)}

    base = "/players/%d/heroes" % aid
    try:
        if filt == "ranked":
            raw = od_get(base, {"lobby_type": 7})
        elif filt == "turbo":
            raw = od_get(base, {"game_mode": 23})
        elif filt == "normal":
            lobby0 = od_get(base, {"lobby_type": 0}) or []
            turbo = {h["hero_id"]: h for h in (od_get(base, {"game_mode": 23}) or [])}
            raw = []
            for h in lobby0:
                t = turbo.get(h["hero_id"], {})
                raw.append({"hero_id": h["hero_id"],
                            "games": (h.get("games") or 0) - (t.get("games") or 0),
                            "win": (h.get("win") or 0) - (t.get("win") or 0)})
        else:
            raw = od_get(base) or []
    except Exception as e:
        return {"error": "OpenDota 英雄战绩失败: %s" % e}
    return {"account_id": aid, "filter": filt, "min_rank": None, "heroes": _process_heroes(raw, hmap)}


def fetch_player_matches(account_id, limit=50, hero_id=None):
    """OpenDota 玩家战绩(带 average_rank 水平段、lobby_type、game_mode)。
    传 hero_id 则只返回该英雄的对局(比 Valve GetMatchHistory 稳,任意账号可用)。"""
    aid = int(account_id)
    if aid > STEAM64_BASE:
        aid -= STEAM64_BASE
    params = {"limit": limit}
    if hero_id:
        params["hero_id"] = int(hero_id)
    try:
        arr = od_get("/players/%d/matches" % aid, params) or []
    except Exception as e:
        return {"error": "OpenDota 玩家战绩失败: %s" % e}
    hmap = get_heroes()
    out = []
    for m in arr:
        hid = str(m.get("hero_id"))
        info = hmap.get(hid, {"name": "hero_id " + hid, "img": ""})
        radiant = int(m.get("player_slot", 0)) < 128
        m["_hero"] = info
        m["_win"] = (radiant == bool(m.get("radiant_win")))
        out.append(m)
    return {"account_id": aid, "matches": out, "hero_id": hero_id}


def fetch_by_seq(seq_num):
    """按 match_seq_num 查:GetMatchHistoryBySequenceNum,稳定,结构同 GetMatchDetails。"""
    try:
        d = api_get("/IDOTA2Match_570/GetMatchHistoryBySequenceNum/v1/",
                    {"start_at_match_seq_num": seq_num, "matches_requested": 1})
        matches = d.get("result", {}).get("matches", [])
        if not matches:
            return {"error": "该 seq_num 未返回对局"}
        return {"result": matches[0]}
    except Exception as e:
        return {"error": "GetMatchHistoryBySequenceNum 失败: %s" % e}


# ---- match_id -> seq_num 定位器(路线B:插值+二分) ----
# 锚点:一对已知的 (match_id, seq_num),用来估算全局斜率。两者都随时间从 ~0 增长,
# 所以 seq ≈ SLOPE * match_id 是个不错的初猜。大版本后可换更新的锚点。
ANCHOR_ID = 8907752871
ANCHOR_SEQ = 7487669010
SLOPE = ANCHOR_SEQ / ANCHOR_ID          # ≈ 0.8406 seq / match_id
_seq_cache = {}                          # match_id -> 命中过的完整 match 对象(省得重查)


def _seq_batch(seq):
    d = api_get("/IDOTA2Match_570/GetMatchHistoryBySequenceNum/v1/",
                {"start_at_match_seq_num": max(1, int(seq)), "matches_requested": 100})
    return d.get("result", {}).get("matches", []) or []


RESOLVE_MAX_CALLS = int(os.environ.get("DOTA_RESOLVE_MAX_CALLS", "25"))  # 在线兜底封顶,防限流


def resolve_match_by_id(target_id):
    """任意 match_id → 完整对局。
    先查本地索引库(秒回);库里没有再走在线定位(封顶 RESOLVE_MAX_CALLS 次),
    命中后写回库,越用越全。"""
    target = int(target_id)

    # 1) 本地索引库
    cached = db_get_match(target)
    if cached:
        out = dict(cached); out["_source"] = "db"; out["_resolver_calls"] = 0
        return {"result": out}

    # 2) 在线定位(插值+二分,封顶)
    guess = max(1, int(target * SLOPE))
    lo, hi = 1, None
    seen = set()
    calls = 0
    while calls < RESOLVE_MAX_CALLS:
        if guess in seen:
            guess = (lo + (hi if hi else guess * 2)) // 2
        seen.add(guess)
        batch = _seq_batch(guess)
        calls += 1
        if not batch:
            hi = guess
            guess = max(1, (lo + guess) // 2)
            continue
        db_put_matches(batch)                    # 顺手把扫到的都入库
        hit = next((m for m in batch if m["match_id"] == target), None)
        if hit:
            out = dict(hit); out["_source"] = "online"; out["_resolver_calls"] = calls
            return {"result": out}
        ids = [m["match_id"] for m in batch]
        min_id, max_id = min(ids), max(ids)
        seq0 = batch[0]["match_seq_num"]
        seqN = batch[-1]["match_seq_num"]
        if target < min_id:
            hi = seq0
            g = seq0 - (int((min_id - target) * SLOPE) + 1)
            guess = (lo + seq0) // 2 if g <= lo else g
        elif target > max_id:
            lo = seqN
            g = seqN + (int((target - max_id) * SLOPE) + 1)
            guess = (seqN + hi) // 2 if (hi and g >= hi) else g
        else:
            # 落在本窗口 id 区间但没精确命中:seq 抖动(长局晚入库),向两侧成批顺扫
            for k in range(1, RESOLVE_MAX_CALLS - calls):
                for d in (k * 100, -k * 100):
                    b2 = _seq_batch(seq0 + d)
                    calls += 1
                    db_put_matches(b2)
                    hit = next((m for m in b2 if m["match_id"] == target), None)
                    if hit:
                        out = dict(hit); out["_source"] = "online"; out["_resolver_calls"] = calls
                        return {"result": out}
                    if calls >= RESOLVE_MAX_CALLS:
                        break
                if calls >= RESOLVE_MAX_CALLS:
                    break
            return {"error": "match_id %d 未命中(seq 抖动过大或非公开局)。已用 %d 次调用。建议:用①按 account_id 查(精确),或先 ingest.py 灌流入库。" % (target, calls)}
    return {"error": "match_id %d 在线定位超预算(%d 次)。建议用①按 account_id 查,或用 ingest.py 把该时段灌入索引库。" % (target, calls)}


def enrich(data):
    """给每个 player 补上:_hero(中文名/头像图) 与 _persona/_avatar/_anon(玩家资料)。"""
    res = data.get("result")
    if not res or not isinstance(res, dict):
        return data
    players = res.get("players", []) or []

    ids64 = []
    for p in players:
        aid = p.get("account_id")
        if aid is not None and aid != ANON:
            ids64.append(str(STEAM64_BASE + int(aid)))

    summaries = {}
    if ids64:
        try:
            d = api_get("/ISteamUser/GetPlayerSummaries/v2/", {"steamids": ",".join(ids64)})
            for s in d.get("response", {}).get("players", []):
                summaries[s["steamid"]] = s
        except Exception as e:
            print("GetPlayerSummaries 失败(头像会缺):", e)

    heroes = get_heroes()
    items = get_items()
    for p in players:
        aid = p.get("account_id")
        # side: player_slot 的 0x80 位;0=天辉 1=夜魇(比 team_number 更可靠)
        p["_side"] = 1 if (int(p.get("player_slot", 0)) & 128) else 0
        p["_hero"] = heroes.get(str(p.get("hero_id")), {"name": "hero_id %s" % p.get("hero_id"), "img": ""})
        # 装备图标:item_0..5(0/None 为空格子,跳过);中立装备单列
        p["_items"] = []
        for k in ("item_0", "item_1", "item_2", "item_3", "item_4", "item_5"):
            iid = p.get(k)
            if iid:
                info = items.get(str(iid))
                p["_items"].append({"id": iid,
                                    "name": info["name"] if info else str(iid),
                                    "img": info["img"] if info else ""})
        nid = p.get("item_neutral")
        if nid:
            info = items.get(str(nid))
            p["_neutral"] = {"id": nid, "name": info["name"] if info else str(nid),
                             "img": info["img"] if info else ""}
        if aid is None or aid == ANON:
            p["_anon"] = True
        else:
            sid = str(STEAM64_BASE + int(aid))
            p["_steamid64"] = sid
            s = summaries.get(sid)
            if s:
                p["_persona"] = s.get("personaname")
                p["_avatar"] = s.get("avatarmedium")
                p["_profileurl"] = s.get("profileurl")
    return data


class Handler(http.server.BaseHTTPRequestHandler):
    def _send(self, code, body, ctype="application/json; charset=utf-8"):
        b = body.encode("utf-8") if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        if u.path in ("/", "/index.html"):
            try:
                with open(os.path.join(HERE, "index.html"), "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
            except FileNotFoundError:
                self._send(404, "index.html 不存在", "text/plain; charset=utf-8")
            return
        if u.path == "/api/match":
            q = urllib.parse.parse_qs(u.query)
            val = (q.get("id") or [""])[0].strip()
            mode = (q.get("mode") or ["id"])[0]
            if not val.isdigit():
                self._send(400, json.dumps({"error": "ID 必须是数字"}, ensure_ascii=False))
                return
            if mode == "od":
                data = fetch_match_opendota(val)  # 按 match_id 从 OpenDota 取(推荐)
            elif mode == "seq":
                data = fetch_by_seq(val)          # 按 seq_num 从 Valve 取
            elif mode == "id":
                data = resolve_match_by_id(val)   # 在线定位器(兜底,慢)
            else:
                data = fetch_match(val)
            data = enrich(data)
            self._send(200, json.dumps(data, ensure_ascii=False))
            return
        if u.path == "/api/history":
            q = urllib.parse.parse_qs(u.query)
            val = (q.get("id") or [""])[0].strip()
            hero = (q.get("hero_id") or [""])[0].strip()
            if not val.isdigit():
                self._send(400, json.dumps({"error": "account_id 必须是数字"}, ensure_ascii=False))
                return
            data = fetch_history(val, hero_id=(int(hero) if hero.isdigit() else None))
            self._send(200, json.dumps(data, ensure_ascii=False))
            return
        if u.path == "/api/player":
            q = urllib.parse.parse_qs(u.query)
            val = (q.get("id") or [""])[0].strip()
            if not val.isdigit():
                self._send(400, json.dumps({"error": "account_id 必须是数字"}, ensure_ascii=False))
                return
            self._send(200, json.dumps(fetch_player_opendota(val), ensure_ascii=False))
            return
        if u.path == "/api/search":
            q = urllib.parse.parse_qs(u.query)
            name = (q.get("q") or [""])[0].strip()
            if not name:
                self._send(400, json.dumps({"error": "请输入游戏名"}, ensure_ascii=False))
                return
            self._send(200, json.dumps(search_players(name), ensure_ascii=False))
            return
        if u.path == "/api/player_heroes":
            q = urllib.parse.parse_qs(u.query)
            val = (q.get("id") or [""])[0].strip()
            filt = (q.get("filter") or ["all"])[0]
            mr = (q.get("min_rank") or [""])[0].strip()
            if not val.isdigit():
                self._send(400, json.dumps({"error": "account_id 必须是数字"}, ensure_ascii=False))
                return
            data = fetch_player_heroes(val, filt, min_rank=(int(mr) if mr.isdigit() else None))
            self._send(200, json.dumps(data, ensure_ascii=False))
            return
        if u.path == "/api/player_matches":
            q = urllib.parse.parse_qs(u.query)
            val = (q.get("id") or [""])[0].strip()
            hero = (q.get("hero_id") or [""])[0].strip()
            if not val.isdigit():
                self._send(400, json.dumps({"error": "account_id 必须是数字"}, ensure_ascii=False))
                return
            data = fetch_player_matches(val, hero_id=(int(hero) if hero.isdigit() else None))
            self._send(200, json.dumps(data, ensure_ascii=False))
            return
        self._send(404, json.dumps({"error": "not found"}))

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    print("英雄/装备表预热中...")
    get_heroes()
    get_items()
    print("Dota 查询服务已启动:  %s:%d" % (HOST, PORT))
    http.server.HTTPServer((HOST, PORT), Handler).serve_forever()
