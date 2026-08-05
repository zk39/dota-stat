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
import threading
import urllib.request
import urllib.parse
import urllib.error
import http.server


def _load_env_file():
    """直接 python server.py 时,自动加载同目录 .env(纯标准库)。
    已存在的真实环境变量优先(所以 Docker 的 env_file 路径不受影响)。"""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v


_load_env_file()

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

MIN_INTERVAL = float(os.environ.get("DOTA_MIN_INTERVAL", "1.1"))  # 每次上游调用最小间隔(秒)
_last_call = [0.0]
_throttle_lock = threading.Lock()   # 多线程下把出站上游调用限在 ~1/秒

# ---- TTL 内存缓存:玩家/搜索/战绩这类“会变”的数据缓存一小段时间,挡住重复请求省额度 ----
TTL = int(os.environ.get("DOTA_CACHE_TTL", "600"))      # 秒,默认 10 分钟
MAX_CACHE = int(os.environ.get("DOTA_CACHE_MAX", "2000"))  # 缓存条目上限,超了删最早的一批
_ttl_store = {}
_ttl_lock = threading.Lock()

# ---- 限速 / 熔断:公网暴露后防止被刷爆(nginx 层也应该配一份,这里是第二道保险) ----
# 按 IP 的固定窗口限速,只管 /api/*(静态页不算钱,不限)。
RATE_WINDOW = 60
RATE_MAX = int(os.environ.get("DOTA_RATE_LIMIT_PER_MIN", "60"))   # 每 IP 每分钟最多 /api 请求数
_rate_store = {}
_rate_lock = threading.Lock()

# 全局并发上限:同时在处理的请求数超过这个数就直接 503,防止极端流量把线程/内存打爆
# (ThreadingHTTPServer 是来一个连接开一个线程,没有这个的话没有上限)。
MAX_CONCURRENT = int(os.environ.get("DOTA_MAX_CONCURRENT", "40"))
_concurrency_sem = threading.Semaphore(MAX_CONCURRENT)
CONCURRENCY_WAIT = float(os.environ.get("DOTA_CONCURRENCY_WAIT", "2"))  # 等不到空位就放弃、直接拒绝(秒)


def client_ip(handler):
    """取真实客户端 IP。优先 X-Real-IP / CF-Connecting-IP(由 nginx real_ip 模块 / Cloudflare
    设置,客户端伪造不了);X-Forwarded-For 首值可被客户端伪造,不能用来限速,仅作为备用来源。"""
    for h in ("X-Real-IP", "CF-Connecting-IP"):
        v = handler.headers.get(h)
        if v:
            return v.strip()
    xff = handler.headers.get("X-Forwarded-For")
    if xff:
        return xff.split(",")[0].strip()
    return handler.client_address[0]


def rate_limited(ip):
    """固定窗口限速:同一 IP 同一分钟内超过 RATE_MAX 次 /api 请求就拒绝。"""
    now = time.time()
    bucket = int(now // RATE_WINDOW)
    key = (ip, bucket)
    with _rate_lock:
        if len(_rate_store) > 20000:      # 顺手清掉旧窗口,别无限攒
            for k in list(_rate_store.keys()):
                if k[1] < bucket:
                    _rate_store.pop(k, None)
        n = _rate_store.get(key, 0) + 1
        _rate_store[key] = n
    return n > RATE_MAX


_flight_locks = {}
_flight_guard = threading.Lock()


def _flight_lock(key):
    with _flight_guard:
        lk = _flight_locks.get(key)
        if lk is None:
            lk = threading.Lock()
            _flight_locks[key] = lk
        return lk


def cached(key, fn, ttl=TTL):
    """命中且未过期→返回缓存;否则算一次并存(不缓存报错结果)。
    single-flight:同一 key 并发只算一次,其余线程等结果,防止缓存击穿把上游打爆。"""
    with _ttl_lock:
        v = _ttl_store.get(key)
        if v and v[0] > time.time():
            return v[1]
    with _flight_lock(key):                        # 同 key 串行:第一个算,其余等
        with _ttl_lock:                            # 拿到锁后再查一次(别人可能已填好)
            v = _ttl_store.get(key)
            if v and v[0] > time.time():
                return v[1]
        val = fn()
        if not (isinstance(val, dict) and val.get("error")):
            with _ttl_lock:
                _ttl_store.pop(key, None)          # 重写则挪到末尾,保证“最早写入”排在前面
                _ttl_store[key] = (time.time() + ttl, val)
                if len(_ttl_store) > MAX_CACHE:    # 满了:删掉最早的一批,降到上限的 90%
                    target = MAX_CACHE * 9 // 10
                    for k in list(_ttl_store.keys())[:len(_ttl_store) - target]:
                        _ttl_store.pop(k, None)
            if len(_flight_locks) > 4 * MAX_CACHE:  # 顺手别让 flight 锁无限攒
                with _flight_guard:
                    _flight_locks.clear()
        return val


_hero_cache = None


OPENDOTA = "https://api.opendota.com/api"
OPENDOTA_KEY = os.environ.get("OPENDOTA_API_KEY")   # 可选,填了配额更高

STRATZ_TOKEN = os.environ.get("STRATZ_API_TOKEN")   # 玩家资料 / 对局详情的备用数据源(OpenDota 异常时使用)
STRATZ_URL = "https://api.stratz.com/graphql"
# 注:实测过 STRATZ 的 GraphQL schema(DotaQuery 根节点),它没有按昵称搜索的字段——
# player(steamAccountId)/players(steamAccountIds) 都只接受数字 ID,所以 STRATZ 不能替代
# OpenDota 的 /search。按 ID 查玩家资料(player)和对局详情(match)是支持的,故只在这两处接入。

# ---- 对局地区:Valve 的 region id -> 人类可读名称,数据取自 odota/dotaconstants(region.json)。
# cluster -> region 的映射(cluster.json)留作兜底:极老的对局有的没有直接的 region 字段。
REGION_NAMES_ZH = {
    1: "美西", 2: "美东", 3: "欧洲", 5: "新加坡", 6: "迪拜",
    7: "澳大利亚", 8: "斯德哥尔摩", 9: "奥地利", 10: "巴西", 11: "南非",
    12: "上海电信", 13: "中国联通", 14: "智利", 15: "秘鲁",
    16: "印度", 17: "广东电信", 18: "浙江电信",
    19: "日本", 20: "武汉电信", 25: "天津联通",
    37: "台湾", 38: "阿根廷",
}
CLUSTER_TO_REGION = {
    111: 1, 112: 1, 113: 1, 114: 1, 117: 1, 118: 1,
    121: 2, 122: 2, 123: 2, 124: 2,
    131: 3, 132: 3, 133: 3, 134: 3, 135: 3, 136: 3, 137: 3, 138: 3,
    141: 19, 142: 19, 143: 19, 144: 19, 145: 19,
    151: 5, 152: 5, 153: 5, 154: 5, 155: 5, 156: 5,
    161: 6, 162: 6, 163: 6,
    171: 7, 172: 7,
    181: 8, 182: 8, 183: 8, 184: 8, 185: 8, 186: 8, 187: 8, 188: 8,
    191: 9, 192: 9, 193: 9,
    200: 10, 201: 10, 202: 10, 203: 10, 204: 10,
    211: 11, 212: 11, 213: 11, 214: 11,
    221: 12, 222: 12, 223: 18, 224: 12, 225: 17, 227: 20,
    231: 13, 232: 25, 235: 13, 236: 13,
    241: 14, 242: 14,
    251: 15,
    261: 16,
    271: 3, 272: 3, 273: 3, 274: 3,
    346: 38, 347: 38,
    381: 15, 382: 15,
    410: 2, 412: 2,
}


def region_name(region_id=None, cluster_id=None):
    """region id / cluster id -> 中文地区名。优先用 region 字段,没有的话用 cluster 反查。"""
    rid = region_id
    if rid is None and cluster_id is not None:
        try:
            rid = CLUSTER_TO_REGION.get(int(cluster_id))
        except (TypeError, ValueError):
            rid = None
    if rid is None:
        return None
    try:
        return REGION_NAMES_ZH.get(int(rid))
    except (TypeError, ValueError):
        return None


def _throttle_gate():
    """限速门:确保出站调用 ≤1 次/秒(多线程共用一把锁)。"""
    with _throttle_lock:
        wait = MIN_INTERVAL - (time.time() - _last_call[0])
        if wait > 0:
            time.sleep(wait)
        _last_call[0] = time.time()


def _get_json(url, _retry=0, timeout=25, retry5xx=True):
    """统一的限速 + 429/5xx 退避 HTTP GET。retry5xx=False 用于响应不稳定的接口(如 /search),快速失败。"""
    _throttle_gate()
    req = urllib.request.Request(url, headers={"User-Agent": "dota-local/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 429 and _retry < 4:
            back = 3 * (2 ** _retry)          # 3, 6, 12, 24 秒
            print("429 限流,%d 秒后重试(第 %d 次)" % (back, _retry + 1))
            time.sleep(back)
            return _get_json(url, _retry + 1, timeout, retry5xx)
        # 522/超时/网关类:多为对端临时故障;retry5xx=False 时不重试、直接失败
        if retry5xx and e.code in (500, 502, 503, 504, 522, 524) and _retry < 3:
            back = 2 * (_retry + 1)           # 2, 4, 6 秒
            print("上游 %s,%d 秒后重试(第 %d 次)" % (e.code, back, _retry + 1))
            time.sleep(back)
            return _get_json(url, _retry + 1, timeout, retry5xx)
        raise
    except urllib.error.URLError as e:          # 连接超时/无响应
        if retry5xx and _retry < 2:
            time.sleep(2 * (_retry + 1))
            return _get_json(url, _retry + 1, timeout, retry5xx)
        raise


def api_get(path, params):
    """Valve WebAPI 调用(自动带 key)。"""
    p = dict(params)
    p["key"] = KEY
    return _get_json(BASE + path + "?" + urllib.parse.urlencode(p))


def od_get(path, params=None, timeout=25, retry5xx=True):
    """OpenDota 调用(免 key;设了 OPENDOTA_API_KEY 则带上)。"""
    params = dict(params or {})
    if OPENDOTA_KEY:
        params["api_key"] = OPENDOTA_KEY
    q = ("?" + urllib.parse.urlencode(params)) if params else ""
    return _get_json(OPENDOTA + path + q, timeout=timeout, retry5xx=retry5xx)


# ---------------- SQLite:OpenDota 对局缓存 ----------------
def get_db():
    con = sqlite3.connect(DB_PATH)
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
    hit = db_get_od(match_id)
    if hit is not None:
        return {"result": hit, "_source": "od-cache"}
    try:
        d = od_get("/matches/%d" % int(match_id))
    except Exception as e:
        return {"error": "OpenDota 查询失败: %s" % e}
    if not d or not d.get("match_id"):
        return {"error": "OpenDota 未收录该对局(可能太新还没入库,或 id 无效)"}
    db_put_od(match_id, d)
    return {"result": d, "_source": "od"}


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


def _stratz_request(query, variables=None):
    """统一的 STRATZ GraphQL POST(鉴权 + 限速 + 错误归一化)。返回 {"data": ...} 或 {"error": ...}。"""
    if not STRATZ_TOKEN:
        return {"error": "未配置 STRATZ_API_TOKEN"}
    payload = json.dumps({"query": query, "variables": variables or {}}).encode("utf-8")
    req = urllib.request.Request(
        STRATZ_URL, data=payload, method="POST",
        headers={"Authorization": "Bearer " + STRATZ_TOKEN,
                 "Content-Type": "application/json",
                 "Accept": "application/json",
                 "User-Agent": "STRATZ_API"})   # STRATZ 要求 UA 精确为此值,否则拒绝
    _throttle_gate()
    try:
        with urllib.request.urlopen(req, timeout=12) as r:
            d = json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8")[:200]
        except Exception:
            pass
        return {"error": "STRATZ HTTP %s %s" % (e.code, body)}
    except Exception as e:
        return {"error": "STRATZ 请求失败: %s" % e}
    if d.get("errors"):
        return {"error": "STRATZ GraphQL 错误: %s" % json.dumps(d["errors"], ensure_ascii=False)[:300]}
    return {"data": d.get("data") or {}}


def search_players(q):
    """按游戏昵称搜玩家。Valve 没有按昵称搜人的接口,只能用 OpenDota /search;
    STRATZ 的公开 API 不支持按名字搜索(实测过 schema,player/players 只接受数字 ID),
    所以这里没有备用数据源,查不到就是查不到。"""
    arr = None
    try:
        arr = od_get("/search", {"q": q}, timeout=6, retry5xx=False)   # 该接口响应不稳定,设置较短超时并快速失败
    except Exception as e:
        print("OpenDota /search 失败:", e)
    if arr:
        arr.sort(key=lambda x: (x.get("last_match_time") or ""), reverse=True)
        return {"query": q, "results": arr, "_source": "opendota"}
    return {"query": q, "results": []}


def stratz_fetch_player(account_id):
    """STRATZ 玩家资料备用查询(仅在 OpenDota 异常时使用)。只能拿到昵称/头像/段位/总场次胜场,
    拿不到常玩英雄列表(STRATZ 那部分数据结构复杂,收益不高,暂不接,使用备用源时英雄列表留空)。"""
    aid = int(account_id)
    if aid > STEAM64_BASE:
        aid -= STEAM64_BASE
    query = """
    query($id: Long!) {
      player(steamAccountId: $id) {
        matchCount
        winCount
        steamAccount { id name avatar seasonRank isAnonymous }
      }
    }
    """
    r = _stratz_request(query, {"id": aid})
    if r.get("error"):
        return {"error": "STRATZ 玩家查询失败: %s" % r["error"]}
    p = (r.get("data") or {}).get("player")
    if not p:
        return {"error": "STRATZ 未收录该玩家"}
    sa = p.get("steamAccount") or {}
    return {
        "account_id": aid,
        "profile": {
            "personaname": sa.get("name") or ("account %s" % aid),
            "avatarfull": sa.get("avatar") or "",
        },
        "rank_tier": sa.get("seasonRank"),
        "mmr_estimate": None,
        "heroes": [],
        "_source": "stratz",
    }


def fetch_player(account_id, source="auto"):
    """玩家资料。source: auto(默认,OpenDota 异常时自动切换至 STRATZ) / opendota(只用 OpenDota)
    / stratz(直接用 STRATZ)。STRATZ 备用源拿不到常玩英雄,仅用于 OpenDota 异常时应急。"""
    if source == "stratz":
        return stratz_fetch_player(account_id)
    r = fetch_player_opendota(account_id)
    if not r.get("error"):
        r["_source"] = "opendota"
        return r
    if source == "opendota":
        return r
    if STRATZ_TOKEN:
        r2 = stratz_fetch_player(account_id)
        if not r2.get("error"):
            return r2
        return {"error": "玩家查询暂时不可用(OpenDota 失败:%s;STRATZ 备用查询也失败:%s)" % (r["error"], r2["error"])}
    return r


def stratz_fetch_match(match_id):
    """STRATZ 对局详情备用查询(仅在 OpenDota 异常时使用)。字段名来自实测的 schema 内省
    (PlayerType/MatchType/MatchPlayerType/SteamAccountType),直接产出和 enrich() 之后
    同样结构的玩家字段,调用方不需要、也不应该再对结果调用 enrich()。"""
    mid = int(match_id)
    query = """
    query($id: Long!) {
      match(id: $id) {
        id
        didRadiantWin
        durationSeconds
        lobbyType
        gameMode
        averageRank
        regionId
        radiantKills
        direKills
        players {
          steamAccountId
          isRadiant
          heroId
          kills
          deaths
          assists
          numLastHits
          numDenies
          goldPerMinute
          experiencePerMinute
          networth
          item0Id item1Id item2Id item3Id item4Id item5Id
          neutral0Id
          steamAccount { id name avatar isAnonymous }
        }
      }
    }
    """
    r = _stratz_request(query, {"id": mid})
    if r.get("error"):
        return {"error": "STRATZ 对局查询失败: %s" % r["error"]}
    m = (r.get("data") or {}).get("match")
    if not m:
        return {"error": "STRATZ 未收录该对局"}

    heroes = get_heroes()
    items = get_items()
    players = []
    for p in (m.get("players") or []):
        hid = p.get("heroId")
        hinfo = heroes.get(str(hid), {"name": "hero_id %s" % hid, "img": ""})
        pitems = []
        for k in ("item0Id", "item1Id", "item2Id", "item3Id", "item4Id", "item5Id"):
            iid = p.get(k)
            if iid:
                info = items.get(str(iid))
                pitems.append({"id": iid, "name": info["name"] if info else str(iid),
                                "img": info["img"] if info else ""})
        neutral = None
        nid = p.get("neutral0Id")
        if nid:
            info = items.get(str(nid))
            neutral = {"id": nid, "name": info["name"] if info else str(nid),
                       "img": info["img"] if info else ""}
        sa = p.get("steamAccount") or {}
        aid = p.get("steamAccountId")
        players.append({
            "account_id": aid,
            "_side": 0 if p.get("isRadiant") else 1,
            "_hero": hinfo,
            "_items": pitems,
            "_neutral": neutral,
            "_anon": bool(aid is None or sa.get("isAnonymous")),
            "_persona": sa.get("name"),
            "_avatar": sa.get("avatar"),
            "rank_tier": m.get("averageRank"),   # STRATZ 不给逐人段位,只给整场平均,退化处理
            "kills": p.get("kills"), "deaths": p.get("deaths"), "assists": p.get("assists"),
            "net_worth": p.get("networth"),
            "gold_per_min": p.get("goldPerMinute"), "xp_per_min": p.get("experiencePerMinute"),
            "last_hits": p.get("numLastHits"), "denies": p.get("numDenies"),
        })

    rk = m.get("radiantKills") or []
    dk = m.get("direKills") or []
    result = {
        "match_id": m.get("id"),
        "duration": m.get("durationSeconds"),
        "radiant_win": m.get("didRadiantWin"),
        "radiant_score": rk[-1] if rk else None,   # STRATZ 给的是逐分钟数组,取最后一项当终局比分
        "dire_score": dk[-1] if dk else None,
        "lobby_type": m.get("lobbyType"),          # 字符串枚举(如 "RANKED"),不是 OpenDota 的数字编码
        "game_mode": m.get("gameMode"),            # 前端 lobbyLabel() 已经兼容两种取值
        "region": m.get("regionId"),
        "players": players,
    }
    result["_region_name"] = region_name(result.get("region"))
    # 不写永久 SQLite 缓存:那张表是给 OpenDota 原始结构用的(enrich() 依赖 hero_id/item_0
    # 这些字段名),STRATZ 这边已经是加工好的结构,混进去会在下次读缓存时被 enrich() 处理错。
    # 10 分钟内的重复请求已经有上层 cached() 的内存 TTL 兜着,足够用了。
    return {"result": result, "_source": "stratz"}


def fetch_match(match_id, source="auto"):
    """对局详情。source: auto(默认,OpenDota 异常时自动切换至 STRATZ) / opendota(只用 OpenDota)
    / stratz(直接用 STRATZ)。"""
    if source == "stratz":
        return stratz_fetch_match(match_id)
    r = fetch_match_opendota(match_id)
    if not r.get("error"):
        return enrich(r)
    if source == "opendota":
        return enrich(r)
    if STRATZ_TOKEN:
        r2 = stratz_fetch_match(match_id)
        if not r2.get("error"):
            return r2   # 已经是加工好的结构,不能再 enrich()
        return {"error": "对局查询暂时不可用(OpenDota 失败:%s;STRATZ 备用查询也失败:%s)" % (r["error"], r2["error"])}
    return enrich(r)


def fetch_rank(account_id):
    """只取段位(供搜索结果按行懒加载,轻量:一次 /players/{id})。"""
    aid = int(account_id)
    if aid > STEAM64_BASE:
        aid -= STEAM64_BASE
    try:
        prof = od_get("/players/%d" % aid) or {}
    except Exception as e:
        return {"error": str(e)}
    return {"account_id": aid, "rank_tier": prof.get("rank_tier")}


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


def fetch_player_peers(account_id, limit=20):
    """最近 N 把组队情况:OpenDota /players/{id}/peers 的 limit 参数就是“只统计最近 N 把”,
    一次调用就能拿到每个队友一起打了几把、赢了几把,不用逐场拉 20 次对局详情。"""
    aid = int(account_id)
    if aid > STEAM64_BASE:
        aid -= STEAM64_BASE
    try:
        raw = od_get("/players/%d/peers" % aid, {"limit": int(limit)}) or []
    except Exception as e:
        return {"error": "OpenDota 组队数据失败: %s" % e}
    out = []
    for p in raw:
        g = p.get("with_games") or 0
        if g <= 0:
            continue
        w = p.get("with_win") or 0
        out.append({
            "account_id": p.get("account_id"),
            "personaname": p.get("personaname") or ("#" + str(p.get("account_id"))),
            "avatar": p.get("avatar"),
            "games": g,
            "win": w,
            "winrate": round(100.0 * w / g, 1),
            "last_played": p.get("last_played"),
        })
    out.sort(key=lambda x: -x["games"])
    return {"account_id": aid, "limit": limit, "peers": out[:15]}


def enrich(data):
    """给每个 player 补上:_hero(中文名/头像图) 与 _persona/_avatar/_anon(玩家资料);
    给整场对局补上 _region_name(地区中文名)。"""
    res = data.get("result")
    if not res or not isinstance(res, dict):
        return data
    res["_region_name"] = region_name(res.get("region"), res.get("cluster"))
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
    def _send(self, code, body, ctype="application/json; charset=utf-8", headers=None):
        b = body.encode("utf-8") if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(b)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        # 熔断:并发太高就直接拒绝,不排队占线程(等 CONCURRENCY_WAIT 秒还抢不到位就放弃)
        if not _concurrency_sem.acquire(timeout=CONCURRENCY_WAIT):
            self._send(503, json.dumps({"error": "服务器当前请求过多,请稍后再试"}, ensure_ascii=False),
                       headers={"Retry-After": "3"})
            return
        try:
            self._route()
        finally:
            _concurrency_sem.release()

    def _route(self):
        u = urllib.parse.urlparse(self.path)
        if u.path.startswith("/api/") and rate_limited(client_ip(self)):
            self._send(429, json.dumps({"error": "请求太频繁,请求慢一点"}, ensure_ascii=False),
                       headers={"Retry-After": "30"})
            return
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
            src = (q.get("source") or ["auto"])[0].strip().lower()
            if src not in ("auto", "opendota", "stratz"):
                src = "auto"
            if not val.isdigit():
                self._send(400, json.dumps({"error": "ID 必须是数字"}, ensure_ascii=False))
                return
            data = cached("match:%s:%s" % (src, val), lambda: fetch_match(val, src))
            self._send(200, json.dumps(data, ensure_ascii=False))
            return
        if u.path == "/api/player":
            q = urllib.parse.parse_qs(u.query)
            val = (q.get("id") or [""])[0].strip()
            src = (q.get("source") or ["auto"])[0].strip().lower()
            if src not in ("auto", "opendota", "stratz"):
                src = "auto"
            if not val.isdigit():
                self._send(400, json.dumps({"error": "account_id 必须是数字"}, ensure_ascii=False))
                return
            data = cached("player:%s:%s" % (src, val), lambda: fetch_player(val, src))
            self._send(200, json.dumps(data, ensure_ascii=False))
            return
        if u.path == "/api/search":
            q = urllib.parse.parse_qs(u.query)
            name = (q.get("q") or [""])[0].strip()
            if not name:
                self._send(400, json.dumps({"error": "请输入游戏名"}, ensure_ascii=False))
                return
            data = cached("search:" + name, lambda: search_players(name))
            self._send(200, json.dumps(data, ensure_ascii=False))
            return
        if u.path == "/api/rank":
            q = urllib.parse.parse_qs(u.query)
            val = (q.get("id") or [""])[0].strip()
            if not val.isdigit():
                self._send(400, json.dumps({"error": "account_id 必须是数字"}, ensure_ascii=False))
                return
            self._send(200, json.dumps(cached("rank:" + val, lambda: fetch_rank(val), ttl=1800), ensure_ascii=False))
            return
        if u.path == "/api/player_heroes":
            q = urllib.parse.parse_qs(u.query)
            val = (q.get("id") or [""])[0].strip()
            filt = (q.get("filter") or ["all"])[0]
            mr = (q.get("min_rank") or [""])[0].strip()
            if not val.isdigit():
                self._send(400, json.dumps({"error": "account_id 必须是数字"}, ensure_ascii=False))
                return
            data = cached("heroes:%s:%s:%s" % (val, filt, mr),
                          lambda: fetch_player_heroes(val, filt, min_rank=(int(mr) if mr.isdigit() else None)))
            self._send(200, json.dumps(data, ensure_ascii=False))
            return
        if u.path == "/api/player_matches":
            q = urllib.parse.parse_qs(u.query)
            val = (q.get("id") or [""])[0].strip()
            hero = (q.get("hero_id") or [""])[0].strip()
            if not val.isdigit():
                self._send(400, json.dumps({"error": "account_id 必须是数字"}, ensure_ascii=False))
                return
            data = cached("matches:%s:%s" % (val, hero),
                          lambda: fetch_player_matches(val, hero_id=(int(hero) if hero.isdigit() else None)))
            self._send(200, json.dumps(data, ensure_ascii=False))
            return
        if u.path == "/api/player_peers":
            q = urllib.parse.parse_qs(u.query)
            val = (q.get("id") or [""])[0].strip()
            lim = (q.get("limit") or ["20"])[0].strip()
            if not val.isdigit():
                self._send(400, json.dumps({"error": "account_id 必须是数字"}, ensure_ascii=False))
                return
            lim = int(lim) if lim.isdigit() else 20
            data = cached("peers:%s:%s" % (val, lim), lambda: fetch_player_peers(val, limit=lim))
            self._send(200, json.dumps(data, ensure_ascii=False))
            return
        self._send(404, json.dumps({"error": "not found"}))

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    print("英雄/装备表预热中...")
    get_heroes()
    get_items()
    print("Dota 查询服务已启动:  %s:%d(多线程 + TTL缓存%d秒)" % (HOST, PORT, TTL))
    # ThreadingHTTPServer:并发处理多用户请求,不再一个个排队
    http.server.ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
