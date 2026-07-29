#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
灌流入库 (路线A) —— 把 GetMatchHistoryBySequenceNum 的顺序流写进 dota.db。
入库后,server.py 按 match_id 查就是秒回、离线、不再触限流。

复用 server.py 里的限速调用,所以同样 ~1 次/秒,安全。

用法:
  python ingest.py                      # 从库里最大 seq 继续灌(增量);库空则从最近往回一段
  python ingest.py --from-seq 7487669010 --count 5000   # 从指定 seq 灌 5000 局
  python ingest.py --from-match-id 8897157671 --count 20000
        # 用插值把 match_id 换算成大致 seq 起点,灌一段(覆盖这个 match_id 所在时段)

覆盖策略:你日常搜的多是近期局,先 `python ingest.py --count 50000` 灌最近一段,
之后挂着 `python ingest.py`(增量)让它持续跟进即可。想查某个较老的 match_id,
就 --from-match-id 它,灌它周围一段。
"""
import argparse
import sqlite3

import server  # 复用 api_get / _seq_batch / db / SLOPE


def current_max_seq():
    """取库里已灌到的最大 seq;库空返回 None。"""
    con = sqlite3.connect(server.DB_PATH)
    try:
        server.get_db().close()  # 确保表存在
        row = con.execute("SELECT MAX(seq_num) FROM matches").fetchone()
        return row[0]
    except sqlite3.OperationalError:
        return None
    finally:
        con.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-seq", type=int, help="从该 match_seq_num 开始灌")
    ap.add_argument("--from-match-id", type=int, help="用插值换算成 seq 起点(覆盖该 match_id 时段)")
    ap.add_argument("--count", type=int, default=20000, help="最多灌多少局(默认 20000)")
    args = ap.parse_args()

    if args.from_seq:
        cursor = args.from_seq
    elif args.from_match_id:
        cursor = max(1, int(args.from_match_id * server.SLOPE) - 5000)  # 稍微往前留余量
        print("match_id %d → 估算 seq 起点 %d" % (args.from_match_id, cursor))
    else:
        mx = current_max_seq()
        if mx:
            cursor = mx + 1
            print("增量续灌,从 seq %d 继续" % cursor)
        else:
            # 库空:从一个近期锚点往回退一段起步
            cursor = server.ANCHOR_SEQ - args.count
            print("库为空,从 seq %d 起步(近期一段)" % cursor)

    total = 0
    empty_hits = 0
    while total < args.count:
        batch = server._seq_batch(cursor)
        if not batch:
            empty_hits += 1
            print("seq %d 无数据(可能已到最新)。" % cursor)
            if empty_hits >= 2:
                print("已追平最新,停止。")
                break
            continue
        empty_hits = 0
        n = server.db_put_matches(batch)
        total += len(batch)
        last_seq = batch[-1]["match_seq_num"]
        print("灌入 seq %d..%d  本批 %d 局(新增 %d)  累计 %d" %
              (batch[0]["match_seq_num"], last_seq, len(batch), n, total))
        cursor = last_seq + 1

    print("完成,共处理 %d 局,库文件:%s" % (total, server.DB_PATH))


if __name__ == "__main__":
    main()
