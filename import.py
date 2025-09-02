#!/usr/bin/env python3
import argparse
import json
import time

import boto3


def load_requests(path, table_arg):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    table = table_arg
    # items 配列を取り出す（形のゆらぎを吸収）
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        if "Items" in data:
            items = data["Items"]
        elif "items" in data:
            items = data["items"]
        elif "RequestItems" in data and isinstance(data["RequestItems"], dict):
            # RequestItems 形式ならテーブル名を特定
            if not table:
                keys = list(data["RequestItems"].keys())
                if len(keys) == 1:
                    table = keys[0]
                else:
                    raise ValueError("RequestItems に複数テーブルがあるため --table を指定してください。")
            if table not in data["RequestItems"]:
                raise ValueError('RequestItems に "{}" が見つかりません。'.format(table))
            items = data["RequestItems"][table]
        else:
            raise ValueError("入力JSONから Items/items/RequestItems を特定できません。")
    else:
        raise ValueError("入力JSONはオブジェクトまたは配列である必要があります。")

    if not table:
        raise ValueError("--table を指定するか、RequestItems から推測できる形にしてください。")

    # 各要素を PutRequest 形式にそろえる（型はそのまま）
    reqs = []
    for el in items:
        if isinstance(el, dict) and ("PutRequest" in el or "DeleteRequest" in el):
            reqs.append(el)
        elif isinstance(el, dict) and "Item" in el:
            reqs.append({"PutRequest": {"Item": el["Item"]}})
        else:
            reqs.append({"PutRequest": {"Item": el}})
    return reqs, table


def chunks(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def main():
    ap = argparse.ArgumentParser(description="Batch import DynamoDB items from JSON (AttributeValue format).")
    ap.add_argument("-t", "--table", required=False, help="DynamoDB table name")
    ap.add_argument("-f", "--file", required=True, help="Input JSON path")
    ap.add_argument("--region", default=None, help="AWS region")
    ap.add_argument("--profile", default=None, help="AWS named profile")
    ap.add_argument("--sleep-max", type=int, default=30, help="バックオフの最大秒数 (default: 30)")
    args = ap.parse_args()

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    client = session.client("dynamodb")

    reqs, table = load_requests(args.file, args.table)
    total = len(reqs)
    print("Table: {} / Total requests: {}".format(table, total))

    sent = 0
    for batch in chunks(reqs, 25):
        request_items = {table: batch}
        backoff = 1
        while True:
            resp = client.batch_write_item(RequestItems=request_items)
            unp = resp.get("UnprocessedItems") or {}
            unp_count = sum(len(v) for v in unp.values()) if unp else 0
            if unp_count == 0:
                sent += len(batch)
                break
            # 未処理のみ再送（指数バックオフ）
            request_items = unp
            time.sleep(backoff)
            backoff = min(backoff * 2, args.sleep_max)
        print("Progress: {}/{}".format(sent, total))

    print("Done.")


if __name__ == "__main__":
    main()
