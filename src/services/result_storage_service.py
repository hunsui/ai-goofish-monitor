"""
结果数据的 SQLite 读写服务。
"""
from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime

from src.infrastructure.persistence.sqlite_bootstrap import bootstrap_sqlite_storage
from src.infrastructure.persistence.sqlite_connection import sqlite_connection
from src.infrastructure.persistence.storage_names import build_result_filename
from src.services.price_history_service import parse_price_value
from src.services.result_blacklist_service import (
    match_blacklist_keywords,
    normalize_blacklist_keywords,
)


SORT_COLUMN_MAP = {
    "crawl_time": "crawl_time",
    "publish_time": "COALESCE(publish_time, '')",
    "price": "COALESCE(price, 0)",
    "keyword_hit_count": "keyword_hit_count",
}


def _get_link_unique_key(link: str) -> str:
    return link.split("&", 1)[0]


def _fallback_unique_key(record: dict, item: dict) -> str:
    item_id = str(item.get("商品ID") or "").strip()
    if item_id:
        return f"item:{item_id}"
    digest = hashlib.sha1(
        json.dumps(record, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return f"hash:{digest}"


def _parse_raw_record(raw_json: str, *, status: str | None = None) -> dict:
    record = json.loads(raw_json)
    if status is not None:
        record["_status"] = status
    return record


def _build_query_conditions(
    *,
    filename: str,
    ai_recommended_only: bool,
    keyword_recommended_only: bool,
) -> tuple[str, list]:
    conditions = ["result_filename = ?"]
    params: list = [filename]
    if ai_recommended_only:
        conditions.append("is_recommended = 1")
        conditions.append("analysis_source = ?")
        params.append("ai")
    if keyword_recommended_only:
        conditions.append("is_recommended = 1")
        conditions.append("analysis_source = ?")
        params.append("keyword")
    return " AND ".join(conditions), params


def _sort_expression(sort_by: str, sort_order: str, *, active_only: bool = False) -> str:
    """构造 ORDER BY 片段。

    active_only=True 表示调用方已用 `status = 'active'` 约束了结果集，此时
    排序里的 `CASE WHEN status = 'active' THEN 0 ELSE 1 END` 恒等于 0，去掉它
    可以让 SQLite 复用索引顺序、少建一次临时 B 树（实测 15.3ms -> 10.6ms）。
    """
    column = SORT_COLUMN_MAP.get(sort_by, SORT_COLUMN_MAP["crawl_time"])
    direction = "ASC" if sort_order == "asc" else "DESC"
    if active_only:
        return f"{column} {direction}, id {direction}"
    return f"(CASE WHEN status = 'active' THEN 0 ELSE 1 END), {column} {direction}, id {direction}"


def _load_blacklist_keywords_from_conn(conn, filename: str) -> list[str]:
    row = conn.execute(
        """
        SELECT blacklist_keywords_json
        FROM result_blacklist_rules
        WHERE result_filename = ?
        """,
        (filename,),
    ).fetchone()
    if row is None:
        return []
    try:
        payload = json.loads(row["blacklist_keywords_json"] or "[]")
    except json.JSONDecodeError:
        return []
    return normalize_blacklist_keywords(payload)


def _decorate_record_visibility(record: dict, status: str | None, blacklist_keywords: list[str]) -> dict:
    matched_keywords = match_blacklist_keywords(record, blacklist_keywords)
    hidden_reason = None
    if status == "expired":
        hidden_reason = "expired"
    elif status and status != "active":
        hidden_reason = "manual"
    elif matched_keywords:
        hidden_reason = "rule"

    record["_status"] = status or "active"
    record["_matched_blacklist_keywords"] = matched_keywords
    record["_hidden_reason"] = hidden_reason
    record["_effective_hidden"] = hidden_reason is not None
    return record


def _is_record_visible(record: dict) -> bool:
    return record.get("_effective_hidden") is not True


def _load_filtered_records_from_conn(
    conn,
    *,
    filename: str,
    ai_recommended_only: bool,
    keyword_recommended_only: bool,
    sort_by: str,
    sort_order: str,
    include_hidden: bool,
) -> list[dict]:
    where_clause, params = _build_query_conditions(
        filename=filename,
        ai_recommended_only=ai_recommended_only,
        keyword_recommended_only=keyword_recommended_only,
    )
    order_clause = _sort_expression(sort_by, sort_order)
    rows = conn.execute(
        f"""
        SELECT raw_json, status
        FROM result_items
        WHERE {where_clause}
        ORDER BY {order_clause}
        """,
        tuple(params),
    ).fetchall()
    blacklist_keywords = _load_blacklist_keywords_from_conn(conn, filename)

    records: list[dict] = []
    for row in rows:
        record = _parse_raw_record(str(row["raw_json"]), status=row["status"])
        decorated = _decorate_record_visibility(record, row["status"], blacklist_keywords)
        if include_hidden or _is_record_visible(decorated):
            records.append(decorated)
    return records


async def save_result_record(record: dict, keyword: str) -> bool:
    return await asyncio.to_thread(_save_result_record_sync, record, keyword)


def _save_result_record_sync(record: dict, keyword: str) -> bool:
    bootstrap_sqlite_storage()
    item = record.get("商品信息", {}) or {}
    analysis = record.get("ai_analysis", {}) or {}
    link = str(item.get("商品链接") or "")
    link_unique_key = _get_link_unique_key(link) if link else _fallback_unique_key(record, item)
    keyword_hit_count = analysis.get("keyword_hit_count", 0)
    try:
        keyword_hit_count = int(keyword_hit_count)
    except (TypeError, ValueError):
        keyword_hit_count = 0

    with sqlite_connection() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO result_items (
                result_filename, keyword, task_name, crawl_time, publish_time, price,
                price_display, item_id, title, link, link_unique_key, seller_nickname,
                is_recommended, analysis_source, keyword_hit_count, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                build_result_filename(keyword),
                record.get("搜索关键字", keyword),
                record.get("任务名称", ""),
                record.get("爬取时间", ""),
                item.get("发布时间"),
                parse_price_value(item.get("当前售价")),
                item.get("当前售价"),
                item.get("商品ID"),
                item.get("商品标题"),
                link,
                link_unique_key,
                (record.get("卖家信息", {}) or {}).get("卖家昵称") or item.get("卖家昵称"),
                1 if analysis.get("is_recommended") else 0,
                analysis.get("analysis_source"),
                keyword_hit_count,
                json.dumps(record, ensure_ascii=False),
            ),
        )
        conn.commit()
    return True


def load_processed_link_keys(keyword: str) -> set[str]:
    bootstrap_sqlite_storage()
    filename = build_result_filename(keyword)
    with sqlite_connection() as conn:
        rows = conn.execute(
            "SELECT link_unique_key FROM result_items WHERE result_filename = ?",
            (filename,),
        ).fetchall()
    return {str(row["link_unique_key"]) for row in rows if row["link_unique_key"]}


async def list_result_filenames() -> list[str]:
    return await asyncio.to_thread(_list_result_filenames_sync)


def _list_result_filenames_sync() -> list[str]:
    bootstrap_sqlite_storage()
    with sqlite_connection() as conn:
        rows = conn.execute(
            """
            SELECT result_filename, MAX(crawl_time) AS latest_crawl_time
            FROM result_items
            GROUP BY result_filename
            ORDER BY latest_crawl_time DESC, result_filename DESC
            """
        ).fetchall()
    return [str(row["result_filename"]) for row in rows]


async def result_file_exists(filename: str) -> bool:
    return await asyncio.to_thread(_result_file_exists_sync, filename)


def _result_file_exists_sync(filename: str) -> bool:
    bootstrap_sqlite_storage()
    with sqlite_connection() as conn:
        row = conn.execute(
            "SELECT 1 FROM result_items WHERE result_filename = ? LIMIT 1",
            (filename,),
        ).fetchone()
    return row is not None


async def delete_result_file_records(filename: str) -> int:
    return await asyncio.to_thread(_delete_result_file_records_sync, filename)


def _delete_result_file_records_sync(filename: str) -> int:
    bootstrap_sqlite_storage()
    with sqlite_connection() as conn:
        cursor = conn.execute(
            "DELETE FROM result_items WHERE result_filename = ?",
            (filename,),
        )
        conn.commit()
    return int(cursor.rowcount or 0)


async def query_result_records(
    filename: str,
    *,
    ai_recommended_only: bool,
    keyword_recommended_only: bool,
    sort_by: str,
    sort_order: str,
    page: int,
    limit: int,
    include_hidden: bool = False,
) -> tuple[int, list[dict]]:
    return await asyncio.to_thread(
        _query_result_records_sync,
        filename,
        ai_recommended_only,
        keyword_recommended_only,
        sort_by,
        sort_order,
        page,
        limit,
        include_hidden,
    )


def _query_result_records_sync(
    filename: str,
    ai_recommended_only: bool,
    keyword_recommended_only: bool,
    sort_by: str,
    sort_order: str,
    page: int,
    limit: int,
    include_hidden: bool,
) -> tuple[int, list[dict]]:
    """分页查询结果记录。

    性能要点：旧实现先 `_load_filtered_records_from_conn` 取回该文件的**全部**记录
    （逐条 json.loads + 黑名单匹配），再在 Python 里切片分页。NAS 实测
    macbook_air_M1（2136 行）单次 1336ms，而真正返回的只有 100 条。

    现在按「是否需要黑名单过滤」分两条路径：

    - **快路径**（无黑名单规则，或 include_hidden=True）：
      黑名单为空时不存在「被规则隐藏」的记录，因此 `status` 过滤就是全部可见性
      语义。此时 `COUNT(*)` + `LIMIT/OFFSET` 与旧实现结果**完全等价**，但只解析
      当页记录。实测 1336ms -> 约 50ms。
    - **慢路径**（存在黑名单规则且 include_hidden=False）：
      规则命中与否必须解析 raw_json 才能判定，无法用 SQL 表达（规则支持 `re:`
      正则）。为了让 `total` 与分页都保持精确，仍走全量扫描。
      实测 Kindle 文件（213 行 / 规则 `["越狱"]`，隐藏 59 条）约 193ms，可接受。
      若将来给大文件加规则，这里会退化回全量扫描——届时再考虑落库 search_text 列。
    """
    bootstrap_sqlite_storage()
    offset = max(page - 1, 0) * limit
    where_clause, params = _build_query_conditions(
        filename=filename,
        ai_recommended_only=ai_recommended_only,
        keyword_recommended_only=keyword_recommended_only,
    )
    if not include_hidden:
        where_clause += " AND status = 'active'"
    order_clause = _sort_expression(
        sort_by, sort_order, active_only=not include_hidden
    )
    with sqlite_connection() as conn:
        blacklist_keywords = _load_blacklist_keywords_from_conn(conn, filename)

        if include_hidden or not blacklist_keywords:
            total_row = conn.execute(
                f"SELECT COUNT(*) AS total FROM result_items WHERE {where_clause}",
                tuple(params),
            ).fetchone()
            total = int(total_row["total"])
            rows = conn.execute(
                f"""
                SELECT raw_json, status
                FROM result_items
                WHERE {where_clause}
                ORDER BY {order_clause}
                LIMIT ? OFFSET ?
                """,
                tuple(params) + (limit, offset),
            ).fetchall()
            records: list[dict] = []
            for row in rows:
                record = _parse_raw_record(str(row["raw_json"]), status=row["status"])
                records.append(
                    _decorate_record_visibility(record, row["status"], blacklist_keywords)
                )
            return total, records

        records = _load_filtered_records_from_conn(
            conn,
            filename=filename,
            ai_recommended_only=ai_recommended_only,
            keyword_recommended_only=keyword_recommended_only,
            sort_by=sort_by,
            sort_order=sort_order,
            include_hidden=include_hidden,
        )
    return len(records), records[offset: offset + limit]


async def load_all_result_records(
    filename: str,
    *,
    ai_recommended_only: bool,
    keyword_recommended_only: bool,
    sort_by: str,
    sort_order: str,
    include_hidden: bool = False,
) -> list[dict]:
    return await asyncio.to_thread(
        _load_all_result_records_sync,
        filename,
        ai_recommended_only,
        keyword_recommended_only,
        sort_by,
        sort_order,
        include_hidden,
    )


def _load_all_result_records_sync(
    filename: str,
    ai_recommended_only: bool,
    keyword_recommended_only: bool,
    sort_by: str,
    sort_order: str,
    include_hidden: bool,
) -> list[dict]:
    bootstrap_sqlite_storage()
    with sqlite_connection() as conn:
        return _load_filtered_records_from_conn(
            conn,
            filename=filename,
            ai_recommended_only=ai_recommended_only,
            keyword_recommended_only=keyword_recommended_only,
            sort_by=sort_by,
            sort_order=sort_order,
            include_hidden=include_hidden,
        )


async def build_result_ndjson(filename: str) -> str:
    return await asyncio.to_thread(_build_result_ndjson_sync, filename)


def _build_result_ndjson_sync(filename: str) -> str:
    bootstrap_sqlite_storage()
    with sqlite_connection() as conn:
        rows = conn.execute(
            "SELECT raw_json FROM result_items WHERE result_filename = ? ORDER BY id ASC",
            (filename,),
        ).fetchall()
    return "\n".join(str(row["raw_json"]) for row in rows)


async def load_result_summary(filename: str) -> dict | None:
    return await asyncio.to_thread(_load_result_summary_sync, filename)


def _summarize_visible_records(visible_records: list[dict]) -> dict | None:
    """把「已按可见性过滤好的记录列表」汇总成统计指标。

    这是 dashboard 统计的**精确**语义定义：只统计未被规则/手动/过期隐藏的记录。
    `_load_result_summary_sync` 与 `_aggregate_result_file_stats_sync` 的黑名单分支
    共用它，保证两条路径口径一致。
    """
    if not visible_records:
        return None

    recommended_records = [
        record
        for record in visible_records
        if (record.get("ai_analysis", {}) or {}).get("is_recommended") is True
    ]
    ai_recommended_items = 0
    keyword_recommended_items = 0
    for record in recommended_records:
        source = (record.get("ai_analysis", {}) or {}).get("analysis_source")
        if source == "ai":
            ai_recommended_items += 1
        elif source == "keyword":
            keyword_recommended_items += 1

    return {
        "total_items": len(visible_records),
        "recommended_items": len(recommended_records),
        "ai_recommended_items": ai_recommended_items,
        "keyword_recommended_items": keyword_recommended_items,
        "latest_crawl_time": visible_records[0].get("爬取时间"),
        "latest_record": visible_records[0],
        "latest_recommendation": recommended_records[0] if recommended_records else None,
    }


def _load_result_summary_sync(filename: str) -> dict | None:
    bootstrap_sqlite_storage()
    with sqlite_connection() as conn:
        visible_records = _load_filtered_records_from_conn(
            conn,
            filename=filename,
            ai_recommended_only=False,
            keyword_recommended_only=False,
            sort_by="crawl_time",
            sort_order="desc",
            include_hidden=False,
        )
    return _summarize_visible_records(visible_records)


async def aggregate_result_file_stats(filename: str) -> dict | None:
    """轻量聚合单个结果文件的统计指标，避免加载全部 raw_json。

    返回 total_items / recommended_items / ai_recommended_items /
    keyword_recommended_items / latest_crawl_time / latest_record /
    latest_recommendation。**口径与 `load_result_summary` 完全一致**
    （都只统计未被黑名单规则/手动/过期隐藏的记录），只是换了一条更快的实现路径。

    性能要点：dashboard 每次刷新都会遍历全部结果文件，旧的 `load_result_summary`
    对每个文件都逐条 json.loads 全量记录，是仪表盘慢的主因。这里改为：

    - **无黑名单规则**（绝大多数文件）：纯 SQL 聚合，不解析任何 raw_json。
    - **有黑名单规则**：规则命中与否无法用 SQL 表达（规则支持 `re:` 正则），
      退回 `_summarize_visible_records` 精确路径，保证计数口径不变。
    """
    return await asyncio.to_thread(_aggregate_result_file_stats_sync, filename)


def _aggregate_result_file_stats_sync(filename: str) -> dict | None:
    bootstrap_sqlite_storage()
    with sqlite_connection() as conn:
        if _load_blacklist_keywords_from_conn(conn, filename):
            # 有规则：必须逐条判定命中，退回精确实现。
            return _summarize_visible_records(
                _load_filtered_records_from_conn(
                    conn,
                    filename=filename,
                    ai_recommended_only=False,
                    keyword_recommended_only=False,
                    sort_by="crawl_time",
                    sort_order="desc",
                    include_hidden=False,
                )
            )

        stats = conn.execute(
            """
            SELECT
                COUNT(*) AS total_items,
                SUM(CASE WHEN is_recommended = 1 AND analysis_source = 'ai' THEN 1 ELSE 0 END) AS ai_recommended_items,
                SUM(CASE WHEN is_recommended = 1 AND analysis_source = 'keyword' THEN 1 ELSE 0 END) AS keyword_recommended_items,
                SUM(is_recommended) AS recommended_items,
                MAX(crawl_time) AS latest_crawl_time
            FROM result_items
            WHERE result_filename = ? AND status = 'active'
            """,
            (filename,),
        ).fetchone()
        if not stats or int(stats["total_items"]) == 0:
            return None
        latest_row = conn.execute(
            """
            SELECT raw_json FROM result_items
            WHERE result_filename = ? AND status = 'active'
            ORDER BY crawl_time DESC, id DESC
            LIMIT 1
            """,
            (filename,),
        ).fetchone()
        latest_recommendation_row = conn.execute(
            """
            SELECT raw_json FROM result_items
            WHERE result_filename = ? AND status = 'active' AND is_recommended = 1
            ORDER BY crawl_time DESC, id DESC
            LIMIT 1
            """,
            (filename,),
        ).fetchone()
    return {
        "total_items": int(stats["total_items"]),
        "recommended_items": int(stats["recommended_items"] or 0),
        "ai_recommended_items": int(stats["ai_recommended_items"] or 0),
        "keyword_recommended_items": int(stats["keyword_recommended_items"] or 0),
        "latest_crawl_time": stats["latest_crawl_time"],
        # 与参考实现（_load_filtered_records_from_conn）保持一致：记录必须带上
        # _status / _matched_blacklist_keywords / _hidden_reason / _effective_hidden。
        # 否则前端 ResultCard 读不到这些字段，渲染行为会与结果页不一致。
        # 本分支已确认无黑名单规则且只取 status='active'，故用 ('active', []) 装饰即可。
        "latest_record": (
            _decorate_record_visibility(
                _parse_raw_record(str(latest_row["raw_json"])), "active", []
            )
            if latest_row
            else None
        ),
        "latest_recommendation": (
            _decorate_record_visibility(
                _parse_raw_record(str(latest_recommendation_row["raw_json"])), "active", []
            )
            if latest_recommendation_row
            else None
        ),
    }


async def update_item_status(filename: str, item_id: str, status: str) -> bool:
    valid = {"active", "hidden", "expired"}
    if status not in valid:
        raise ValueError(f"status must be one of {valid}")
    return await asyncio.to_thread(_update_item_status_sync, filename, item_id, status)


def _update_item_status_sync(filename: str, item_id: str, status: str) -> bool:
    bootstrap_sqlite_storage()
    with sqlite_connection() as conn:
        cursor = conn.execute(
            "UPDATE result_items SET status = ? WHERE result_filename = ? AND item_id = ?",
            (status, filename, item_id),
        )
        conn.commit()
        return cursor.rowcount > 0


async def load_result_blacklist_keywords(filename: str) -> list[str]:
    return await asyncio.to_thread(_load_result_blacklist_keywords_sync, filename)


def _load_result_blacklist_keywords_sync(filename: str) -> list[str]:
    bootstrap_sqlite_storage()
    with sqlite_connection() as conn:
        return _load_blacklist_keywords_from_conn(conn, filename)


async def save_result_blacklist_keywords(filename: str, keywords: list[str]) -> list[str]:
    return await asyncio.to_thread(_save_result_blacklist_keywords_sync, filename, keywords)


def _save_result_blacklist_keywords_sync(filename: str, keywords: list[str]) -> list[str]:
    bootstrap_sqlite_storage()
    normalized_keywords = normalize_blacklist_keywords(keywords)
    now = datetime.now().isoformat()
    with sqlite_connection() as conn:
        conn.execute(
            """
            INSERT INTO result_blacklist_rules (
                result_filename, blacklist_keywords_json, updated_at
            ) VALUES (?, ?, ?)
            ON CONFLICT(result_filename) DO UPDATE SET
                blacklist_keywords_json = excluded.blacklist_keywords_json,
                updated_at = excluded.updated_at
            """,
            (filename, json.dumps(normalized_keywords, ensure_ascii=False), now),
        )
        conn.commit()
    return normalized_keywords


def load_visible_result_item_ids(filename: str) -> set[str]:
    """取「可见」记录的 item_id 集合，用于价格走势上下文。

    性能要点：旧实现调用 `_load_filtered_records_from_conn`，会把该文件**全部**记录
    取回并逐条 json.loads + 跑黑名单规则，只为拿到 item_id。NAS 实测
    macbook_air_M1（2136 行）单次 1042ms。

    现在按是否配置黑名单规则分流：

    - **无规则**（绝大多数文件）：直接用 `SELECT item_id` 投影，实测 20ms，
      结果与旧实现完全一致——没有规则就不存在「被规则隐藏」的记录。
    - **有规则**：无法用 SQL 判定规则命中，保留逐条解析的精确路径
      （Kindle 文件 213 行，实测约 193ms）。
    """
    bootstrap_sqlite_storage()
    with sqlite_connection() as conn:
        if not _load_blacklist_keywords_from_conn(conn, filename):
            rows = conn.execute(
                "SELECT item_id FROM result_items WHERE result_filename = ? AND status = 'active'",
                (filename,),
            ).fetchall()
            item_ids: set[str] = set()
            for row in rows:
                item_id = str(row["item_id"] or "").strip()
                if item_id:
                    item_ids.add(item_id)
            return item_ids

        visible_records = _load_filtered_records_from_conn(
            conn,
            filename=filename,
            ai_recommended_only=False,
            keyword_recommended_only=False,
            sort_by="crawl_time",
            sort_order="desc",
            include_hidden=False,
        )
    item_ids: set[str] = set()
    for record in visible_records:
        product = record.get("商品信息", {}) or {}
        item_id = str(product.get("商品ID") or "").strip()
        if item_id:
            item_ids.add(item_id)
    return item_ids
