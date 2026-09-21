"""
结果记录富化与文件名校验服务
"""

from src.infrastructure.persistence.storage_names import normalize_keyword_from_filename
from src.services.price_history_service import (
    build_item_price_context,
    build_market_context,
    load_price_snapshots,
    parse_price_value,
)
from src.services.result_storage_service import load_visible_result_item_ids


def validate_result_filename(filename: str) -> None:
    if not filename.endswith(".jsonl") or "/" in filename or ".." in filename:
        raise ValueError("无效的文件名")


def enrich_records_with_price_insight(records: list[dict], filename: str) -> list[dict]:
    """为结果记录附加价格上下文。

    性能要点：同一次请求内只做一次全量扫描，而不是每条记录扫一遍。
      - item_index：一次性把快照按 item_id 分组，替代逐条的线性查找；
      - market_context：市场概览与具体商品无关，整批只计算一次。
    两处叠加把复杂度由 O(记录数 × 快照数) 降到 O(快照数)。
    （NAS 实测：20 条记录 + 16697 条快照，2.41s -> 约 0.05s）
    """
    snapshots = load_price_snapshots(normalize_keyword_from_filename(filename))
    if not snapshots:
        return records

    visible_item_ids = load_visible_result_item_ids(filename)
    visible_snapshots = [
        snapshot
        for snapshot in snapshots
        if str(snapshot.get("item_id") or "") in visible_item_ids
    ]

    item_index: dict[str, list[dict]] = {}
    for snapshot in snapshots:
        item_index.setdefault(str(snapshot.get("item_id") or ""), []).append(snapshot)

    market_context = build_market_context(visible_snapshots)

    enriched = []
    for record in records:
        info = record.get("商品信息", {}) or {}
        clone = dict(record)
        item_id = str(info.get("商品ID") or "")
        clone["price_insight"] = build_item_price_context(
            snapshots,
            item_id=item_id,
            current_price=parse_price_value(info.get("当前售价")),
            market_snapshots=visible_snapshots,
            item_snapshots=item_index.get(item_id, []),
            market_context=market_context,
        )
        enriched.append(clone)
    return enriched
