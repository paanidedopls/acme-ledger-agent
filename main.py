"""Acme Appliances ledger question-answering API for GA1.

Configure SOURCE_ROOT_URL in the deployment environment to the personalized
questionData root URL (the URL ending in questionId=q-ledger-agent-server&path=/).
The source dataset is loaded and cached at process startup.
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("acme-ledger")

EMAIL = "24f2001785@ds.study.iitm.ac.in".strip().lower()
IST = ZoneInfo("Asia/Kolkata")
SOURCE_ROOT_URL = os.getenv("SOURCE_ROOT_URL", "").strip()
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "1800"))

app = FastAPI(title="Acme Appliances Ledger Agent", version="1.0.0")

DATA_LOCK = threading.Lock()
DATA: list[dict[str, Any]] = []
RATES: dict[str, Decimal] = {"USD": Decimal("1"), "EUR": Decimal("1.1"), "INR": Decimal("0.01215")}
PRODUCTS: list[str] = []
LAST_LOAD = 0.0
LOAD_ERROR: str | None = None


class QuestionRequest(BaseModel):
    question: str = Field(min_length=1, max_length=2000)


def _url_with_params(url: str, **updates: str) -> str:
    parts = urlsplit(url)
    params = dict(parse_qsl(parts.query, keep_blank_values=True))
    params.update({k: str(v) for k, v in updates.items()})
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(params), parts.fragment))


def _get_text(url: str, timeout: float = 10.0) -> str:
    headers = {"User-Agent": "AcmeLedgerAgent/1.0"}
    with httpx.Client(timeout=timeout, follow_redirects=True, headers=headers) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.text


def _extract_rows(text: str) -> list[dict[str, Any]]:
    """Accept JSON arrays/objects as well as a JSON-lines export."""
    text = text.strip()
    if not text:
        return []
    try:
        obj = json.loads(text)
        if isinstance(obj, list):
            return [x for x in obj if isinstance(x, dict) and x.get("id")]
        if isinstance(obj, dict):
            for key in ("orders", "rows", "data", "records"):
                value = obj.get(key)
                if isinstance(value, list):
                    return [x for x in value if isinstance(x, dict) and x.get("id")]
            if obj.get("id"):
                return [obj]
    except json.JSONDecodeError:
        pass

    rows: list[dict[str, Any]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
            if isinstance(item, dict) and item.get("id"):
                rows.append(item)
        except json.JSONDecodeError:
            continue
    return rows


def _load_via_pages(orders_url: str) -> list[dict[str, Any]]:
    """Fallback if /export isn't a usable newline-delimited export."""
    first_text = _get_text(_url_with_params(orders_url, page="1"), timeout=12)
    try:
        first = json.loads(first_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Orders endpoint did not return JSON") from exc
    first_rows = first.get("orders", []) if isinstance(first, dict) else []
    pages = int(first.get("pages", 1)) if isinstance(first, dict) else 1
    if not isinstance(first_rows, list):
        raise RuntimeError("Orders response is missing its orders array")
    rows = [x for x in first_rows if isinstance(x, dict) and x.get("id")]
    if pages <= 1:
        return rows

    def fetch_page(page: int) -> list[dict[str, Any]]:
        body = json.loads(_get_text(_url_with_params(orders_url, page=str(page)), timeout=12))
        vals = body.get("orders", []) if isinstance(body, dict) else []
        return [x for x in vals if isinstance(x, dict) and x.get("id")] if isinstance(vals, list) else []

    # Bounded parallelism makes the fallback practical even for many pages.
    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = [pool.submit(fetch_page, i) for i in range(2, pages + 1)]
        for future in as_completed(futures):
            rows.extend(future.result())
    return rows


def _parse_updated(value: Any) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=IST)
        return dt.astimezone(timezone.utc)
    except (ValueError, TypeError):
        return datetime.min.replace(tzinfo=timezone.utc)


def _current_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # Same order ID can appear multiple times; latest updated_at is current.
    by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        oid = str(row.get("id", ""))
        if not oid:
            continue
        previous = by_id.get(oid)
        if previous is None or _parse_updated(row.get("updated_at")) >= _parse_updated(previous.get("updated_at")):
            by_id[oid] = row
    return list(by_id.values())


def load_data(force: bool = False) -> None:
    global DATA, RATES, PRODUCTS, LAST_LOAD, LOAD_ERROR
    if not SOURCE_ROOT_URL:
        LOAD_ERROR = "SOURCE_ROOT_URL is not configured on the hosting service"
        return
    with DATA_LOCK:
        if not force and DATA and (time.time() - LAST_LOAD) < CACHE_TTL_SECONDS:
            return
        try:
            root_text = _get_text(SOURCE_ROOT_URL, timeout=12)
            root = json.loads(root_text)
            links = root.get("links", {}) if isinstance(root, dict) else {}
            orders_url = links.get("orders")
            export_url = links.get("export")
            rates_url = links.get("rates")
            if not orders_url or not export_url or not rates_url:
                raise RuntimeError("Source root must expose orders, rates, and export links")

            rate_obj = json.loads(_get_text(rates_url, timeout=12))
            rate_data = rate_obj.get("usd_per_unit", {}) if isinstance(rate_obj, dict) else {}
            rates = {str(k).upper(): Decimal(str(v)) for k, v in rate_data.items()}
            rates.setdefault("USD", Decimal("1"))

            export_text = _get_text(export_url, timeout=15)
            rows = _extract_rows(export_text)
            if len(rows) < 50:
                log.warning("Export returned only %d order rows; trying paginated API", len(rows))
                rows = _load_via_pages(orders_url)
            if not rows:
                raise RuntimeError("Could not load any order records from export or orders pages")

            current = _current_rows(rows)
            products = sorted({str(r.get("product", "")).strip() for r in current if r.get("product")}, key=len, reverse=True)
            DATA = current
            RATES = rates
            PRODUCTS = products
            LAST_LOAD = time.time()
            LOAD_ERROR = None
            log.info("Loaded %d rows (%d unique current orders), %d products", len(rows), len(current), len(products))
        except Exception as exc:  # Keep server alive so logs/health endpoint remain reachable.
            LOAD_ERROR = f"{type(exc).__name__}: {exc}"
            log.exception("Loading source data failed")


@app.on_event("startup")
def startup_load() -> None:
    load_data(force=True)


@app.get("/")
def health() -> dict[str, Any]:
    return {"service": "Acme Appliances Ledger Agent", "status": "ok" if DATA else "degraded", "orders_loaded": len(DATA), "last_loaded": int(LAST_LOAD) if LAST_LOAD else None}


@app.get("/health")
def health_alias() -> dict[str, Any]:
    return health()


def _date_local(row: dict[str, Any]) -> datetime | None:
    raw = row.get("created_at")
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=IST)
        return dt.astimezone(IST)
    except (ValueError, TypeError):
        return None


MONTHS = {
    "january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3,
    "april": 4, "apr": 4, "may": 5, "june": 6, "jun": 6, "july": 7, "jul": 7,
    "august": 8, "aug": 8, "september": 9, "sept": 9, "sep": 9, "october": 10,
    "oct": 10, "november": 11, "nov": 11, "december": 12, "dec": 12,
}
REGIONS = ["North", "South", "East", "West", "Central"]


def _month_filters(q: str) -> set[int] | None:
    ql = q.lower()
    for quarter, months in (("q1", {1, 2, 3}), ("q2", {4, 5, 6}), ("q3", {7, 8, 9}), ("q4", {10, 11, 12})):
        if re.search(rf"\b{quarter}\b|\b{quarter[0]}\s*{quarter[1]}\b", ql):
            return months
    found = set()
    # Avoid matching month abbreviations inside other words.
    for name, n in MONTHS.items():
        if re.search(rf"\b{re.escape(name)}\b", ql):
            found.add(n)
    return found or None


def _extract_date_range(q: str) -> tuple[datetime | None, datetime | None]:
    # ISO date range, interpreted in Asia/Kolkata, inclusive start/exclusive end.
    match = re.search(r"(20\d{2}-\d{2}-\d{2})\s*(?:to|through|until|and|-)\s*(20\d{2}-\d{2}-\d{2})", q, re.I)
    if not match:
        return None, None
    try:
        start = datetime.fromisoformat(match.group(1)).replace(tzinfo=IST)
        end_day = datetime.fromisoformat(match.group(2)).replace(tzinfo=IST)
        from datetime import timedelta
        return start, end_day + timedelta(days=1)
    except ValueError:
        return None, None


def _filters(q: str) -> dict[str, Any]:
    ql = q.lower()
    f: dict[str, Any] = {"year": None, "months": _month_filters(q), "region": None, "product": None, "customer": None, "start": None, "end": None}
    ym = re.search(r"\b(20\d{2})\b", ql)
    if ym:
        f["year"] = int(ym.group(1))
    for region in REGIONS:
        if re.search(rf"\b{region.lower()}\b", ql):
            f["region"] = region
            break
    f["customer"] = (re.search(r"\bC\d{3,5}\b", q, re.I).group(0).upper() if re.search(r"\bC\d{3,5}\b", q, re.I) else None)
    for product in PRODUCTS:
        if product and product.lower() in ql:
            f["product"] = product
            break
    f["start"], f["end"] = _extract_date_range(q)
    # An explicit date range takes priority over broad year/month tokens in the dates.
    if f["start"] and f["end"]:
        f["year"] = None
        f["months"] = None
    return f


def _usd(row: dict[str, Any]) -> Decimal:
    try:
        amount = Decimal(str(row.get("amount", 0)))
        currency = str(row.get("currency", "USD")).upper()
        rate = RATES.get(currency)
        if rate is None:
            rate = Decimal("1") if currency == "USD" else Decimal("0")
        return amount * rate
    except Exception:
        return Decimal("0")


def _money(value: Decimal) -> float:
    return float(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _selected(q: str) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    f = _filters(q)
    ql = q.lower()
    refund_mode = any(w in ql for w in ("refund", "refunded", "refunds", "returned", "returning"))
    rows = []
    for row in DATA:
        status = str(row.get("status", "")).lower()
        if refund_mode:
            if status != "refunded":
                continue
        elif "void" in ql and "avoid" not in ql:
            if status != "void":
                continue
        elif status != "paid":
            # Revenue calculations must only include paid orders. For explicit order-count
            # questions, include other statuses unless they ask for paid orders.
            if not any(w in ql for w in ("order", "orders", "transaction", "transactions", "status")):
                continue
            if "paid" in ql or "revenue" in ql or "sales" in ql or "spent" in ql or "spend" in ql:
                continue

        if f["region"] and str(row.get("region", "")).lower() != f["region"].lower():
            continue
        if f["product"] and str(row.get("product", "")).lower() != f["product"].lower():
            continue
        if f["customer"] and str(row.get("customer", "")).upper() != f["customer"]:
            continue
        dt = _date_local(row)
        if f["year"] and (dt is None or dt.year != f["year"]):
            continue
        if f["months"] and (dt is None or dt.month not in f["months"]):
            continue
        if f["start"] and (dt is None or dt < f["start"]):
            continue
        if f["end"] and (dt is None or dt >= f["end"]):
            continue
        rows.append(row)
    return rows, f, ("refunded" if refund_mode else "paid")


def _groups(rows: list[dict[str, Any]], key: str, metric: str = "revenue") -> dict[str, Decimal | int]:
    acc: dict[str, Decimal | int] = defaultdict(lambda: Decimal("0") if metric == "revenue" else 0)
    for r in rows:
        k = str(r.get(key, "Unknown"))
        if metric == "revenue":
            acc[k] = Decimal(acc[k]) + _usd(r)
        elif metric == "units":
            acc[k] = int(acc[k]) + int(r.get("qty", 0) or 0)
        elif metric == "count":
            acc[k] = int(acc[k]) + 1
    return dict(acc)


def _answer(question: str) -> Any:
    rows, filters, mode = _selected(question)
    q = question.lower()
    if not rows:
        # For a product/customer/region name that exists in data, zero is a valid answer.
        if any(k in q for k in ("revenue", "sales", "refund", "how many", "count", "total")):
            if "how many" in q or "count" in q or "number of" in q:
                return 0
            return 0.0

    is_refund = mode == "refunded"
    asks_count = bool(re.search(r"\bhow many\b|\bcount\b|\bnumber of\b|\bquantity of refunds\b", q))
    asks_avg = any(w in q for w in ("average", "avg.", "mean ", "per order"))
    asks_units = any(w in q for w in ("units", "quantity", "quantities", "items sold", "most sold", "top-selling", "top selling", "volume"))
    asks_orders = any(w in q for w in ("order", "orders", "transaction", "transactions"))
    asks_refund_frequency = any(w in q for w in ("most refunds", "most refunded", "most frequently", "most often", "frequency", "frequency of refund", "refund count"))
    asks_customers = "customer" in q or "customers" in q or filters["customer"] is not None
    asks_products = "product" in q or "products" in q or filters["product"] is not None
    asks_regions = "region" in q or "regions" in q
    ranking = any(w in q for w in ("highest", "largest", "most", "top", "best", "highest", "greatest", "maximum", "max ", "which product", "which customer", "which region", "who "))
    money_words = any(w in q for w in ("revenue", "sales", "spend", "spent", "amount", "value", "usd", "dollar", "money", "earn", "earned", "refund"))

    # Count / order questions.
    if asks_count and (is_refund or asks_orders or "transaction" in q):
        if "customer" in q and (any(w in q for w in ("unique", "distinct", "different")) or filters["customer"] is None):
            return len({str(r.get("customer")) for r in rows})
        if "refund" in q:
            return len(rows)
        if asks_orders:
            return len(rows)
        if "unit" in q or "quantity" in q:
            return sum(int(r.get("qty", 0) or 0) for r in rows)

    # Totals and averages.
    if asks_avg:
        if not rows:
            return 0.0
        if money_words:
            return _money(sum((_usd(r) for r in rows), Decimal("0")) / Decimal(len(rows)))
        return round(sum(int(r.get("qty", 0) or 0) for r in rows) / len(rows), 2)

    # Ranking / comparison, with explicit group dimension.
    if asks_products and filters["product"] is None and ranking:
        if is_refund and (asks_refund_frequency or asks_count):
            metric = "count"
        elif asks_units:
            metric = "units"
        elif asks_orders:
            metric = "count"
        else:
            metric = "revenue"
        groups = _groups(rows, "product", metric)
        if groups:
            return sorted(groups, key=lambda k: (-Decimal(groups[k]) if metric == "revenue" else -int(groups[k]), k))[0]
    if asks_customers and filters["customer"] is None and ranking:
        metric = "count" if asks_orders or (is_refund and (asks_refund_frequency or asks_count)) else ("units" if asks_units else "revenue")
        groups = _groups(rows, "customer", metric)
        if groups:
            return sorted(groups, key=lambda k: (-Decimal(groups[k]) if metric == "revenue" else -int(groups[k]), k))[0]
    if asks_regions and ranking:
        metric = "count" if asks_orders or (is_refund and (asks_refund_frequency or asks_count)) else "revenue"
        groups = _groups(rows, "region", metric)
        if groups:
            return sorted(groups, key=lambda k: (-Decimal(groups[k]) if metric == "revenue" else -int(groups[k]), k))[0]

    # A numeric total grouped by product/customer/region is understood from the filter.
    if asks_count:
        if "customer" in q and not filters["customer"]:
            return len({str(r.get("customer")) for r in rows})
        if asks_units:
            return sum(int(r.get("qty", 0) or 0) for r in rows)
        if asks_orders or "refund" in q:
            return len(rows)

    if asks_units and ("total" in q or asks_count or not money_words):
        return sum(int(r.get("qty", 0) or 0) for r in rows)

    # If asked about a specific customer's/product's orders, default to count only if
    # explicitly framed as number of orders; otherwise the default metric is USD total.
    if not money_words and asks_orders:
        return len(rows)

    total = sum((_usd(r) for r in rows), Decimal("0"))
    if "net" in q and is_refund:
        # Explicit refund queries report the refund value as a positive USD amount.
        return _money(total)
    return _money(total)


@app.post("/")
@app.post("/ask")
def ask(request: QuestionRequest) -> dict[str, Any]:
    if not DATA or (time.time() - LAST_LOAD) >= CACHE_TTL_SECONDS:
        load_data(force=True)
    if not DATA:
        raise HTTPException(status_code=503, detail=f"Ledger data is unavailable: {LOAD_ERROR or 'unknown error'}")
    try:
        answer = _answer(request.question.strip())
        return {"answer": answer}
    except Exception as exc:
        log.exception("Could not answer question")
        raise HTTPException(status_code=422, detail=f"Could not interpret question: {exc}") from exc


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
