# Copyright (c) 2025, QCS
# Licence information: see licence.txt

import re
from collections import defaultdict
import frappe
from frappe.utils import flt


# ────────────────────────────────
# GLOBAL CONSTANTS / HELPERS
# ────────────────────────────────
EXCLUDE_WH = (
    " AND W.name NOT LIKE '%%RMA%%' "
    "AND W.name NOT LIKE '%%DEMO%%' "
)

_safe = lambda t: re.sub(r"[^A-Za-z0-9]+", "_", t).strip("_")   # make safe fieldnames

REPORT_CCY      = "USD"          # <—— desired display currency
REPORT_CCY_KEY  = "report_currency"   # helper field injected per-row


# ────────────────────────────────
# AED→USD factor  (live, fallback)
# ────────────────────────────────
def get_aed_to_usd_rate() -> float:
    row = frappe.db.sql(
        """
        SELECT exchange_rate
        FROM   `tabCurrency Exchange`
        WHERE  from_currency = 'USD' AND to_currency = 'AED'
        ORDER  BY date DESC, creation DESC
        LIMIT 1
        """,
        as_dict=True,
    )
    if row:
        return 1 / flt(row[0].exchange_rate or 3.673)
    return 1 / 3.673                     # hard-coded peg

AED_TO_USD = get_aed_to_usd_rate()


# ===========================================================
#  REPORT ENTRY-POINT
# ===========================================================
def execute(filters=None):
    filters = filters or {}

    item_map   = get_items(filters)
    if not item_map:
        return [], []
    item_codes = list(item_map)

    bin_rows   = get_bin_rows(item_codes)
    if not bin_rows:
        return [], []

    bin_sum, companies = aggregate_bins(bin_rows)
    lc_rate            = get_landed_cost(item_codes, companies)

    # convert landed-cost AED → USD once
    for it in lc_rate:
        for co in lc_rate[it]:
            lc_rate[it][co] *= AED_TO_USD

    columns = build_columns(companies)
    data    = build_rows(item_map, bin_sum, lc_rate, companies)
    data    = scrub_zero_rows(columns, data)

    return columns, data


# ===========================================================
#  ITEM MASTER  (optional filters)
# ===========================================================
def get_items(flt):
    cond = [
        "I.disabled = 0",
        "I.is_stock_item = 1",
        "(I.end_of_life > CURDATE() OR I.end_of_life IS NULL OR I.end_of_life='0000-00-00')",
    ]
    if flt.get("item_code"):
        cond.append("I.item_code = %(item_code)s")
    if flt.get("brand"):
        cond.append("I.brand = %(brand)s")
    if flt.get("item_group"):
        cond.append("I.item_group = %(item_group)s")

    rows = frappe.db.sql(
        f"""
        SELECT
            I.name       AS item_code,
            I.item_group AS brand_type,
            I.brand      AS brand_name,
            I.part_number,
            I.item_name  AS model,
            I.description
        FROM `tabItem` I
        WHERE {" AND ".join(cond)}
        """,
        flt,
        as_dict=True,
    )
    return {r.item_code: r for r in rows}


# ===========================================================
#  BIN rows (filtered)
# ===========================================================
def get_bin_rows(item_codes):
    ph = ", ".join(["%s"] * len(item_codes))
    return frappe.db.sql(
        f"""
        SELECT
            B.item_code, B.warehouse,
            B.actual_qty, B.ordered_qty,
            B.reserved_qty, B.indented_qty,
            B.valuation_rate
        FROM `tabBin` B
        JOIN `tabWarehouse` W ON W.name = B.warehouse
        WHERE B.item_code IN ({ph})
          {EXCLUDE_WH}
        """,
        tuple(item_codes),
        as_dict=True,
    )


# ===========================================================
#  Aggregate Bin → company
# ===========================================================
def aggregate_bins(rows):
    wh_company = {
        r.warehouse: frappe.db.get_value("Warehouse", r.warehouse, "company") or ""
        for r in rows
    }

    out = defaultdict(lambda: defaultdict(lambda: {
        "actual":   0.0,
        "ordered":  0.0,
        "reserved": 0.0,
        "indented": 0.0,
        "val_qty":  0.0,
        "val_val":  0.0,
    }))

    for r in rows:
        co  = wh_company[r.warehouse]
        agg = out[r.item_code][co]

        agg["actual"]   += flt(r.actual_qty)
        agg["ordered"]  += flt(r.ordered_qty)
        agg["reserved"] += flt(r.reserved_qty)
        agg["indented"] += flt(r.indented_qty)

        q = flt(r.actual_qty)
        agg["val_qty"] += q
        agg["val_val"] += q * flt(r.valuation_rate)

    return out, sorted({c for m in out.values() for c in m})


# ===========================================================
#  LANDED COST (AED)
# ===========================================================
def get_landed_cost(codes, companies):
    rate = defaultdict(lambda: defaultdict(float))

    stock = frappe.db.sql(
        f"""
        SELECT  B.item_code, W.company,
                SUM(B.actual_qty)                 AS qty,
                SUM(B.actual_qty*B.valuation_rate) AS val
        FROM `tabBin` B
        JOIN `tabWarehouse` W ON W.name = B.warehouse
        WHERE B.item_code IN ({", ".join(["%s"]*len(codes))})
          AND B.actual_qty > 0
          {EXCLUDE_WH}
        GROUP BY B.item_code, W.company
        """,
        tuple(codes),
        as_dict=True,
    )
    for r in stock:
        rate[r.item_code][r.company] = flt(r.val) / flt(r.qty)

    for it in codes:
        for co in companies:
            if rate[it][co]:
                continue
            po = frappe.db.sql(
                """
                SELECT PO.name
                FROM `tabPurchase Order` PO
                JOIN `tabPurchase Order Item` POI ON POI.parent = PO.name
                WHERE PO.docstatus = 1
                  AND PO.company   = %s
                  AND POI.item_code = %s
                ORDER BY PO.transaction_date DESC, PO.creation DESC
                LIMIT 1
                """,
                (co, it),
                as_dict=True,
            )
            if po:
                avg = frappe.db.sql(
                    """
                    SELECT AVG(rate) AS r
                    FROM `tabPurchase Order Item`
                    WHERE parent = %s AND item_code = %s
                    """,
                    (po[0].name, it),
                    as_dict=True,
                )
                rate[it][co] = flt(avg[0].r) if avg else 0.0
    return rate


# ===========================================================
#  Columns
# ===========================================================
def build_columns(companies):
    cols = [
        {"label": "Brand Type",  "fieldname": "brand_type", "width": 120},
        {"label": "Brand Name",  "fieldname": "brand_name", "width": 120},
        {"label": "Item Code",   "fieldname": "item_code",  "width": 120},
        {"label": "Part Number", "fieldname": "part_number","width": 120},
        {"label": "Model",       "fieldname": "model",      "width": 120},
        {"label": "Description", "fieldname": "description","width": 150},
        {
            "label": "Unit Price (Avg Landed)",
            "fieldname": "unit_price",
            "fieldtype": "Currency",
            "options": REPORT_CCY_KEY,
            "width": 110,
        },
    ]

    sub = [
        ("W/H Qty",            "wh_stock_qty",  "Float"),
        ("W/H Stock-$",        "wh_stock_val",  "Currency"),
        ("Ordered Qty",        "ordered_qty",   "Float"),
        ("Ordered Stock-$",    "ordered_val",   "Currency"),
        ("Demand Qty",         "demand_qty",    "Float"),
        ("Demanded-$",         "demand_val",    "Currency"),
        ("Free Qty",           "free_qty",      "Float"),
        ("Free Stock-$",       "free_val",      "Currency"),
        ("Net Free Qty",       "net_free_qty",  "Float"),
        ("Net Free Stock-$",   "net_free_val",  "Currency"),
    ]

    for co in companies:
        sk = _safe(co)

        cols.append({
            "label": f"{co} – Unit Price",
            "fieldname": f"{sk}_unit_price",
            "fieldtype": "Currency",
            "options": REPORT_CCY_KEY,
            "width": 110,
        })

        for lbl, fn, ft in sub:
            cols.append({
                "label": f"{co} – {lbl}",
                "fieldname": f"{sk}_{fn}",
                "fieldtype": ft,
                "options": REPORT_CCY_KEY if ft == "Currency" else "",
                "width": 110,
            })

    for lbl, fn, ft in sub:
        cols.append({
            "label": "Total – " + lbl,
            "fieldname": f"total_{fn}",
            "fieldtype": ft,
            "options": REPORT_CCY_KEY if ft == "Currency" else "",
            "width": 110,
        })

    return cols


# ===========================================================
#  Rows
# ===========================================================
def build_rows(item_map, bin_sum, lc_rate, companies):
    money = lambda q, r: q * r * AED_TO_USD     # AED → USD
    rows  = []

    for it, meta in item_map.items():
        nz  = [lc_rate[it][c] for c in companies if lc_rate[it][c] > 0]
        avg = sum(nz) / len(nz) if nz else 0.0

        row = {
            REPORT_CCY_KEY: REPORT_CCY,    # <— key every Currency col points to
            "brand_type":  meta.brand_type,
            "brand_name":  meta.brand_name,
            "item_code":   it,
            "part_number": meta.part_number,
            "model":       meta.model,
            "description": meta.description,
            "unit_price":  avg,
        }

        tot = defaultdict(float)

        for co in companies:
            sk   = _safe(co)
            rate = lc_rate[it][co]                 # already USD
            agg  = bin_sum[it][co]

            act, ord_, res, ind = (
                agg["actual"], agg["ordered"], agg["reserved"], agg["indented"]
            )

            demand   = res + ind
            free     = act - res
            net_free = free + ord_ - demand

            row[f"{sk}_unit_price"] = rate
            row[f"{sk}_wh_stock_qty"] = act
            row[f"{sk}_wh_stock_val"] = money(act,       rate)
            row[f"{sk}_ordered_qty"]  = ord_
            row[f"{sk}_ordered_val"]  = money(ord_,      rate)
            row[f"{sk}_demand_qty"]   = demand
            row[f"{sk}_demand_val"]   = money(demand,    rate)
            row[f"{sk}_free_qty"]     = free
            row[f"{sk}_free_val"]     = money(free,      rate)
            row[f"{sk}_net_free_qty"] = net_free
            row[f"{sk}_net_free_val"] = money(net_free,  rate)

            tot["wh_stock_qty"]  += act
            tot["wh_stock_val"]  += money(act,    rate)
            tot["ordered_qty"]   += ord_
            tot["ordered_val"]   += money(ord_,   rate)
            tot["demand_qty"]    += demand
            tot["demand_val"]    += money(demand, rate)
            tot["free_qty"]      += free
            tot["free_val"]      += money(free,   rate)
            tot["net_free_qty"]  += net_free
            tot["net_free_val"]  += money(net_free, rate)

        for k, v in tot.items():
            row[f"total_{k}"] = v

        rows.append(row)

    return rows


# ===========================================================
#  Remove all-zero rows
# ===========================================================
def scrub_zero_rows(columns, rows):
    numeric = [
        c["fieldname"]
        for c in columns
        if c.get("fieldtype") in ("Float", "Currency", "Int")
        and not (
            c["fieldname"] == "unit_price"
            or c["fieldname"].endswith("_unit_price")
        )
    ]
    return [r for r in rows if any(r.get(f) not in (0, None) for f in numeric)]
