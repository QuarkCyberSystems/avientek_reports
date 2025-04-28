# Copyright (c) 2025, QCS
# License information: see license.txt

import re
import frappe
from frappe.utils import flt
from collections import defaultdict

# ──────────────────────────────────────────────────────────────
#  CONSTANT : ignore any warehouse whose name contains RMA/DEMO
# ──────────────────────────────────────────────────────────────
EXCLUDE_WH = " AND W.name NOT LIKE '%%RMA%%' AND W.name NOT LIKE '%%DEMO%%' "

# helper: turn arbitrary text into a fieldname-safe key
def _safe_key(txt: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", txt).strip("_")


# ===========================================================
#  REPORT ENTRY-POINT
# ===========================================================
def execute(filters=None):
    filters = filters or {}

    # 1) Item master
    item_map = get_items(filters)
    if not item_map:
        return [], []
    item_codes = list(item_map.keys())

    # 2) Stock bins (RMA/DEMO filtered out)
    bin_rows = get_bin_rows(item_codes)
    if not bin_rows:
        return [], []

    # 3) Aggregate by company
    bin_sum, companies = aggregate_bins_by_company(bin_rows)

    # 4) Landed-cost rate (stock avg → latest PO fallback)
    lc_rate = get_landed_cost_rates(item_codes, companies)

    # 5) Columns & rows
    columns = build_columns(companies)
    data    = build_rows(item_map, bin_sum, lc_rate, companies)

    # 6) Remove rows that are all zero (ignore *_unit_price)
    data = scrub_zero_rows(columns, data)

    return columns, data


# ===========================================================
#  ITEM MASTER (with optional filters)
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
#  BIN ROWS  (RMA / DEMO excluded)
# ===========================================================
def get_bin_rows(item_codes):
    ph = ", ".join(["%s"] * len(item_codes))
    return frappe.db.sql(
        f"""
        SELECT
            B.item_code,
            B.warehouse,
            B.actual_qty,
            B.ordered_qty,
            B.reserved_qty,
            B.indented_qty,
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
#  AGGREGATE BINS  (item → company)
# ===========================================================
def aggregate_bins_by_company(bin_rows):
    wh_company = {
        r.warehouse: frappe.db.get_value("Warehouse", r.warehouse, "company") or ""
        for r in bin_rows
    }

    sums = defaultdict(lambda: defaultdict(lambda: {
        "actual_qty": 0.0,
        "ordered_qty": 0.0,
        "reserved_qty": 0.0,
        "indented_qty": 0.0,
        "val_qty": 0.0,
        "val_val": 0.0,
    }))

    for r in bin_rows:
        comp = wh_company[r.warehouse]
        s = sums[r.item_code][comp]

        s["actual_qty"]   += flt(r.actual_qty)
        s["ordered_qty"]  += flt(r.ordered_qty)
        s["reserved_qty"] += flt(r.reserved_qty)
        s["indented_qty"] += flt(r.indented_qty)

        q = flt(r.actual_qty)
        s["val_qty"] += q
        s["val_val"] += q * flt(r.valuation_rate)

    companies = sorted({c for m in sums.values() for c in m})
    return sums, companies


# ===========================================================
#  LANDED COST  (stock avg → latest PO)
# ===========================================================
def get_landed_cost_rates(item_codes, companies):
    rate = defaultdict(lambda: defaultdict(float))

    # Stock averages
    stock = frappe.db.sql(
        f"""
        SELECT
            B.item_code,
            W.company,
            SUM(B.actual_qty)               AS qty,
            SUM(B.actual_qty*B.valuation_rate) AS val
        FROM `tabBin` B
        JOIN `tabWarehouse` W ON W.name = B.warehouse
        WHERE B.item_code IN ({", ".join(["%s"]*len(item_codes))})
          AND B.actual_qty > 0
          {EXCLUDE_WH}
        GROUP BY B.item_code, W.company
        """,
        tuple(item_codes),
        as_dict=True,
    )
    for r in stock:
        rate[r.item_code][r.company] = flt(r.val) / flt(r.qty)

    # Fallback: latest submitted PO
    for it in item_codes:
        for co in companies:
            if rate[it][co]:
                continue
            latest = frappe.db.sql(
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
            if latest:
                avg = frappe.db.sql(
                    """
                    SELECT AVG(rate) AS r
                    FROM `tabPurchase Order Item`
                    WHERE parent = %s
                      AND item_code = %s
                    """,
                    (latest[0].name, it),
                    as_dict=True,
                )
                rate[it][co] = flt(avg[0].r) if avg else 0.0
    return rate


# ===========================================================
#  COLUMNS
# ===========================================================
def build_columns(companies):

    # Static details
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
            "width": 110,
        },
    ]

    # metrics that repeat per company
    sub = [
        ("W/H Qty",     "wh_stock_qty",  "Float"),
        ("W/H $",       "wh_stock_val",  "Currency"),
        ("Ordered Qty", "ordered_qty",   "Float"),
        ("Ordered $",   "ordered_val",   "Currency"),
        ("Demand Qty",  "demand_qty",    "Float"),
        ("Demand $",    "demand_val",    "Currency"),
        ("Free Qty",    "free_qty",      "Float"),
        ("Free $",      "free_val",      "Currency"),
        ("Net Free Qty","net_free_qty",  "Float"),
        ("Net Free $",  "net_free_val",  "Currency"),
    ]

    #  ✓  Unit-price column at the **start** of each company block
    for co in companies:
        sk = _safe_key(co)

        cols.append({
            "label": f"{co} – Unit Price",
            "fieldname": f"{sk}_unit_price",
            "fieldtype": "Currency",
            "width": 110,
        })

        for lbl, fn, ft in sub:
            cols.append({
                "label": f"{co} – {lbl}",
                "fieldname": f"{sk}_{fn}",
                "fieldtype": ft,
                "width": 110,
            })

    # Grand totals
    for lbl, fn, ft in sub:
        cols.append({
            "label": "Total – " + lbl,
            "fieldname": f"total_{fn}",
            "fieldtype": ft,
            "width": 110,
        })

    return cols


# ===========================================================
#  ROW DATA
# ===========================================================
def build_rows(item_map, bin_sum, lc_rate, companies):

    money = lambda q, r: q * r
    rows  = []

    for it, meta in item_map.items():

        # overall avg landed cost (simple mean of non-zero rates)
        nz = [lc_rate[it][c] for c in companies if lc_rate[it][c] > 0]
        overall = sum(nz) / len(nz) if nz else 0.0

        row = {
            "brand_type":  meta.brand_type,
            "brand_name":  meta.brand_name,
            "item_code":   it,
            "part_number": meta.part_number,
            "model":       meta.model,
            "description": meta.description,
            "unit_price":  overall,
        }

        tots = defaultdict(float)

        for co in companies:
            sk   = _safe_key(co)
            rate = lc_rate[it][co]
            agg  = bin_sum[it][co]

            act = agg["actual_qty"]
            ord_ = agg["ordered_qty"]
            res  = agg["reserved_qty"]
            ind  = agg["indented_qty"]

            demand   = res + ind
            free     = act - res
            net_free = free + ord_ - demand

            # per-company unit price
            row[f"{sk}_unit_price"] = rate

            # metrics
            row[f"{sk}_wh_stock_qty"] = act
            row[f"{sk}_wh_stock_val"] = money(act, rate)
            row[f"{sk}_ordered_qty"]  = ord_
            row[f"{sk}_ordered_val"]  = money(ord_, rate)
            row[f"{sk}_demand_qty"]   = demand
            row[f"{sk}_demand_val"]   = money(demand, rate)
            row[f"{sk}_free_qty"]     = free
            row[f"{sk}_free_val"]     = money(free, rate)
            row[f"{sk}_net_free_qty"] = net_free
            row[f"{sk}_net_free_val"] = money(net_free, rate)

            # accumulate totals
            tots["wh_stock_qty"]  += act
            tots["wh_stock_val"]  += money(act, rate)
            tots["ordered_qty"]   += ord_
            tots["ordered_val"]   += money(ord_, rate)
            tots["demand_qty"]    += demand
            tots["demand_val"]    += money(demand, rate)
            tots["free_qty"]      += free
            tots["free_val"]      += money(free, rate)
            tots["net_free_qty"]  += net_free
            tots["net_free_val"]  += money(net_free, rate)

        # write totals
        for k, v in tots.items():
            row[f"total_{k}"] = v

        rows.append(row)

    return rows


# ===========================================================
#  SCRUB ZERO ROWS  (ignore rate columns)
# ===========================================================
def scrub_zero_rows(columns, rows):
    numeric = [
        c["fieldname"]
        for c in columns
        if c.get("fieldtype") in ("Float", "Currency", "Int")
        and not (c["fieldname"] == "unit_price" or c["fieldname"].endswith("_unit_price"))
    ]
    return [r for r in rows if any(r.get(f) not in (0, None) for f in numeric)]
