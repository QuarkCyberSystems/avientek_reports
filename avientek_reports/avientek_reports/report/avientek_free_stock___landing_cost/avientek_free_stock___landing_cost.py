# Copyright (c) 2025, QCS
# Licence information: see licence.txt

import re
from collections import defaultdict
import frappe
from frappe.utils import flt

# ──────────────────────────────────────────────────────────────
# CONSTANTS & SETTINGS
# ──────────────────────────────────────────────────────────────
EXCLUDE_WH = (
    " AND W.name NOT LIKE '%%RMA%%' "
    "AND W.name NOT LIKE '%%DEMO%%' "
)

REPORT_CCY      = "USD"            # display currency
REPORT_CCY_KEY  = "report_currency"

# Fixed‑rate rules
INDIAN_CO            = "Avientek Electronics Trading PVT. LTD"
AED_TO_USD_FIXED     = 1 / 3.6725          # ≈ 0.272309
INR_TO_USD_FIXED     = 1 / 87.0            # ≈ 0.011494

# Helper to build safe fieldnames per‑company
_safe = lambda t: re.sub(r"[^A-Za-z0-9]+", "_", t).strip("_")


# ===========================================================
#  REPORT ENTRY‑POINT
# ===========================================================
def execute(filters=None):
    filters = filters or {}

    item_map = _get_items(filters)
    if not item_map:
        return [], []
    item_codes = list(item_map)

    bin_rows = _get_bin_rows(item_codes)
    if not bin_rows:
        return [], []

    bin_sum, companies = _aggregate_bins(bin_rows)
    lc_rate            = _get_landed_cost(item_codes, companies)      # stored **in AED**

    cols = _build_columns(companies)
    data = _build_rows(item_map, bin_sum, lc_rate, companies)
    data = _scrub_zero_rows(cols, data)

    return cols, data


# ===========================================================
#  ITEM MASTER  (optional filters)
# ===========================================================
def _get_items(flt):
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
#  BIN rows  (RMA / DEMO filtered)
# ===========================================================
def _get_bin_rows(item_codes):
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
#  AGGREGATE Bin rows  (item ▸ company)
# ===========================================================
def _aggregate_bins(rows):
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

    companies = sorted({c for m in out.values() for c in m})
    return out, companies


# ===========================================================
#  LANDED COST per company  (stored in AED)
# ===========================================================
def _get_landed_cost(codes, companies):
    """
    Return dict[item_code][company] = landed‑cost rate (in AED)
    """
    rate = defaultdict(lambda: defaultdict(float))

    # 1) weighted stock‑average
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

    # 2) fallback → latest submitted PO
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
#  COLUMN DEFINITIONS
# ===========================================================
def _build_columns(companies):
    cols = [
        {"label": "Brand Type",  "fieldname": "brand_type", "width": 120},
        {"label": "Brand Name",  "fieldname": "brand_name", "width": 120},
        {"label": "Item Code",   "fieldname": "item_code",  "width": 120},
        {"label": "Part Number", "fieldname": "part_number","width": 120},
        {"label": "Model",       "fieldname": "model",      "width": 120},
        {"label": "Description", "fieldname": "description","width": 150},
        {
            "label": "Unit Price (Avg Landed)",
            "fieldname": "unit_price",
            "fieldtype": "Currency",
            "options":  REPORT_CCY_KEY,
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
#  BUILD DATA ROWS
# ===========================================================
def _build_rows(item_map, bin_sum, lc_rate, companies):
    def money(qty: float, unit_rate: float, company: str) -> float:
        """
        Convert `qty × unit_rate` from the company’s base currency to USD
        using hard‑wired factors:
          • AED companies      → 3.6725
          • IN Avientek        → 87
        """
        fx = AED_TO_USD_FIXED if company != INDIAN_CO else INR_TO_USD_FIXED
        return qty * unit_rate * fx

    rows = []

    for it, meta in item_map.items():
        row = {
            REPORT_CCY_KEY: REPORT_CCY,
            "brand_type": meta.brand_type,
            "brand_name": meta.brand_name,
            "item_code": it,
            "part_number": meta.part_number,
            "model": meta.model,
            "description": meta.description,
            "unit_price": 0.0,  # calculated later
        }

        tot            = defaultdict(float)
        total_qty      = 0.0   # for overall avg unit price
        total_stockval = 0.0

        for co in companies:
            sk  = _safe(co)
            agg = bin_sum[it][co]

            act    = flt(agg["actual"])
            ord_   = flt(agg["ordered"])
            res    = flt(agg["reserved"])
            ind    = flt(agg["indented"])
            valqty = flt(agg["val_qty"])
            valval = flt(agg["val_val"])

            demand    = res + ind
            free      = act - res
            net_free  = free + ord_ - demand
            lc_aed    = lc_rate[it][co]
            unit_aed  = valval / valqty if valqty else 0.0

            # Company‑specific conversions
            row[f"{sk}_unit_price"]   = money(act, unit_aed, co) if valqty else 0.0
            row[f"{sk}_wh_stock_qty"] = act
            row[f"{sk}_wh_stock_val"] = money(act, unit_aed, co) if valqty else 0.0
            row[f"{sk}_ordered_qty"]  = ord_
            row[f"{sk}_ordered_val"]  = money(ord_, lc_aed, co)
            row[f"{sk}_demand_qty"]   = demand
            row[f"{sk}_demand_val"]   = money(demand, lc_aed, co)
            row[f"{sk}_free_qty"]     = free
            row[f"{sk}_free_val"]     = money(free, lc_aed, co)
            row[f"{sk}_net_free_qty"] = net_free
            row[f"{sk}_net_free_val"] = money(net_free, lc_aed, co)

            # Totals (already USD)
            tot["wh_stock_qty"] += act
            tot["wh_stock_val"] += row[f"{sk}_wh_stock_val"]
            tot["ordered_qty"]  += ord_
            tot["ordered_val"]  += row[f"{sk}_ordered_val"]
            tot["demand_qty"]   += demand
            tot["demand_val"]   += row[f"{sk}_demand_val"]
            tot["free_qty"]     += free
            tot["free_val"]     += row[f"{sk}_free_val"]
            tot["net_free_qty"] += net_free
            tot["net_free_val"] += row[f"{sk}_net_free_val"]

            total_qty      += act
            total_stockval += row[f"{sk}_wh_stock_val"]

        # average unit price across companies (weighted by actual stock)
        row["unit_price"] = (total_stockval / total_qty) if total_qty else 0.0

        for k, v in tot.items():
            row[f"total_{k}"] = v

        rows.append(row)

    return rows


# ===========================================================
#  DROP ALL‑ZERO ROWS
# ===========================================================
def _scrub_zero_rows(columns, rows):
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
