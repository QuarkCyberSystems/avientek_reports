# Copyright (c) 2025, QCS and contributors
# For license information, please see license.txt

import frappe
from frappe import _
from frappe.utils import flt
from collections import defaultdict

# ===========================================================
#  Report entry‑point
# ===========================================================
def execute(filters=None):

    filters = filters or {}

    # 1) item master (allows brand / item_group / item_code filters)
    item_map = get_items(filters)
    if not item_map:
        return [], []

    item_codes = list(item_map.keys())

    # 2) raw Bin table (no warehouse filter)
    bin_rows = get_bin_rows(item_codes)
    if not bin_rows:
        return [], []

    # 3) aggregate those Bins by company
    bin_sum, companies = aggregate_bins_by_company(bin_rows)

    # 4) landed‑cost rate for every (item, company)
    lc_rate = get_landed_cost_rates(item_codes, companies)

    # 5) build columns & rows
    columns = build_columns(companies)
    data    = build_rows(item_map, bin_sum, lc_rate, companies)

    # 6) remove all‑zero rows (ignore unit_price)
    data = scrub_zero_rows(columns, data)

    return columns, data


# ===========================================================
#  Helpers
# ===========================================================

def get_items(flt_map):
    cond = [
        "I.disabled = 0",
        "I.is_stock_item = 1",
        "(I.end_of_life > CURDATE() OR I.end_of_life IS NULL OR I.end_of_life='0000-00-00')",
    ]
    if flt_map.get("item_code"):
        cond.append("I.item_code = %(item_code)s")
    if flt_map.get("brand"):
        cond.append("I.brand = %(brand)s")
    if flt_map.get("item_group"):
        cond.append("I.item_group = %(item_group)s")

    where = " AND ".join(cond)

    rows = frappe.db.sql(
        f"""
        SELECT
            I.name         AS item_code,
            I.item_group   AS brand_type,
            I.brand        AS brand_name,
            I.part_number,
            I.item_name    AS model,
            I.description
        FROM `tabItem` I
        WHERE {where}
        """,
        flt_map,
        as_dict=True,
    )

    item_codes = [r.item_code for r in rows] or [""]

    prices = frappe.db.sql(
        """
        SELECT item_code, price_list_rate
        FROM `tabItem Price`
        WHERE price_list = %s
          AND item_code IN ({})
        """.format(", ".join(["%s"] * len(item_codes))),
        tuple(["Distributer Pricing1"] + item_codes),
        as_dict=True,
    )
    price_map = {p.item_code: p.price_list_rate for p in prices}

    return {
        r.item_code: {**r, "price_list_rate": price_map.get(r.item_code, 0.0)}
        for r in rows
    }


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
        WHERE B.item_code IN ({ph})
        """,
        tuple(item_codes),
        as_dict=True,
    )


def aggregate_bins_by_company(bin_rows):
    wh_to_co = {}
    for r in bin_rows:
        wh_to_co.setdefault(
            r.warehouse, frappe.db.get_value("Warehouse", r.warehouse, "company") or ""
        )

    sums = defaultdict(lambda: defaultdict(lambda: {
        "actual_qty": 0.0,
        "ordered_qty": 0.0,
        "reserved_qty": 0.0,
        "indented_qty": 0.0,
        "val_qty": 0.0,
        "val_val": 0.0,
    }))

    for r in bin_rows:
        comp = wh_to_co[r.warehouse]
        s = sums[r.item_code][comp]

        s["actual_qty"]  += flt(r.actual_qty)
        s["ordered_qty"] += flt(r.ordered_qty)
        s["reserved_qty"]+= flt(r.reserved_qty)
        s["indented_qty"]+= flt(r.indented_qty)

        qty = flt(r.actual_qty)
        s["val_qty"] += qty
        s["val_val"] += qty * flt(r.valuation_rate)

    companies = sorted({c for _i, m in sums.items() for c in m})
    return sums, companies


def get_landed_cost_rates(item_codes, companies):
    rate = defaultdict(dict)
    for it in item_codes:
        for co in companies:
            rate[it][co] = 0.0

    stock = frappe.db.sql(
        """
        SELECT
            B.item_code,
            W.company,
            SUM(B.actual_qty)              AS qty,
            SUM(B.actual_qty*B.valuation_rate) AS val
        FROM `tabBin` B
        JOIN `tabWarehouse` W ON W.name = B.warehouse
        WHERE B.item_code IN ({})
          AND B.actual_qty   > 0
        GROUP BY B.item_code, W.company
        """.format(", ".join(["%s"] * len(item_codes))),
        tuple(item_codes),
        as_dict=True,
    )
    for r in stock:
        if r.qty:
            rate[r.item_code][r.company] = flt(r.val) / flt(r.qty)

    need = [(it, co) for it in item_codes for co in companies if rate[it][co] == 0]
    for it, co in need:
        rate[it][co] = fetch_last_po_average(it, co)

    return rate


def fetch_last_po_average(item_code, company):
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
        (company, item_code),
        as_dict=True,
    )
    if not latest:
        return 0.0

    po = latest[0].name
    avg = frappe.db.sql(
        """
        SELECT AVG(rate) AS avg_rate
        FROM `tabPurchase Order Item`
        WHERE parent = %s
          AND item_code = %s
        """,
        (po, item_code),
        as_dict=True,
    )
    return flt(avg[0].avg_rate) if avg else 0.0


def build_columns(companies):
    cols = [
        {"label": _("Brand Type"), "fieldname": "brand_type", "width": 120},
        {"label": _("Brand Name"), "fieldname": "brand_name", "width": 120},
        {"label": _("Item Code"),  "fieldname": "item_code",  "width": 120},
        {"label": _("Part Number"),"fieldname": "part_number","width": 120},
        {"label": _("Model"),      "fieldname": "model",      "width": 120},
        {"label": _("Description"),"fieldname": "description","width": 150},
        {
            "label": _("Unit Price (Avg Landed)"),
            "fieldname": "unit_price",
            "fieldtype": "Currency",
            "width": 100,
        },
    ]

    sub = [
        ("W/H Qty",   "wh_stock_qty",  "Float"),
        ("W/H $",     "wh_stock_val",  "Currency"),
        ("Ordered Qty","ordered_qty",   "Float"),
        ("Ordered $",  "ordered_val",   "Currency"),
        ("Demand Qty", "demand_qty",    "Float"),
        ("Demand $",   "demand_val",    "Currency"),
        ("Free Qty",   "free_qty",      "Float"),
        ("Free $",     "free_val",      "Currency"),
        ("Net Free Qty","net_free_qty", "Float"),
        ("Net Free $", "net_free_val",  "Currency"),
    ]

    for co in companies:
        for lbl, fn, ft in sub:
            cols.append(dict(label=f"{co} – {lbl}", fieldname=f"{co}_{fn}", fieldtype=ft, width=110))

    for lbl, fn, ft in sub:
        cols.append(dict(label=_("Total") + f" – {lbl}", fieldname=f"total_{fn}", fieldtype=ft, width=110))

    return cols


def build_rows(item_map, bin_sum, lc_rate, companies):

    rows = []
    for it, meta in item_map.items():

        rates = [lc_rate[it][c] for c in companies if lc_rate[it][c] > 0]
        unit  = sum(rates) / len(rates) if rates else 0

        row = {
            "brand_type":  meta["brand_type"],
            "brand_name":  meta["brand_name"],
            "item_code":   it,
            "part_number": meta["part_number"],
            "model":       meta["model"],
            "description": meta["description"],
            "unit_price":  unit,
        }

        tot = defaultdict(float)

        for co in companies:
            agg = bin_sum[it][co]
            act, ord_, res, ind = (
                agg["actual_qty"],
                agg["ordered_qty"],
                agg["reserved_qty"],
                agg["indented_qty"],
            )
            demand   = res + ind
            free     = act - res
            net_free = free + ord_ - demand
            rate     = lc_rate[it][co]

            money = lambda q, r=rate: q * r  # helper

            row[f"{co}_wh_stock_qty"]  = act
            row[f"{co}_wh_stock_val"]  = money(act)
            row[f"{co}_ordered_qty"]   = ord_
            row[f"{co}_ordered_val"]   = money(ord_)
            row[f"{co}_demand_qty"]    = demand
            row[f"{co}_demand_val"]    = money(demand)
            row[f"{co}_free_qty"]      = free
            row[f"{co}_free_val"]      = money(free)
            row[f"{co}_net_free_qty"]  = net_free
            row[f"{co}_net_free_val"]  = money(net_free)

            tot["wh_stock_qty"]  += act
            tot["wh_stock_val"]  += money(act)
            tot["ordered_qty"]   += ord_
            tot["ordered_val"]   += money(ord_)
            tot["demand_qty"]    += demand
            tot["demand_val"]    += money(demand)
            tot["free_qty"]      += free
            tot["free_val"]      += money(free)
            tot["net_free_qty"]  += net_free
            tot["net_free_val"]  += money(net_free)

        for k, v in tot.items():
            row[f"total_{k}"] = v

        rows.append(row)

    return rows


def scrub_zero_rows(columns, rows):
    # list every numeric field except unit_price
    numeric = [
        c["fieldname"]                         # <-- use [] not .
        for c in columns
        if c.get("fieldtype") in ("Float", "Currency", "Int")
        and c["fieldname"] != "unit_price"     # <-- and here
    ]

    return [
        r for r in rows
        if any(r.get(f) not in (0, None) for f in numeric)
    ]

