# Copyright (c) 2025, QCS
# licence information: see licence.txt

import frappe
from frappe.utils import flt
from collections import defaultdict

# ──────────────────────────────────────────────────────────────
# CONSTANTS
# ──────────────────────────────────────────────────────────────
EXCLUDE_WH = (
    " AND W.name NOT LIKE '%%RMA%%' "
    "AND W.name NOT LIKE '%%DEMO%%' "
)
SO_STATUS_FILTER = "so.status = 'To Deliver and Bill'"

# ──────────────────────────────────────────────────────────────
# REPORT ENTRY-POINT
# ──────────────────────────────────────────────────────────────
def execute(filters=None):
    filters = filters or {}
    return get_columns(), get_data(filters)

# ──────────────────────────────────────────────────────────────
# COLUMN DEFINITIONS
# ──────────────────────────────────────────────────────────────
def get_columns():
    return [
        # ── identification ──────────────────────────
        {"label": "Date & Time",        "fieldname": "transaction_date",              "fieldtype": "Datetime", "width": 150},
        {"label": "Company Name",       "fieldname": "company",                       "fieldtype": "Data",     "width": 130},
        {"label": "Sales Order No.",    "fieldname": "sales_order",                   "fieldtype": "Link",     "options": "Sales Order", "width": 120},
        {"label": "Sales Person",       "fieldname": "sales_person",                  "fieldtype": "Data",     "width": 120},
        {"label": "Country",            "fieldname": "country",                       "fieldtype": "Data",     "width": 100},
        {"label": "Customer Name",      "fieldname": "customer",                      "fieldtype": "Data",     "width": 150},
        {"label": "Brand",              "fieldname": "brand",                         "fieldtype": "Data",     "width": 120},
        {"label": "Part Number",        "fieldname": "part_number",                   "fieldtype": "Data",     "width": 120},
        {"label": "Item Code",          "fieldname": "item_code",                     "fieldtype": "Link",     "options": "Item", "width": 110},
        {"label": "Item Name",          "fieldname": "item_name",                     "fieldtype": "Data",     "width": 160},

        # ── demand / delivery ───────────────────────
        {"label": "Total Demanded Qty", "fieldname": "total_demanded_qty",            "fieldtype": "Float",   "width": 120},
        {"label": "Sales Order Qty",    "fieldname": "sales_order_qty",               "fieldtype": "Float",   "width": 110},
        {"label": "Delivered Qty",      "fieldname": "delivered_qty",                 "fieldtype": "Float",   "width": 110},
        {"label": "Balance Qty",        "fieldname": "balance_qty",                   "fieldtype": "Float",   "width": 110},

        # ── warehouse ───────────────────────────────
        {"label": "Total W/H Qty",              "fieldname": "total_wh_qty",              "fieldtype": "Float",   "width": 120},
        {"label": "Allocated Qty (FIFO)",       "fieldname": "allocated_qty",             "fieldtype": "Float",   "width": 130},
        {"label": "Balance to Allocate",        "fieldname": "balance_to_allocate",       "fieldtype": "Float",   "width": 130},
        {"label": "W/H Qty After Allocation",   "fieldname": "wh_qty_after_alloc",        "fieldtype": "Float",   "width": 150},

        # ── purchasing ─────────────────────────────
        {"label": "Total Ordered Qty",              "fieldname": "total_ordered_qty",            "fieldtype": "Float", "width": 130},
        {"label": "Ordered Qty Against SO",         "fieldname": "ordered_qty_against_so",       "fieldtype": "Float", "width": 130},
        {"label": "Ordered Open Qty Against SO",    "fieldname": "ordered_open_qty_against_so",  "fieldtype": "Float", "width": 150},
        {"label": "PO Date",                        "fieldname": "po_date",                      "fieldtype": "Date",  "width": 100},
        {"label": "PO Number",                      "fieldname": "po_number",                    "fieldtype": "Data",  "width": 120},

        # ── balances ───────────────────────────────
        {"label": "SO Ref Number",                  "fieldname": "so_ref_number",                "fieldtype": "Data",  "width": 120},
        {"label": "Balance to Order Against SO",    "fieldname": "balance_to_order_against_so",   "fieldtype": "Float","width": 170},
        {"label": "Total Balance to Order",         "fieldname": "total_balance_to_order",        "fieldtype": "Float","width": 150},
    ]

# ──────────────────────────────────────────────────────────────
# STOCK / BIN HELPERS (unchanged)
# ──────────────────────────────────────────────────────────────
def make_bin_aggregate(item_codes):
    rows = frappe.db.sql(
        f"""
        SELECT B.item_code, W.company,
               SUM(B.actual_qty)   AS wh_qty,
               SUM(B.reserved_qty) AS dem_qty,
               SUM(B.ordered_qty)  AS ord_qty
          FROM `tabBin` B
          JOIN `tabWarehouse` W ON W.name = B.warehouse
         WHERE B.item_code IN ({", ".join(["%s"]*len(item_codes))})
           {EXCLUDE_WH}
         GROUP BY B.item_code, W.company
        """, tuple(item_codes), as_dict=True
    )
    out = defaultdict(dict)
    for r in rows:
        out[r.item_code][r.company] = {
            "wh_qty":  flt(r.wh_qty),
            "dem_qty": flt(r.dem_qty),
            "ord_qty": flt(r.ord_qty),
        }
    return out

def make_fifo_map(item_codes):
    rows = frappe.db.sql(
        f"""
        SELECT  SLE.item_code, W.company,
                SLE.actual_qty, SLE.posting_date
          FROM `tabStock Ledger Entry` SLE
          JOIN `tabWarehouse` W ON W.name = SLE.warehouse
         WHERE SLE.item_code IN ({", ".join(["%s"]*len(item_codes))})
           AND SLE.actual_qty > 0
           {EXCLUDE_WH}
         ORDER BY W.company, SLE.item_code, SLE.posting_date
        """, tuple(item_codes), as_dict=True
    )
    fifo = defaultdict(lambda: defaultdict(list))
    for r in rows:
        fifo[r.item_code][r.company].append({"qty": flt(r.actual_qty)})
    return fifo

def clone_qty_map(src):
    dup = defaultdict(dict)
    for it, m in src.items():
        for co, qty in m.items():
            dup[it][co] = flt(qty)
    return dup

# ──────────────────────────────────────────────────────────────
# MAIN DATA
# ──────────────────────────────────────────────────────────────
def get_data(filters):

    # ---- Sales-Order items (capture soi.name for linking) --------------
    so_cond = [SO_STATUS_FILTER]
    if filters.get("company"):
        so_cond.append("so.company = %(company)s")
    if filters.get("from_date") and filters.get("to_date"):
        so_cond.append("so.transaction_date BETWEEN %(from_date)s AND %(to_date)s")
    if filters.get("item_code"):
        so_cond.append("soi.item_code = %(item_code)s")
    where_so = "WHERE " + " AND ".join(so_cond)

    sales_orders = frappe.db.sql(
        f"""
        SELECT  so.transaction_date, so.company,
                so.name  AS sales_order,
                (SELECT st.sales_person FROM `tabSales Team` st
                  WHERE st.parent = so.name LIMIT 1) AS sales_person,
                so.customer_name,
                (SELECT addr.country FROM `tabAddress` addr
                  WHERE addr.name = so.customer_address LIMIT 1) AS country,
                soi.name AS so_detail,                       -- link-id
                soi.item_code, soi.part_number,
                (SELECT item_name FROM `tabItem`
                  WHERE name = soi.item_code) AS item_name,
                soi.brand,
                soi.qty                       AS sales_order_qty,
                soi.delivered_qty,
                (soi.qty - soi.delivered_qty) AS balance_qty,
                soi.purchase_order            AS po_number,
                (SELECT po.transaction_date   FROM `tabPurchase Order` po
                  WHERE po.name = soi.purchase_order LIMIT 1) AS po_date
          FROM `tabSales Order` so
          JOIN `tabSales Order Item` soi ON soi.parent = so.name
        {where_so}
          AND so.docstatus = 1
        """, filters, as_dict=True
    )
    if not sales_orders:
        return []

    # ---- snapshots ----------------------------------------------------
    item_codes = list({r.item_code for r in sales_orders})
    bin_map    = make_bin_aggregate(item_codes)
    fifo_map   = make_fifo_map(item_codes)
    stock_left = clone_qty_map({it: {co: v["wh_qty"] for co, v in comp.items()}
                                for it, comp in bin_map.items()})

    # 1) Initialize PO related aggregates
    line_po_tot = defaultdict(float)
    line_po_open = defaultdict(float)
    fallback_po_tot = defaultdict(float)
    fallback_po_open = defaultdict(float)

    # Calculate PO totals for Sales Order Item
    so_detail_ids = [r.so_detail for r in sales_orders if r.so_detail]
    if so_detail_ids:
        rows = frappe.db.sql(
            f"""
            SELECT sales_order_item,
                   SUM(qty)                      AS tot,
                   SUM(qty - received_qty)       AS open
              FROM `tabPurchase Order Item` poi
              JOIN `tabPurchase Order` po ON po.name = poi.parent
             WHERE poi.sales_order_item IN ({", ".join(["%s"]*len(so_detail_ids))})
               AND po.docstatus = 1
             GROUP BY sales_order_item
            """, tuple(so_detail_ids), as_dict=True
        )
        for r in rows:
            line_po_tot[r.sales_order_item]  = flt(r.tot)
            line_po_open[r.sales_order_item] = flt(r.open)

    # 2) Grouped by (SO, item_code)
    so_names = [r.sales_order for r in sales_orders]
    if so_names:
        rows = frappe.db.sql(
            f"""
            SELECT poi.sales_order, poi.item_code,
                   SUM(poi.qty)                    AS tot,
                   SUM(poi.qty - poi.received_qty) AS open
              FROM `tabPurchase Order Item` poi
              JOIN `tabPurchase Order` po ON po.name = poi.parent
             WHERE poi.sales_order IN ({", ".join(["%s"]*len(so_names))})
               AND po.docstatus = 1
             GROUP BY poi.sales_order, poi.item_code
            """, tuple(so_names), as_dict=True
        )
        for r in rows:
            key = (r.sales_order, r.item_code)
            fallback_po_tot[key]  += flt(r.tot)
            fallback_po_open[key] += flt(r.open)

    # total SO-qty per (SO,item) for proportional split
    so_item_qty_sum = defaultdict(float)
    for row in sales_orders:
        if not row.so_detail:
            so_item_qty_sum[(row.sales_order, row.item_code)] += row.sales_order_qty

    # ------------------------------------------------------------------
    # BUILD REPORT ROWS
    # ------------------------------------------------------------------
    data = []

    for so in sales_orders:
        comp, item = so.company, so.item_code
        bins = bin_map.get(item, {}).get(comp, {})
        wh_qty_company = bins.get("wh_qty", 0)
        total_demand   = bins.get("dem_qty", 0)
        total_ordered  = bins.get("ord_qty", 0)

        # FIFO allocation (capped)
        available = stock_left[item].get(comp, 0)
        alloc = 0
        if available > 0:
            need = so.balance_qty
            for lot in fifo_map[item].get(comp, []):
                if need <= 0 or alloc >= available:
                    break
                take = min(lot["qty"], need, available - alloc)
                alloc += take
                need  -= take
                lot["qty"] -= take

        stock_left[item][comp] = max(available - alloc, 0)
        wh_after_alloc = stock_left[item][comp]
        balance_to_allocate = so.balance_qty - alloc

        # line-level PO figures
        po_num = None  # Initialize po_num here
        po_date = None  # Initialize po_date here
        if so.so_detail and so.so_detail in line_po_tot:
            ordered_qty_so  = line_po_tot[so.so_detail]
            ordered_open_so = line_po_tot[so.so_detail] - line_po_open.get(so.so_detail, 0)  # Correct calculation for open qty
        else:
            key = (so.sales_order, item)
            g_tot, g_open = fallback_po_tot.get(key, 0), fallback_po_open.get(key, 0)
            if g_tot:
                # Calculate proportional share of total ordered qty and open qty, checking for zero division
                if so_item_qty_sum[key] != 0:
                    share = so.sales_order_qty / so_item_qty_sum[key]
                else:
                    share = 0  # Default to 0 or handle accordingly
                ordered_qty_so = flt(g_tot * share)
                ordered_open_so = flt(g_open * share)
            else:
                ordered_qty_so = ordered_open_so = 0

        # Fetch PO number and PO date based on Sales Order reference
        if not po_num:
            hdr = frappe.db.sql(
                """
                SELECT po.name, po.transaction_date
                  FROM `tabPurchase Order` po
                  JOIN `tabPurchase Order Item` poi ON poi.parent = po.name
                 WHERE po.docstatus = 1
                   AND poi.sales_order = %s
                   AND poi.item_code = %s
                 ORDER BY po.transaction_date, po.creation
                 LIMIT 1
                """, (so.sales_order, item), as_dict=True
            )
            if hdr:
                po_num = hdr[0].name  # Correctly set PO number
                po_date = hdr[0].transaction_date  # Set PO date if PO number exists

        # balances
        balance_to_order_against_so = ordered_qty_so + wh_after_alloc - balance_to_allocate
        total_balance_to_order = total_ordered + wh_qty_company - so.balance_qty

        # row
        data.append({
            "transaction_date": so.transaction_date,
            "company":          comp,
            "sales_order":      so.sales_order,
            "sales_person":     so.sales_person,
            "customer":         so.customer_name,
            "country":          so.country,
            "brand":            so.brand,
            "part_number":      so.part_number,
            "item_code":        item,
            "item_name":        so.item_name,

            "total_demanded_qty": total_demand,
            "sales_order_qty":    so.sales_order_qty,
            "delivered_qty":      so.delivered_qty,
            "balance_qty":        so.balance_qty,

            "total_wh_qty":        wh_qty_company,
            "allocated_qty":       alloc,
            "balance_to_allocate": balance_to_allocate,
            "wh_qty_after_alloc":  wh_after_alloc,

            "total_ordered_qty":           total_ordered,
            "ordered_qty_against_so":      ordered_qty_so,
            "ordered_open_qty_against_so": ordered_open_so,
            "po_date":                     po_date,  # PO date fetched only if PO number exists
            "po_number":                   po_num,  # Correctly fetched PO number

            "so_ref_number":               so.sales_order,
            "balance_to_order_against_so": balance_to_order_against_so,
            "total_balance_to_order":      total_balance_to_order,
        })

    return data
