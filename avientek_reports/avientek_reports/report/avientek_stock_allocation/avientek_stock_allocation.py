# Copyright (c) 2025, QCS and contributors
# For license information, please see license.txt

import frappe
from frappe.utils import flt
from collections import defaultdict

# ===========================================================
#  Report entry-point
# ===========================================================
def execute(filters=None):
    filters = filters or {}
    columns = get_columns()
    data    = get_data(filters)
    return columns, data

# ===========================================================
#  Columns
# ===========================================================
def get_columns():
    return [
        {"label": "Date & Time",      "fieldname": "transaction_date",     "fieldtype": "Datetime", "width": 150},
        {"label": "Company Name",     "fieldname": "company",              "fieldtype": "Data",     "width": 130},
        {"label": "Sales Order No.",  "fieldname": "sales_order",          "fieldtype": "Link",     "options": "Sales Order", "width": 120},
        {"label": "Sales Person",     "fieldname": "sales_person",         "fieldtype": "Data",     "width": 120},
        {"label": "Country",          "fieldname": "country",              "fieldtype": "Data",     "width": 100},
        {"label": "Customer Name",    "fieldname": "customer",             "fieldtype": "Data",     "width": 150},
        {"label": "Brand",            "fieldname": "brand",                "fieldtype": "Data",     "width": 120},
        {"label": "Part Number",      "fieldname": "part_number",          "fieldtype": "Data",     "width": 120},
        {"label": "Item Code",        "fieldname": "item_code",            "fieldtype": "Link",     "options": "Item", "width": 110},
        {"label": "Item Name",        "fieldname": "item_name",            "fieldtype": "Data",     "width": 160},

        # --- demand / delivery status ---
        {"label": "Total Demanded Qty", "fieldname": "total_demanded_qty",    "fieldtype": "Float", "width": 120},
        {"label": "Sales Order Qty",    "fieldname": "sales_order_qty",       "fieldtype": "Float", "width": 110},
        {"label": "Delivered Qty",      "fieldname": "delivered_qty",         "fieldtype": "Float", "width": 110},
        {"label": "Balance Qty",        "fieldname": "balance_qty",           "fieldtype": "Float", "width": 110},

        # --- warehouse / allocation ---
        {"label": "Total W/H Qty",          "fieldname": "total_wh_qty",          "fieldtype": "Float", "width": 120},
        {"label": "Allocated Qty (FIFO)",   "fieldname": "allocated_qty",         "fieldtype": "Float", "width": 130},
        {"label": "Balance to Allocate",    "fieldname": "balance_to_allocate",   "fieldtype": "Float", "width": 130},
        {"label": "W/H Qty After Allocation","fieldname": "wh_qty_after_alloc",  "fieldtype": "Float", "width": 150},

        # --- purchase orders ---
        {"label": "Total Ordered Qty",          "fieldname": "total_ordered_qty",          "fieldtype": "Float", "width": 130},
        {"label": "Ordered Qty Against SO",     "fieldname": "ordered_qty_against_so",     "fieldtype": "Float", "width": 150},
        {"label": "PO Date",                    "fieldname": "po_date",                    "fieldtype": "Date",  "width": 100},
        {"label": "PO Number",                  "fieldname": "po_number",                  "fieldtype": "Data",  "width": 120},

        # --- misc ---
        {"label": "SO Ref Number",              "fieldname": "so_ref_number",              "fieldtype": "Data",  "width": 120},
        {"label": "Balance to Order Against SO","fieldname": "balance_to_order_against_so", "fieldtype": "Float", "width": 170},
        {"label": "Total Balance to Order",     "fieldname": "total_balance_to_order",      "fieldtype": "Float", "width": 150},
    ]

# ===========================================================
#  Helpers : pre-compute company-wise stock & PO data
# ===========================================================
EXCLUDE_WH = " AND W.name NOT LIKE '%%RMA%%' AND W.name NOT LIKE '%%DEMO%%' "

def make_bin_map(item_codes):
    """
    returns stock[item_code][company] = total_actual_qty (excl RMA/DEMO)
    """
    if not item_codes:
        return defaultdict(dict)

    rows = frappe.db.sql(
        f"""
        SELECT
            B.item_code,
            W.company,
            SUM(B.actual_qty) AS qty
        FROM `tabBin` B
        JOIN `tabWarehouse` W ON W.name = B.warehouse
        WHERE B.item_code IN ({", ".join(["%s"]*len(item_codes))})
          {EXCLUDE_WH}
        GROUP BY B.item_code, W.company
        """,
        tuple(item_codes),
        as_dict=True,
    )

    out = defaultdict(dict)
    for r in rows:
        out[r.item_code][r.company] = flt(r.qty)
    return out


def make_fifo_map(item_codes):
    """
    returns fifo[item_code][company] = list(lots in posting-date order)
    each lot: {"qty": actual_qty}
    """
    if not item_codes:
        return defaultdict(dict)

    rows = frappe.db.sql(
        f"""
        SELECT
            SLE.item_code,
            W.company,
            SLE.actual_qty,
            SLE.posting_date
        FROM `tabStock Ledger Entry` SLE
        JOIN `tabWarehouse` W ON W.name = SLE.warehouse
        WHERE SLE.item_code IN ({", ".join(["%s"]*len(item_codes))})
          AND SLE.actual_qty > 0
          {EXCLUDE_WH}
        ORDER BY W.company, SLE.item_code, SLE.posting_date ASC
        """,
        tuple(item_codes),
        as_dict=True,
    )

    fifo = defaultdict(lambda: defaultdict(list))
    for r in rows:
        fifo[r.item_code][r.company].append({"qty": flt(r.actual_qty)})
    return fifo


def make_po_pending_map(item_codes):
    """
    returns po_pending[item_code][company] = SUM(qty - received_qty) open
    """
    if not item_codes:
        return defaultdict(dict)

    rows = frappe.db.sql(
        f"""
        SELECT
            POI.item_code,
            PO.company,
            SUM(POI.qty - POI.received_qty) AS pend
        FROM `tabPurchase Order` PO
        JOIN `tabPurchase Order Item` POI ON POI.parent = PO.name
        WHERE POI.item_code IN ({", ".join(["%s"]*len(item_codes))})
          AND PO.docstatus = 1
          AND (POI.qty - POI.received_qty) > 0
        GROUP BY POI.item_code, PO.company
        """,
        tuple(item_codes),
        as_dict=True,
    )

    out = defaultdict(dict)
    for r in rows:
        out[r.item_code][r.company] = flt(r.pend)
    return out


# ===========================================================
#  Main data assembly
# ===========================================================
def get_data(filters):

    # --------------- dynamic SO filters
    so_cond = []
    if filters.get("company"):
        so_cond.append("so.company = %(company)s")
    if filters.get("from_date") and filters.get("to_date"):
        so_cond.append("so.transaction_date BETWEEN %(from_date)s AND %(to_date)s")
    if filters.get("item_code"):
        so_cond.append("soi.item_code = %(item_code)s")
    where = " AND ".join(so_cond)
    where = ("WHERE " + where) if where else ""

    # --------------- fetch Sales Orders (+ minimal item fields)
    sales_orders = frappe.db.sql(
        f"""
        SELECT
            so.transaction_date,
            so.company,
            so.name                       AS sales_order,
            (SELECT st.sales_person FROM `tabSales Team` st WHERE st.parent = so.name LIMIT 1) AS sales_person,
            so.customer_name,
            (SELECT addr.country FROM `tabAddress` addr WHERE addr.name = so.customer_address LIMIT 1) AS country,
            soi.item_code,
            soi.part_number,
            (SELECT item_name FROM `tabItem` WHERE name = soi.item_code) AS item_name,
            soi.brand,
            soi.qty                        AS sales_order_qty,
            soi.delivered_qty,
            (soi.qty - soi.delivered_qty)  AS balance_qty,
            soi.purchase_order             AS po_number,
            (SELECT po.transaction_date FROM `tabPurchase Order` po WHERE po.name = soi.purchase_order LIMIT 1) AS po_date
        FROM `tabSales Order` so
        JOIN `tabSales Order Item` soi ON soi.parent = so.name
        {where}
        AND so.docstatus = 1
        """,
        filters,
        as_dict=True,
    )

    if not sales_orders:
        return []

    # --------------- prepare helper maps
    item_codes = list({row.item_code for row in sales_orders})

    stock_map  = make_bin_map(item_codes)     # item>company -> wh_qty
    fifo_map   = make_fifo_map(item_codes)    # item>company -> lots
    po_map     = make_po_pending_map(item_codes)  # item>company -> pending PO qty

    # demanded qty per (item, company)
    demand_rows = frappe.db.sql(
        f"""
        SELECT
            soi.item_code,
            so.company,
            SUM(soi.qty) AS total_dem
        FROM `tabSales Order` so
        JOIN `tabSales Order Item` soi ON soi.parent = so.name
        WHERE soi.item_code IN ({", ".join(["%s"]*len(item_codes))})
          AND so.docstatus = 1
        GROUP BY soi.item_code, so.company
        """,
        tuple(item_codes),
        as_dict=True,
    )
    demand_map = defaultdict(dict)
    for r in demand_rows:
        demand_map[r.item_code][r.company] = flt(r.total_dem)

    # ordered qty against SO (pending) per SO line
    so_ordered_pending = {}
    if sales_orders:
        so_names = [row.sales_order for row in sales_orders]
        rows = frappe.db.sql(
            f"""
            SELECT
                poi.sales_order,
                poi.item_code,
                SUM(poi.qty - poi.received_qty) AS pend_so
            FROM `tabPurchase Order Item` poi
            JOIN `tabPurchase Order` po ON po.name = poi.parent
            WHERE poi.sales_order IN ({", ".join(["%s"]*len(so_names))})
              AND po.docstatus = 1
            GROUP BY poi.sales_order, poi.item_code
            """,
            tuple(so_names),
            as_dict=True,
        )
        for r in rows:
            so_ordered_pending[(r.sales_order, r.item_code)] = flt(r.pend_so)

    # --------------- build rows
    data = []

    for so in sales_orders:

        comp = so.company
        item = so.item_code

        wh_qty_company = stock_map[item].get(comp, 0)

        # ---------- FIFO allocation (company-scoped)
        lots = fifo_map[item].get(comp, [])
        needed = so.balance_qty
        alloc = 0
        for lot in lots:
            if needed <= 0:
                break
            take = min(lot["qty"], needed)
            alloc += take
            needed -= take
            lot["qty"] -= take

        # ---------- totals & balances
        total_demand = demand_map[item].get(comp, 0)
        total_ordered = po_map[item].get(comp, 0)
        ordered_against_so = so_ordered_pending.get((so.sales_order, item), 0)

        balance_to_allocate = so.balance_qty - alloc
        wh_after_alloc      = wh_qty_company - alloc
        balance_to_order_so = so.balance_qty - ordered_against_so
        total_balance_order = total_demand - alloc - total_ordered

        data.append(
            {
                "transaction_date":  so.transaction_date,
                "company":           comp,
                "sales_order":       so.sales_order,
                "sales_person":      so.sales_person,
                "customer":          so.customer_name,
                "country":           so.country,
                "brand":             so.brand,
                "part_number":       so.part_number,
                "item_code":         item,
                "item_name":         so.item_name,

                # demand / delivery
                "total_demanded_qty": total_demand,
                "sales_order_qty":    so.sales_order_qty,
                "delivered_qty":      so.delivered_qty,
                "balance_qty":        so.balance_qty,

                # warehouse & allocation
                "total_wh_qty":          wh_qty_company,
                "allocated_qty":         alloc,
                "balance_to_allocate":   balance_to_allocate,
                "wh_qty_after_alloc":    wh_after_alloc,

                # purchasing
                "total_ordered_qty":          total_ordered,
                "ordered_qty_against_so":     ordered_against_so,
                "po_date":                    so.po_date,
                "po_number":                  so.po_number,

                # misc
                "so_ref_number":              so.sales_order,
                "balance_to_order_against_so": balance_to_order_so,
                "total_balance_to_order":      total_balance_order,
            }
        )

    return data
