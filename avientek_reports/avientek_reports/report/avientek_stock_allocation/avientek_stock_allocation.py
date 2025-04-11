# Copyright (c) 2025, QCS and contributors
# For license information, please see license.txt

import frappe
from frappe.utils import flt

def execute(filters=None):
    columns = get_columns()
    data = get_data(filters)
    return columns, data

def get_columns():
    return [
        {"label": "Date & Time", "fieldname": "transaction_date", "fieldtype": "Datetime", "width": 150},
        {"label": "Company Name", "fieldname": "company", "fieldtype": "Data", "width": 120},
        {"label": "Sales Order Number", "fieldname": "sales_order", "fieldtype": "Link", "options": "Sales Order", "width": 120},
        {"label": "Sales Person", "fieldname": "sales_person", "fieldtype": "Data", "width": 120},
        {"label": "Country", "fieldname": "country", "fieldtype": "Data", "width": 100},
        {"label": "Customer Name", "fieldname": "customer", "fieldtype": "Data", "width": 150},
        {"label": "Brand", "fieldname": "brand", "fieldtype": "Data", "width": 120},
        {"label": "Part Number", "fieldname": "part_number", "fieldtype": "Data", "width": 120},
        {"label": "Total Demanded Qty", "fieldname": "total_demanded_qty", "fieldtype": "Float", "width": 120},
        {"label": "Sales Order Qty", "fieldname": "sales_order_qty", "fieldtype": "Float", "width": 120},
        {"label": "Delivered Qty", "fieldname": "delivered_qty", "fieldtype": "Float", "width": 120},
        {"label": "Balance Qty", "fieldname": "balance_qty", "fieldtype": "Float", "width": 120},
        {"label": "Total W/H Qty", "fieldname": "total_wh_qty", "fieldtype": "Float", "width": 120},
        {"label": "Allocated Qty (FIFO)", "fieldname": "allocated_qty", "fieldtype": "Float", "width": 120},
        {"label": "Balance to Allocate", "fieldname": "balance_to_allocate", "fieldtype": "Float", "width": 120},
        {"label": "W/H Qty After Allocation", "fieldname": "wh_qty_after_allocation", "fieldtype": "Float", "width": 120},
        {"label": "Total Ordered Qty", "fieldname": "total_ordered_qty", "fieldtype": "Float", "width": 120},
        {"label": "Ordered Qty Against SO", "fieldname": "ordered_qty_against_so", "fieldtype": "Float", "width": 120},
        {"label": "PO Date", "fieldname": "po_date", "fieldtype": "Date", "width": 120},
        {"label": "PO Number", "fieldname": "po_number", "fieldtype": "Data", "width": 120},
        {"label": "SO Ref Number", "fieldname": "so_ref_number", "fieldtype": "Data", "width": 120},
        {"label": "Balance to Order Against SO", "fieldname": "balance_to_order_against_so", "fieldtype": "Float", "width": 120},
        {"label": "Total Balance to Order", "fieldname": "total_balance_to_order", "fieldtype": "Float", "width": 120},
    ]


def get_total_ordered_qty(item_code):
    """Fetch the total ordered quantity from Purchase Orders."""
    total_ordered_qty = frappe.db.sql("""
        SELECT SUM(qty)
        FROM `tabPurchase Order Item`
        WHERE item_code = %s
        AND docstatus = 1
    """, (item_code,))[0][0] or 0  # Default to 0 if no records found
    return total_ordered_qty


def get_data(filters):
    conditions = ""
    if filters.get("company"):
        conditions += f" AND so.company = '{filters.get('company')}'"
    if filters.get("from_date") and filters.get("to_date"):
        conditions += f" AND so.transaction_date BETWEEN '{filters.get('from_date')}' AND '{filters.get('to_date')}'"
    if filters.get("part_number"):
        conditions += f" AND soi.item_code = '{filters.get('part_number')}'"

    sales_orders = frappe.db.sql(f'''
        SELECT so.transaction_date, so.company, so.name AS sales_order,
            (SELECT st.sales_person FROM `tabSales Team` st WHERE st.parent = so.name LIMIT 1) AS sales_person,
            so.customer,
            (SELECT addr.country FROM `tabAddress` addr WHERE addr.name = so.customer_address LIMIT 1) AS country,
            soi.item_code AS part_number, soi.brand, soi.qty AS sales_order_qty, soi.delivered_qty,
            (soi.qty - soi.delivered_qty) AS balance_qty,
            soi.purchase_order AS po_number,
            (SELECT po.transaction_date FROM `tabPurchase Order` po WHERE po.name = soi.purchase_order LIMIT 1) AS po_date, 
            (SELECT SUM(qty) FROM `tabSales Order Item` WHERE item_code = soi.item_code) AS total_demanded_qty,
            (SELECT SUM(actual_qty) FROM `tabBin` WHERE item_code = soi.item_code) AS total_wh_qty
        FROM `tabSales Order` so
        JOIN `tabSales Order Item` soi ON so.name = soi.parent
        WHERE so.docstatus = 1 {conditions}
    ''', as_dict=True)

    stock_entries = frappe.db.sql('''
        SELECT item_code, actual_qty, posting_date
        FROM `tabStock Ledger Entry`
        WHERE actual_qty > 0
        ORDER BY posting_date ASC
    ''', as_dict=True)

    allocated_stock = {}
    for stock in stock_entries:
        allocated_stock.setdefault(stock.item_code, []).append(stock)

    data = []
    for so in sales_orders:
        available_stock = allocated_stock.get(so["part_number"], [])
        allocated_qty = 0
        balance_qty = so["balance_qty"]
        
        for stock in available_stock:
            if balance_qty <= 0:
                break
            allocatable = min(stock["actual_qty"], balance_qty)
            allocated_qty += allocatable
            balance_qty -= allocatable
            stock["actual_qty"] -= allocatable
        total_ordered_qty = get_total_ordered_qty(so["part_number"])
        total_balance_to_deliver = frappe.db.sql("""
            SELECT SUM(qty - delivered_qty) FROM `tabSales Order Item`
            WHERE item_code = %s
        """, (so["part_number"],))[0][0] or 0

        total_balance_to_order = total_balance_to_deliver - allocated_qty
        data.append({
            "transaction_date": so["transaction_date"],
            "company": so["company"],
            "sales_order": so["sales_order"],
            "sales_person": so["sales_person"],
            "customer": so["customer"],
            "country": so["country"],
            "brand": so["brand"],
            "part_number": so["part_number"],
            "total_demanded_qty": so["total_demanded_qty"],
            "sales_order_qty": so["sales_order_qty"],
            "delivered_qty": so["delivered_qty"],
            "balance_qty": so["balance_qty"],
            "total_wh_qty": so["total_wh_qty"],
            "allocated_qty": allocated_qty,
            "balance_to_allocate": so["balance_qty"] - allocated_qty,
            "wh_qty_after_allocation": (so.get("total_wh_qty") or 0) - allocated_qty,
            "total_ordered_qty": total_ordered_qty,
            "so_ref_number": so["sales_order"],
            "ordered_qty_against_so": so["balance_qty"],
            "po_date": so["po_date"],
            "po_number": so["po_number"],
            "balance_to_order_against_so": so["balance_qty"],
            "total_balance_to_order": total_balance_to_order
        })
    
    return data
