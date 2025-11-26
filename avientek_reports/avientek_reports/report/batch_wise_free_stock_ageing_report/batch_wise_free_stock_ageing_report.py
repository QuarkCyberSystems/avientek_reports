#Copyright (c) 2025, QCS and contributors
#For license information, please see license.txt

import frappe

def execute(filters=None):
    if not filters:
        filters = {}

    columns = get_columns()
    data = get_data(filters)

    return columns, data


# ----------------------------------------------
#  COLUMNS
# ----------------------------------------------

def get_columns():
    return [
        {"label": "Item Code", "fieldname": "item_code", "fieldtype": "Link", "options": "Item", "width": 140},
        {"label": "Item Name", "fieldname": "item_name", "fieldtype": "Data", "width": 180},
        {"label": "Description", "fieldname": "description", "fieldtype": "Data", "width": 220},

        {"label": "Company", "fieldname": "company", "fieldtype": "Link", "options": "Company", "width": 120},
        {"label": "Warehouse", "fieldname": "warehouse", "fieldtype": "Link", "options": "Warehouse", "width": 140},

        {"label": "Batch ID", "fieldname": "batch_no", "fieldtype": "Link", "options": "Batch", "width": 120},
        {"label": "Manufacturing Date", "fieldname": "manufacturing_date", "fieldtype": "Date", "width": 130},

        {"label": "Brand", "fieldname": "brand", "fieldtype": "Link", "options": "Brand", "width": 120},
        {"label": "Part Number", "fieldname": "part_number", "fieldtype": "Data", "width": 140},

        {"label": "Balance Qty", "fieldname": "balance_qty", "fieldtype": "Float", "width": 120},
        {"label": "Free Stock", "fieldname": "free_stock", "fieldtype": "Float", "width": 120},

        {"label": "Opening Qty", "fieldname": "opening_qty", "fieldtype": "Float", "width": 130},
        {"label": "Batch Qty", "fieldname": "batch_qty", "fieldtype": "Float", "width": 130},
    ]


# ----------------------------------------------
#  DATA FETCHING
# ----------------------------------------------

def get_data(filters):
    conditions = ""
    values = {}

    if filters.get("company"):
        conditions += " AND sle.company = %(company)s"
        values["company"] = filters.get("company")

    if filters.get("from_date") and filters.get("to_date"):
        conditions += " AND sle.posting_date BETWEEN %(from_date)s AND %(to_date)s"
        values["from_date"] = filters.get("from_date")
        values["to_date"] = filters.get("to_date")

    query = f"""
        SELECT
            sle.item_code,
            item.item_name,
            item.description,
            sle.company,
            sle.warehouse,
            sle.batch_no,
            batch.manufacturing_date,
            item.brand,
            item.part_number AS part_number,

            -- Opening Qty (no negative)
            GREATEST(
                SUM(CASE WHEN sle.posting_date < %(from_date)s THEN sle.actual_qty ELSE 0 END),
            0) AS opening_qty,

            -- Batch Qty (no negative)
            GREATEST(
                SUM(CASE WHEN sle.batch_no IS NOT NULL THEN sle.actual_qty ELSE 0 END),
            0) AS batch_qty,

            -- Balance Qty (no negative)
            GREATEST(SUM(sle.actual_qty), 0) AS balance_qty,

            -- Free Stock (no negative)
            (
                SELECT GREATEST(SUM(bin.actual_qty), 0)
                FROM `tabBin` bin
                WHERE bin.item_code = sle.item_code
                AND bin.warehouse = sle.warehouse
            ) AS free_stock

        FROM `tabStock Ledger Entry` sle
        LEFT JOIN `tabBatch` batch ON batch.name = sle.batch_no
        LEFT JOIN `tabItem` item ON item.name = sle.item_code
        WHERE
            sle.docstatus < 2
            {conditions}
        GROUP BY
            sle.item_code, sle.batch_no, sle.company, sle.warehouse
        ORDER BY
            sle.item_code, sle.batch_no
    """

    data = frappe.db.sql(query, values, as_dict=True)
    return data
