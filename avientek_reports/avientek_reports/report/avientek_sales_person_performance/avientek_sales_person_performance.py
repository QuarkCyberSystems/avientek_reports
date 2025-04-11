# Copyright (c) 2025, QCS and contributors
# For license information, please see license.txt

import frappe

def execute(filters=None):
    if not filters:
        filters = {}

    columns = get_columns()
    data = get_data(filters)

    return columns, data

def get_columns():
    return [
        {"label": "Type", "fieldname": "type", "fieldtype": "Data", "width": 120},
        {"label": "Salesperson", "fieldname": "salesperson", "fieldtype": "Data", "width": 150},
        {"label": "Brand", "fieldname": "brand", "fieldtype": "Data", "width": 120},
        {"label": "Country", "fieldname": "country", "fieldtype": "Data", "width": 120},
        {"label": "Revenue Target ($)", "fieldname": "revenue_target", "fieldtype": "Currency", "width": 150},
        {"label": "Achieved Revenue ($)", "fieldname": "achieved_revenue", "fieldtype": "Currency", "width": 150},
        {"label": "Achieved %", "fieldname": "achieved_percentage", "fieldtype": "Percent", "width": 120},
        {"label": "Margin Target ($)", "fieldname": "margin_target", "fieldtype": "Currency", "width": 150},
        {"label": "Achieved Margin ($)", "fieldname": "achieved_margin", "fieldtype": "Currency", "width": 150},
        {"label": "Achieved Margin %", "fieldname": "achieved_margin_percentage", "fieldtype": "Percent", "width": 120},
    ]

def get_data(filters):
    type_map = get_salesperson_types()
    sales_target_map, margin_target_map = get_sales_targets(filters)
    achieved_revenue_map, achieved_margin_map = get_achieved_revenue_and_margin(filters)

    data = []
    for key, revenue_target in sales_target_map.items():
        salesperson, brand, country = key
        achieved_revenue = achieved_revenue_map.get(key, 0)
        achieved_percentage = (achieved_revenue / revenue_target * 100) if revenue_target else 0

        margin_target = margin_target_map.get(key, 0)
        achieved_margin = achieved_margin_map.get(key, 0)
        achieved_margin_percentage = (achieved_margin / margin_target * 100) if margin_target else 0

        data.append({
            "type": type_map.get(salesperson, "N/A"),
            "salesperson": salesperson,
            "brand": brand,
            "country": country,
            "revenue_target": revenue_target,
            "achieved_revenue": achieved_revenue,
            "achieved_percentage": round(achieved_percentage, 2),
            "margin_target": margin_target,
            "achieved_margin": achieved_margin,
            "achieved_margin_percentage": round(achieved_margin_percentage, 2)
        })

    return data


def get_salesperson_types():
    sales_types = frappe.db.sql("""
        SELECT name, parent_sales_person
        FROM `tabSales Person`
    """, as_dict=True)
    return {row["name"]: row["parent_sales_person"] or "N/A" for row in sales_types}


def get_sales_targets(filters):
    target_map = {}
    margin_map = {}

    # Query to fetch sales targets along with associated territories
    query = """
        SELECT 
            sp.name AS salesperson,
            td.custom_brand AS brand,
            td.target_amount,
            td.custom_margin_target,
            si.territory AS country
        FROM `tabTarget Detail` td
        INNER JOIN `tabSales Person` sp ON td.parent = sp.name
        INNER JOIN `tabSales Team` st ON sp.name = st.sales_person
        INNER JOIN `tabSales Invoice` si ON si.name = st.parent
        WHERE si.docstatus = 1
    """

    params = []

    # Apply filters if they exist
    if filters.get("salesperson"):
        query += " AND sp.name = %s"
        params.append(filters["salesperson"])
    if filters.get("brand"):
        query += " AND td.custom_brand = %s"
        params.append(filters["brand"])
    if filters.get("country"):
        query += " AND si.territory = %s"
        params.append(filters["country"])
    if filters.get("from_date") and filters.get("to_date"):
        query += " AND si.posting_date BETWEEN %s AND %s"
        params.extend([filters["from_date"], filters["to_date"]])

    sales_targets = frappe.db.sql(query, params, as_dict=True)

    for row in sales_targets:
        key = (row.salesperson, row.brand, row.country)
        target_map[key] = row.target_amount or 0
        margin_map[key] = row.custom_margin_target or 0

    return target_map, margin_map


def get_achieved_revenue_and_margin(filters):
    conditions = get_invoice_conditions(filters)
    achieved_data = frappe.db.sql(f"""
        SELECT st.sales_person, si_item.brand, si.territory AS country, 
               SUM(si_item.net_amount * (st.allocated_percentage / 100)) AS achieved_revenue,
               SUM((si_item.net_amount - si_item.incoming_rate) * (st.allocated_percentage / 100)) AS achieved_margin
        FROM `tabSales Invoice` si
        JOIN `tabSales Invoice Item` si_item ON si.name = si_item.parent
        JOIN `tabSales Team` st ON si.name = st.parent
        WHERE {conditions}
        GROUP BY st.sales_person, si_item.brand, si.territory
    """, as_dict=True)

    revenue_map = {}
    margin_map = {}
    for row in achieved_data:
        key = (row["sales_person"], row["brand"], row["country"])
        revenue_map[key] = row["achieved_revenue"] or 0
        margin_map[key] = row["achieved_margin"] or 0

    return revenue_map, margin_map

def get_conditions(filters, date_field=None):
    conditions = ["1=1"]
    if filters.get("salesperson"):
        conditions.append(f"td.parent = '{filters['salesperson']}'")
    if filters.get("brand"):
        conditions.append(f"td.custom_brand = '{filters['brand']}'")
    if filters.get("country"):
        conditions.append(f"si.territory = '{filters['country']}'")
    if date_field and filters.get("from_date") and filters.get("to_date"):
        conditions.append(f"{date_field} BETWEEN '{filters['from_date']}' AND '{filters['to_date']}'")
    return " AND ".join(conditions)


def get_invoice_conditions(filters):
    conditions = ["si.docstatus = 1"]  # Only submitted invoices

    if filters.get("salesperson"):
        conditions.append(f"st.sales_person = '{filters['salesperson']}'")
    if filters.get("brand"):
        conditions.append(f"si_item.brand = '{filters['brand']}'")
    if filters.get("country"):
        conditions.append(f"si.territory = '{filters['country']}'")
    if filters.get("from_date") and filters.get("to_date"):
        conditions.append(f"si.posting_date BETWEEN '{filters['from_date']}' AND '{filters['to_date']}'")

    return " AND ".join(conditions)
