# Copyright (c) 2025, QCS
# License: see license.txt

import frappe

# ------------------------------------------------------------
#  report entry-point
# ------------------------------------------------------------
def execute(filters=None):
    filters = frappe._dict(filters or {})
    columns = get_columns()
    data    = get_data(filters)
    return columns, data


# ------------------------------------------------------------
#  columns
# ------------------------------------------------------------
def get_columns():
    return [
        {"label": "Type",                     "fieldname": "type",                       "width": 120},
        {"label": "Salesperson",              "fieldname": "salesperson",                "width": 150},
        {"label": "Brand",                    "fieldname": "brand",                      "width": 120},
        {"label": "Country",                  "fieldname": "country",                    "width": 120},
        {"label": "Revenue Target ($)",       "fieldname": "revenue_target",   "fieldtype": "Currency", "width": 150},
        {"label": "Achieved Revenue ($)",     "fieldname": "achieved_revenue", "fieldtype": "Currency", "width": 150},
        {"label": "Achieved %",               "fieldname": "achieved_percentage","fieldtype": "Percent",  "width": 110},
        {"label": "Margin Target ($)",        "fieldname": "margin_target",    "fieldtype": "Currency", "width": 150},
        {"label": "Achieved Margin ($)",      "fieldname": "achieved_margin",  "fieldtype": "Currency", "width": 150},
        {"label": "Achieved Margin %",        "fieldname": "achieved_margin_percentage","fieldtype": "Percent","width": 130},
    ]


# ------------------------------------------------------------
#  main data builder
# ------------------------------------------------------------
def get_data(filters):
    """
    1. fetch targets  → sales_target_map, margin_target_map
    2. fetch achieved → achieved_revenue_map, achieved_margin_map
       – both keyed as (salesperson, brand, country)
    3. assemble rows
    4. if 'group_by' == 'All Brands' → aggregate across brands
    """
    # optional aggregation
    group_all = filters.get("group_by") == "All Brands"

    # fetch maps ----------------------------------------------------------------
    type_map                        = get_salesperson_types()
    sales_target_map, margin_target_map          = get_sales_targets(filters, group_all)
    achieved_revenue_map, achieved_margin_map    = get_achieved_revenue_and_margin(filters, group_all)

    # ---------------------------------------------------------------------------
    #  build brand-level list first
    # ---------------------------------------------------------------------------
    rows = []
    for key, revenue_target in sales_target_map.items():
        sp, brand, country = key
        achieved_rev   = achieved_revenue_map.get(key, 0)
        achieved_pct   = (achieved_rev / revenue_target * 100) if revenue_target else 0

        margin_target  = margin_target_map.get(key, 0)
        achieved_marg  = achieved_margin_map.get(key, 0)
        achieved_m_pct = (achieved_marg / margin_target * 100) if margin_target else 0

        rows.append({
            "type":      type_map.get(sp, "N/A"),
            "salesperson": sp,
            "brand":       brand,
            "country":     country,
            "revenue_target":          revenue_target,
            "achieved_revenue":        achieved_rev,
            "achieved_percentage":     round(achieved_pct, 2),
            "margin_target":           margin_target,
            "achieved_margin":         achieved_marg,
            "achieved_margin_percentage": round(achieved_m_pct, 2),
        })

    # ---------------------------------------------------------------------------
    #  aggregate to “All brands” if requested
    # ---------------------------------------------------------------------------
    if group_all:
        agg = {}
        for r in rows:
            k = (r["type"], r["salesperson"], r["country"])  # ignore brand
            if k not in agg:
                agg[k] = {
                    "type"          : r["type"],
                    "salesperson"   : r["salesperson"],
                    "brand"         : "All brands",
                    "country"       : r["country"],
                    "revenue_target": 0,
                    "achieved_revenue": 0,
                    "margin_target" : 0,
                    "achieved_margin": 0,
                }
            agg_row = agg[k]
            agg_row["revenue_target"]   += r["revenue_target"]
            agg_row["achieved_revenue"] += r["achieved_revenue"]
            agg_row["margin_target"]    += r["margin_target"]
            agg_row["achieved_margin"]  += r["achieved_margin"]

        # recompute % fields
        for a in agg.values():
            a["achieved_percentage"] = (
                a["achieved_revenue"] / a["revenue_target"] * 100
                if a["revenue_target"] else 0
            )
            a["achieved_margin_percentage"] = (
                a["achieved_margin"] / a["margin_target"] * 100
                if a["margin_target"] else 0
            )

        rows = list(agg.values())

    return rows


# ------------------------------------------------------------
#  helpers
# ------------------------------------------------------------
def get_salesperson_types():
    rows = frappe.db.sql("""
        SELECT name, parent_sales_person
        FROM `tabSales Person`
    """, as_dict=True)
    return {r.name: r.parent_sales_person or "N/A" for r in rows}


def get_sales_targets(filters, group_all):
    """
    Return two dicts keyed by (salesperson, brand, country)
    """
    target_map  = {}
    margin_map  = {}

    sql = """
        SELECT 
            sp.name                 AS salesperson,
            COALESCE(td.custom_brand, 'Unknown')    AS brand,
            td.target_amount,
            td.custom_margin_target,
            si.territory            AS country
        FROM `tabTarget Detail` td
        INNER JOIN `tabSales Person`   sp ON td.parent = sp.name
        INNER JOIN `tabSales Team`     st ON st.sales_person = sp.name
        INNER JOIN `tabSales Invoice`  si ON si.name = st.parent
        WHERE si.docstatus = 1
    """
    conds, params = [], []

    # brand filter ignored if we are aggregating “All Brands”
    if filters.get("brand") and not group_all:
        conds.append("td.custom_brand = %s")
        params.append(filters["brand"])
    if filters.get("salesperson"):
        conds.append("sp.name = %s")
        params.append(filters["salesperson"])
    if filters.get("country"):
        conds.append("si.territory = %s")
        params.append(filters["country"])
    if filters.get("from_date") and filters.get("to_date"):
        conds.append("si.posting_date BETWEEN %s AND %s")
        params += [filters["from_date"], filters["to_date"]]

    if conds:
        sql += " AND " + " AND ".join(conds)

    for r in frappe.db.sql(sql, params, as_dict=True):
        key = (r.salesperson, r.brand, r.country)
        target_map[key] = r.target_amount or 0
        margin_map[key] = r.custom_margin_target or 0

    return target_map, margin_map


def get_achieved_revenue_and_margin(filters, group_all):
    conds  = ["si.docstatus = 1"]
    params = []

    # ignore brand filter if “All Brands”
    if filters.get("brand") and not group_all:
        conds.append("si_item.brand = %s")
        params.append(filters["brand"])
    if filters.get("salesperson"):
        conds.append("st.sales_person = %s")
        params.append(filters["salesperson"])
    if filters.get("country"):
        conds.append("si.territory = %s")
        params.append(filters["country"])
    if filters.get("from_date") and filters.get("to_date"):
        conds.append("si.posting_date BETWEEN %s AND %s")
        params += [filters["from_date"], filters["to_date"]]

    achieved = frappe.db.sql(f"""
        SELECT
            st.sales_person             AS salesperson,
            COALESCE(si_item.brand, 'Unknown') AS brand,
            si.territory                AS country,
            SUM(si_item.net_amount * st.allocated_percentage / 100)                                AS achieved_revenue,
            SUM((si_item.net_amount - si_item.incoming_rate) * st.allocated_percentage / 100)      AS achieved_margin
        FROM `tabSales Invoice` si
        JOIN `tabSales Invoice Item` si_item ON si_item.parent = si.name
        JOIN `tabSales Team` st ON st.parent = si.name
        WHERE {" AND ".join(conds)}
        GROUP BY st.sales_person, si_item.brand, si.territory
    """, params, as_dict=True)

    rev_map, marg_map = {}, {}
    for r in achieved:
        key = (r.salesperson, r.brand, r.country)
        rev_map[key]  = r.achieved_revenue or 0
        marg_map[key] = r.achieved_margin  or 0
    return rev_map, marg_map


# ------------------------------------------------------------
#  zero-row scrub (optional; unchanged)
# ------------------------------------------------------------
def scrub_zero_rows(cols, rows):
    num_fields = [c["fieldname"] for c in cols if c.get("fieldtype") in ("Currency", "Float", "Percent")]
    return [r for r in rows if any(r.get(f) for f in num_fields)]
