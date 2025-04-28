import frappe
from frappe import _
from frappe.utils import flt, today

def execute(filters=None):
    """
    Script Report entry point. Returns (columns, data).

    - No company or warehouse filters.
    - Sums Bin data by Company.
    - Pivot columns by distinct Company.
    - Removes rows if all pivot columns are zero, ignoring unit_price.
    - Includes an 'Item Code' column before 'Part Number'.
    """
    # 1) We'll still allow brand/item_group/item_code filters for Items if desired:
    item_map = get_items_with_price(filters or {})
    if not item_map:
        return [], []

    # 2) Get Bin data for these items (no warehouse filter).
    bin_data = get_bin_data(list(item_map.keys()))
    if not bin_data:
        return [], []

    # 3) Aggregate the bins by Company
    item_company_map, distinct_companies = aggregate_bins_by_company(bin_data)

    # 4) Build columns (pivot by Company)
    columns = build_columns(distinct_companies)

    # 5) Build pivoted data (one row per Item, repeated columns per Company)
    pivoted_data = build_pivoted_data_by_company(item_map, item_company_map, distinct_companies)

    # 6) Remove rows where all pivot columns are zero, ignoring `unit_price`
    pivoted_data = remove_all_zero_rows(columns, pivoted_data)

    return columns, pivoted_data


# ---------------------------------------------------------------------
# 1) Get Items + Unit Price
# ---------------------------------------------------------------------
def get_items_with_price(filters):
    """
    Retrieve items matching filters (brand, item_group, item_code).
    Fetch their Unit Price from 'Item Price' for price_list='Distributer Pricing1'.

    If you want to remove these filters entirely, strip them out.
    """
    conditions = [
        "I.disabled = 0",
        "I.is_stock_item = 1",
        "(I.end_of_life > CURDATE() OR I.end_of_life IS NULL OR I.end_of_life='0000-00-00')"
    ]

    if filters.get("item_code"):
        conditions.append("I.item_code = %(item_code)s")
    if filters.get("brand"):
        conditions.append("I.brand = %(brand)s")
    if filters.get("item_group"):
        conditions.append("I.item_group = %(item_group)s")

    where_clause = " AND ".join(conditions)

    items = frappe.db.sql(
        f"""
        SELECT
            I.name as item_code,
            I.item_group as brand_type,
            I.brand as brand_name,
            I.part_number,
            I.item_name as model,
            I.description
        FROM `tabItem` I
        WHERE {where_clause}
        """,
        filters,
        as_dict=True
    )
    if not items:
        return {}

    # Build list of item_codes
    item_codes = [it.item_code for it in items]

    # Get prices from "Distributer Pricing1"
    prices = frappe.db.sql(
        """
        SELECT item_code, price_list_rate
        FROM `tabItem Price`
        WHERE price_list = %s
          AND item_code IN ({})
        """.format(", ".join(["%s"] * len(item_codes))),
        tuple(["Distributer Pricing1"] + item_codes),
        as_dict=True
    )

    price_map = {}
    for p in prices:
        price_map[p.item_code] = p.price_list_rate

    # Final map
    item_map = {}
    for it in items:
        it["unit_price"] = price_map.get(it["item_code"], 0.0)
        item_map[it["item_code"]] = it

    return item_map


# ---------------------------------------------------------------------
# 2) Get Bin Data (No Warehouse Filter)
# ---------------------------------------------------------------------
def get_bin_data(item_codes):
    """
    Retrieve all Bin rows for the given items, but ignore warehouses whose
    name contains 'RMA' or 'DEMO'.
    """
    if not item_codes:
        return []

    placeholders = ", ".join(["%s"] * len(item_codes))

    return frappe.db.sql(
        f"""
        SELECT
            b.item_code,
            b.warehouse,
            b.actual_qty,
            b.ordered_qty,
            b.reserved_qty,
            b.indented_qty,
            b.projected_qty
        FROM `tabBin` b
        WHERE b.item_code IN ({placeholders})
          AND b.warehouse NOT LIKE '%%RMA%%'
          AND b.warehouse NOT LIKE '%%DEMO%%'
        ORDER BY b.item_code, b.warehouse
        """,
        tuple(item_codes),
        as_dict=True,
    )


# ---------------------------------------------------------------------
# 3) Aggregate Bins by Company
# ---------------------------------------------------------------------
def aggregate_bins_by_company(bin_data):
    """
    Summarize Bin fields at (item_code, company).
    We'll do warehouse -> company lookup from 'Warehouse.company'.
    Return a nested dict + sorted list of distinct companies.
    """
    from collections import defaultdict

    # Build a cache for warehouse -> company
    wh_set = {d["warehouse"] for d in bin_data}
    wh_to_company = {}
    for wh in wh_set:
        comp = frappe.db.get_value("Warehouse", wh, "company")
        wh_to_company[wh] = comp or ""

    # We'll store sums in a nested dict:
    # item_company_map[item_code][company] = dict of aggregated fields
    item_company_map = defaultdict(lambda: defaultdict(lambda: {
        "actual_qty": 0.0,
        "ordered_qty": 0.0,
        "reserved_qty": 0.0,
        "indented_qty": 0.0,
        "projected_qty": 0.0
    }))

    # Accumulate
    for d in bin_data:
        item_code = d["item_code"]
        warehouse = d["warehouse"]
        company = wh_to_company.get(warehouse, "")
        ic_data = item_company_map[item_code][company]
        ic_data["actual_qty"]    += flt(d["actual_qty"])
        ic_data["ordered_qty"]   += flt(d["ordered_qty"])
        ic_data["reserved_qty"]  += flt(d["reserved_qty"])
        ic_data["indented_qty"]  += flt(d["indented_qty"])
        ic_data["projected_qty"] += flt(d["projected_qty"])

    # Distinct companies
    distinct_companies = set()
    for _, comp_dict in item_company_map.items():
        for comp in comp_dict:
            distinct_companies.add(comp)

    distinct_companies = sorted(list(distinct_companies))

    return item_company_map, distinct_companies


# ---------------------------------------------------------------------
# 4) Build Columns (Pivot by Company)
# ---------------------------------------------------------------------
def build_columns(distinct_companies):
    """
    7 detail columns (including new 'Item Code') + for each company, 10 pivot columns + then 10 "Total" columns.
    """
    columns = [
        {
            "label": _("Brand Type"),
            "fieldname": "brand_type",
            "width": 120
        },
        {
            "label": _("Brand Name"),
            "fieldname": "brand_name",
            "width": 120
        },
        {
            # NEW: Item Code column before Part Number
            "label": _("Item Code"),
            "fieldname": "item_code",
            "width": 120
        },
        {
            "label": _("Part Number"),
            "fieldname": "part_number",
            "width": 120
        },
        {
            "label": _("Model"),
            "fieldname": "model",
            "width": 120
        },
        {
            "label": _("Description"),
            "fieldname": "description",
            "width": 150
        },
        {
            "label": _("Unit Price"),
            "fieldname": "unit_price",
            "fieldtype": "Currency",
            "width": 100
        }
    ]

    # 10 repeated columns per company
    sub_columns = [
        ("W/H Stock-Qty",       "wh_stock_qty",   "Float"),
        ("W/H Stock-$",         "wh_stock_val",   "Currency"),
        ("Ordered Stock-Qty",   "ordered_qty",    "Float"),
        ("Ordered Stock-$",     "ordered_val",    "Currency"),
        ("Demanded-Qty",        "demand_qty",     "Float"),
        ("Demanded-$",          "demand_val",     "Currency"),
        ("Free Stock-Qty",      "free_qty",       "Float"),
        ("Free Stock-$",        "free_val",       "Currency"),
        ("Net Free Stock-Qty",  "net_free_qty",   "Float"),
        ("Net Free Stock-$",    "net_free_val",   "Currency"),
    ]

    for comp in distinct_companies:
        for label, field, ftype in sub_columns:
            columns.append({
                "label": f"{comp} - {label}",
                "fieldname": f"{comp}_{field}",
                "fieldtype": ftype,
                "width": 110
            })

    # Then "Total" columns
    for label, field, ftype in sub_columns:
        columns.append({
            "label": _("Total") + " - " + label,
            "fieldname": f"total_{field}",
            "fieldtype": ftype,
            "width": 110
        })

    return columns


# ---------------------------------------------------------------------
# 5) Build Pivoted Data by Company
# ---------------------------------------------------------------------
def build_pivoted_data_by_company(item_map, item_company_map, distinct_companies):
    """
    One row per Item, columns repeated per Company.

    demand_qty = reserved_qty + indented_qty
    free_qty   = actual_qty - reserved_qty
    net_free_qty = free_qty + ordered_qty - demand_qty
    """
    data = []
    for item_code, item_info in item_map.items():
        # Insert 'item_code' in the row
        row = {
            "brand_type":  item_info["brand_type"],
            "brand_name":  item_info["brand_name"],
            "item_code":   item_code,  # specifically show the item_code
            "part_number": item_info["part_number"],
            "model":       item_info["model"],
            "description": item_info["description"],
            "unit_price":  flt(item_info["unit_price"])
        }

        # Totals across all companies
        total_wh_stock_qty  = 0.0
        total_wh_stock_val  = 0.0
        total_ordered_qty   = 0.0
        total_ordered_val   = 0.0
        total_demand_qty    = 0.0
        total_demand_val    = 0.0
        total_free_qty      = 0.0
        total_free_val      = 0.0
        total_net_free_qty  = 0.0
        total_net_free_val  = 0.0

        price = row["unit_price"]

        # Aggregated bin data for this item, by company
        comp_dict = item_company_map.get(item_code, {})
        for comp in distinct_companies:
            cvals = comp_dict.get(comp, {})
            actual_qty   = flt(cvals.get("actual_qty"))
            ordered_qty  = flt(cvals.get("ordered_qty"))
            reserved_qty = flt(cvals.get("reserved_qty"))
            indented_qty = flt(cvals.get("indented_qty"))

            demand_qty   = reserved_qty + indented_qty
            free_qty     = actual_qty - reserved_qty
            net_free_qty = free_qty + ordered_qty - demand_qty

            wh_stock_val  = actual_qty   * price
            ordered_val   = ordered_qty  * price
            demand_val    = demand_qty   * price
            free_val      = free_qty     * price
            net_free_val  = net_free_qty * price

            # Populate pivot columns for this company
            row[f"{comp}_wh_stock_qty"]  = actual_qty
            row[f"{comp}_wh_stock_val"]  = wh_stock_val
            row[f"{comp}_ordered_qty"]   = ordered_qty
            row[f"{comp}_ordered_val"]   = ordered_val
            row[f"{comp}_demand_qty"]    = demand_qty
            row[f"{comp}_demand_val"]    = demand_val
            row[f"{comp}_free_qty"]      = free_qty
            row[f"{comp}_free_val"]      = free_val
            row[f"{comp}_net_free_qty"]  = net_free_qty
            row[f"{comp}_net_free_val"]  = net_free_val

            # Update totals
            total_wh_stock_qty  += actual_qty
            total_wh_stock_val  += wh_stock_val
            total_ordered_qty   += ordered_qty
            total_ordered_val   += ordered_val
            total_demand_qty    += demand_qty
            total_demand_val    += demand_val
            total_free_qty      += free_qty
            total_free_val      += free_val
            total_net_free_qty  += net_free_qty
            total_net_free_val  += net_free_val

        # "Total" columns
        row["total_wh_stock_qty"]  = total_wh_stock_qty
        row["total_wh_stock_val"]  = total_wh_stock_val
        row["total_ordered_qty"]   = total_ordered_qty
        row["total_ordered_val"]   = total_ordered_val
        row["total_demand_qty"]    = total_demand_qty
        row["total_demand_val"]    = total_demand_val
        row["total_free_qty"]      = total_free_qty
        row["total_free_val"]      = total_free_val
        row["total_net_free_qty"]  = total_net_free_qty
        row["total_net_free_val"]  = total_net_free_val

        data.append(row)

    return data


# ---------------------------------------------------------------------
# 6) Remove Rows with All Zero
# ---------------------------------------------------------------------
def remove_all_zero_rows(columns, data):
    """
    Remove any row if all pivot numeric columns are 0 or None,
    ignoring 'unit_price'.
    """
    if not data or not columns:
        return []

    numeric_fields = []
    for col in columns:
        if col.get("fieldtype") in ("Float", "Currency", "Int"):
            fname = col["fieldname"]
            if fname != "unit_price":
                numeric_fields.append(fname)

    filtered_data = []
    for row in data:
        row_is_all_zero = True
        for field in numeric_fields:
            val = row.get(field)
            if val not in (0, None):
                row_is_all_zero = False
                break
        if not row_is_all_zero:
            filtered_data.append(row)

    return filtered_data
