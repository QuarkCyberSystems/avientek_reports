import frappe
from frappe import _
from frappe.utils import add_to_date, flt, get_datetime, getdate, nowdate
from pypika import functions as fn
from erpnext.stock.doctype.warehouse.warehouse import apply_warehouse_filter

SLE_COUNT_LIMIT = 100_000


# -----------------------------
# MAIN
# -----------------------------
def execute(filters=None):
    if not filters:
        filters = {}

    # basic validations
    if not filters.get("from_date") or not filters.get("to_date"):
        frappe.throw(_("From Date and To Date are required"))

    if filters.get("from_date") > filters.get("to_date"):
        frappe.throw(_("From Date must be before To Date"))

    # safe precision
    float_precision = 3

    # parse ageing ranges
    ranges = parse_ranges(filters.get("range", "30, 60, 90"))
    buckets = build_ageing_buckets(ranges)

    # columns
    columns = get_columns(ranges)

    # masters
    item_map = get_item_details()
    batch_map = get_batch_details_all()

    # fetch SLE rows (both paths)
    sle_rows = get_stock_ledger_entries(filters)

    # sort SLE rows by posting_datetime/posting_date (chronological)
    def _posting_key(r):
        return r.get("posting_datetime") or r.get("posting_date") or ""
    sle_rows = sorted(sle_rows, key=_posting_key)

    # build iwb map: item -> warehouse -> batch -> aggregates
    iwb_map = {}
    from_date = getdate(filters["from_date"])
    to_date = getdate(filters["to_date"])

    for d in sle_rows:
        item_code = d.get("item_code")
        warehouse = d.get("warehouse")
        batch_no = d.get("batch_no")

        # skip empty batch
        if not batch_no:
            continue

        iwb_map.setdefault(item_code, {}).setdefault(warehouse, {}).setdefault(
            batch_no,
            frappe._dict(
                {
                    "opening_qty": 0.0,
                    "in_qty": 0.0,
                    "out_qty": 0.0,
                    "bal_qty": 0.0,
                    "bal_value": 0.0,
                }
            ),
        )

        qty_dict = iwb_map[item_code][warehouse][batch_no]

        # posting_date for date comparisons
        posting_date = d.get("posting_date")
        if posting_date is None and d.get("posting_datetime"):
            pd = d.get("posting_datetime")
            # pd might be string or datetime
            try:
                posting_date = pd.date() if hasattr(pd, "date") else pd
            except Exception:
                posting_date = pd

        actual_qty = flt(d.get("actual_qty", 0.0), float_precision)
        stock_value_diff = flt(d.get("stock_value_difference") or 0.0, float_precision)

        # opening qty -> postings before from_date
        if posting_date and posting_date < from_date:
            qty_dict.opening_qty = flt(qty_dict.opening_qty, float_precision) + actual_qty

        # in/out within the period
        if posting_date and from_date <= posting_date <= to_date:
            if actual_qty > 0:
                qty_dict.in_qty = flt(qty_dict.in_qty, float_precision) + actual_qty
            else:
                qty_dict.out_qty = flt(qty_dict.out_qty, float_precision) + abs(actual_qty)

        # running balance (include SLEs up to to_date)
        qty_dict.bal_qty = flt(qty_dict.bal_qty, float_precision) + actual_qty
        qty_dict.bal_value = flt(qty_dict.bal_value, float_precision) + stock_value_diff

    # build rows
    data = []
    today = to_date

    for item in sorted(iwb_map):
        for wh in sorted(iwb_map[item]):
            for batch in sorted(iwb_map[item][wh]):
                qty = iwb_map[item][wh][batch]

                # exclude negative stock rows per your choice (Option 1)
                if flt(qty.bal_qty, float_precision) < 0:
                    continue

                # skip rows with nothing
                if not (qty.opening_qty or qty.in_qty or qty.out_qty or qty.bal_qty or qty.bal_value):
                    continue

                item_info = item_map.get(item, frappe._dict({}))
                batch_info = batch_map.get(batch, frappe._dict({}))

                # manufacturing date safe handling
                mfg = batch_info.get("manufacturing_date")
                if mfg:
                    if hasattr(mfg, "date"):
                        try:
                            mfg_date = mfg.date()
                        except Exception:
                            mfg_date = mfg
                    else:
                        mfg_date = mfg
                else:
                    mfg_date = None

                # age in days
                if mfg_date:
                    try:
                        age_days = (today - mfg_date).days
                    except Exception:
                        age_days = 0
                else:
                    age_days = 0

                # valuation rate: compute from bal_value / bal_qty where possible
                valuation_rate = flt((qty.bal_value / qty.bal_qty) if qty.bal_qty else 0.0, float_precision)
                # Balance value as requested = bal_qty * valuation_rate
                balance_value = flt(qty.bal_qty * valuation_rate, float_precision)

                # prepare base row (order must match get_columns)
                row = [
                    item,
                    item_info.get("item_name"),
                    item_info.get("description"),
                    wh,
                    batch,
                    flt(qty.opening_qty, float_precision),
                    flt(qty.bal_qty, float_precision),
                    valuation_rate,
                    balance_value,
                    filters.get("company"),
                    mfg_date,
                    item_info.get("brand"),
                    item_info.get("part_number"),
                    batch_info.get("batch_qty") or 0.0,
                ]

                # create ageing buckets values (Qty & Value)
                for start, end in buckets:
                    if start <= age_days <= end:
                        bkt_qty = flt(qty.bal_qty, float_precision)
                        bkt_val = flt(bkt_qty * valuation_rate, float_precision)
                    else:
                        bkt_qty = 0.0
                        bkt_val = 0.0
                    row.append(bkt_qty)
                    row.append(bkt_val)

                data.append(row)

    return columns, data


# ---------------------------
# helper & column functions
# ---------------------------
def parse_ranges(range_str):
    if not range_str:
        return [30, 60, 90]
    try:
        parts = [p.strip() for p in range_str.split(",") if p.strip() != ""]
        nums = []
        for p in parts:
            try:
                n = int(p)
                if n > 0:
                    nums.append(n)
            except Exception:
                continue
        nums = sorted(nums)
        if len(nums) < 3:
            return [30, 60, 90]
        return nums
    except Exception:
        return [30, 60, 90]


def build_ageing_buckets(ranges):
    buckets = []
    prev = 0
    for r in ranges:
        buckets.append((prev, r))
        prev = r + 1
    buckets.append((prev, 99999))
    return buckets


def get_columns(ranges):
    cols = [
        "item_code:Link/Item:120",
        _("Item Name") + "::120",
        _("Description") + "::150",
        _("Warehouse") + ":Link/Warehouse:120",
        _("Batch") + ":Link/Batch:120",
        _("Opening Qty") + ":Float:100",
        _("Balance Qty") + ":Float:100",
        _("Valuation Rate") + ":Float:120",
        _("Balance Value") + ":Currency:120",
        _("Company") + ":Link/Company:120",
        _("Manufacturing Date") + ":Date:120",
        _("Brand") + "::120",
        _("Part Number") + "::120",
        _("Batch Qty") + ":Float:100",
    ]

    for start, end in build_ageing_buckets(ranges):
        if end == 99999:
            label = f"{start}+"
        else:
            label = f"{start} - {end}"
        cols.append(_("Age (" + label + ")") + ":Float:90")
        cols.append(_("Value (" + label + ")") + ":Currency:120")

    return cols


# -----------------------------
# Master fetchers
# -----------------------------
def get_item_details():
    item_map = {}
    for d in frappe.qb.from_("Item").select("name", "item_name", "description", "brand", "part_number").run(as_dict=1):
        item_map.setdefault(d.name, d)
    return item_map


def get_batch_details_all():
    batches = frappe.get_all("Batch", fields=["name", "batch_qty", "manufacturing_date"], limit_page_length=0, ignore_permissions=True)
    return {d.name: d for d in batches}


# -----------------------------
# SLE fetch (batch + bundle combined)
# -----------------------------
def get_stock_ledger_entries(filters):
    entries = []
    entries += get_stock_ledger_entries_for_batch_no(filters)
    entries += get_stock_ledger_entries_for_batch_bundle(filters)
    return entries


def get_stock_ledger_entries_for_batch_no(filters):
    if not filters.get("from_date") or not filters.get("to_date"):
        frappe.throw(_("'From Date' and 'To Date' are required"))

    posting_datetime = get_datetime(add_to_date(filters["to_date"], days=1))

    sle = frappe.qb.DocType("Stock Ledger Entry")
    query = (
        frappe.qb.from_(sle)
        .select(
            sle.item_code,
            sle.warehouse,
            sle.batch_no,
            sle.posting_date,
            fn.Sum(sle.actual_qty).as_("actual_qty"),
            fn.Sum(sle.stock_value_difference).as_("stock_value_difference"),
            fn.Max(sle.valuation_rate).as_("valuation_rate"),
            sle.posting_datetime,
        )
        .where(
            (sle.docstatus < 2)
            & (sle.is_cancelled == 0)
            & (sle.batch_no != "")
            & (sle.posting_datetime < posting_datetime)
        )
        .groupby(sle.voucher_no, sle.batch_no, sle.item_code, sle.warehouse)
    )

    query = apply_warehouse_filter(query, sle, filters)

    if filters.get("warehouse_type") and not filters.get("warehouse"):
        warehouses = frappe.get_all(
            "Warehouse", filters={"warehouse_type": filters.get("warehouse_type"), "is_group": 0}, pluck="name"
        )
        if warehouses:
            query = query.where(sle.warehouse.isin(warehouses))

    for field in ["item_code", "batch_no", "company"]:
        if filters.get(field):
            query = query.where(sle[field] == filters.get(field))

    return query.run(as_dict=True) or []


def get_stock_ledger_entries_for_batch_bundle(filters):
    sle = frappe.qb.DocType("Stock Ledger Entry")
    batch_package = frappe.qb.DocType("Serial and Batch Entry")

    to_date = get_datetime(filters["to_date"] + " 23:59:59")

    # Use parent's stock_value_difference and valuation_rate (child table may not have them)
    query = (
        frappe.qb.from_(sle)
        .inner_join(batch_package)
        .on(batch_package.parent == sle.serial_and_batch_bundle)
        .select(
            sle.item_code,
            sle.warehouse,
            batch_package.batch_no,
            sle.posting_date,
            fn.Sum(batch_package.qty).as_("actual_qty"),
            fn.Sum(sle.stock_value_difference).as_("stock_value_difference"),
            fn.Max(sle.valuation_rate).as_("valuation_rate"),
            sle.posting_datetime,
        )
        .where(
            (sle.docstatus < 2)
            & (sle.is_cancelled == 0)
            & (sle.has_batch_no == 1)
            & (sle.posting_datetime <= to_date)
        )
        .groupby(sle.voucher_no, batch_package.batch_no, batch_package.warehouse)
    )

    query = apply_warehouse_filter(query, sle, filters)

    if filters.get("warehouse_type") and not filters.get("warehouse"):
        warehouses = frappe.get_all(
            "Warehouse", filters={"warehouse_type": filters.get("warehouse_type"), "is_group": 0}, pluck="name"
        )
        if warehouses:
            query = query.where(sle.warehouse.isin(warehouses))

    for field in ["item_code", "batch_no", "company"]:
        if filters.get(field):
            if field == "batch_no":
                query = query.where(batch_package[field] == filters.get(field))
            else:
                query = query.where(sle[field] == filters.get(field))

    return query.run(as_dict=True) or []
