# Copyright (c) 2025, QCS and contributors
# For license information, please see license.txt
# Based on erpnext/stock/report/batch_wise_balance_history/batch_wise_balance_history.py

import frappe
from frappe import _
from frappe.utils import cint, flt, getdate
from pypika import functions as fn

from erpnext.stock.doctype.warehouse.warehouse import apply_warehouse_filter

SLE_COUNT_LIMIT = 100_000


def execute(filters=None):
    if not filters:
        filters = {}

    sle_count = frappe.db.count("Stock Ledger Entry")

    if (
        sle_count > SLE_COUNT_LIMIT
        and not filters.get("item_code")
        and not filters.get("warehouse")
        and not filters.get("warehouse_type")
    ):
        frappe.throw(
            _("Please select either the Item or Warehouse or Warehouse Type filter to generate the report.")
        )

    if filters.get("from_date") and filters.get("to_date"):
        if filters.from_date > filters.to_date:
            frappe.throw(_("From Date must be before To Date"))

    float_precision = cint(frappe.db.get_default("float_precision")) or 3

    columns = get_columns()
    item_map = get_item_details(filters)
    batch_map = get_batch_details()
    iwb_map = get_item_warehouse_batch_map(filters, float_precision)

    # Get undelivered SO quantities per item+warehouse
    so_reserved_map = get_so_reserved_qty(filters)

    # Calculate free stock using FIFO (oldest batch first)
    calculate_free_stock(iwb_map, so_reserved_map, batch_map, float_precision)

    data = []
    for item in sorted(iwb_map):
        if not filters.get("item") or filters.get("item") == item:
            for wh in sorted(iwb_map[item]):
                for batch in sorted(iwb_map[item][wh]):
                    qty_dict = iwb_map[item][wh][batch]
                    # Only show batches with positive balance
                    if flt(qty_dict.bal_qty, float_precision) > 0:
                        item_details = item_map.get(item, {})
                        batch_details = batch_map.get(batch, {})
                        data.append(
                            {
                                "item_code": item,
                                "item_name": item_details.get("item_name", ""),
                                "description": item_details.get("description", ""),
                                "warehouse": wh,
                                "batch_no": batch,
                                "manufacturing_date": batch_details.get("manufacturing_date"),
                                "opening_qty": flt(qty_dict.opening_qty, float_precision),
                                "in_qty": flt(qty_dict.in_qty, float_precision),
                                "out_qty": flt(qty_dict.out_qty, float_precision),
                                "balance_qty": flt(qty_dict.bal_qty, float_precision),
                                "free_stock": flt(qty_dict.free_stock, float_precision),
                                "stock_uom": item_details.get("stock_uom", ""),
                            }
                        )

    return columns, data


def get_columns():
    return [
        {"label": _("Item"), "fieldname": "item_code", "fieldtype": "Link", "options": "Item", "width": 120},
        {"label": _("Item Name"), "fieldname": "item_name", "fieldtype": "Data", "width": 150},
        {"label": _("Description"), "fieldname": "description", "fieldtype": "Data", "width": 150},
        {"label": _("Warehouse"), "fieldname": "warehouse", "fieldtype": "Link", "options": "Warehouse", "width": 120},
        {"label": _("Batch"), "fieldname": "batch_no", "fieldtype": "Link", "options": "Batch", "width": 120},
        {"label": _("Mfg Date"), "fieldname": "manufacturing_date", "fieldtype": "Date", "width": 100},
        {"label": _("Opening Qty"), "fieldname": "opening_qty", "fieldtype": "Float", "width": 100},
        {"label": _("In Qty"), "fieldname": "in_qty", "fieldtype": "Float", "width": 80},
        {"label": _("Out Qty"), "fieldname": "out_qty", "fieldtype": "Float", "width": 80},
        {"label": _("Balance Qty"), "fieldname": "balance_qty", "fieldtype": "Float", "width": 100},
        {"label": _("Free Stock"), "fieldname": "free_stock", "fieldtype": "Float", "width": 100},
        {"label": _("UOM"), "fieldname": "stock_uom", "fieldtype": "Link", "options": "UOM", "width": 80},
    ]


def get_stock_ledger_entries(filters):
    """Fetch stock ledger entries for batch items"""
    entries = get_stock_ledger_entries_for_batch_no(filters)
    entries += get_stock_ledger_entries_for_batch_bundle(filters)
    return entries


def get_stock_ledger_entries_for_batch_no(filters):
    """Legacy batch_no field entries"""
    if not filters.get("from_date"):
        frappe.throw(_("'From Date' is required"))
    if not filters.get("to_date"):
        frappe.throw(_("'To Date' is required"))

    sle = frappe.qb.DocType("Stock Ledger Entry")
    query = (
        frappe.qb.from_(sle)
        .select(
            sle.item_code,
            sle.warehouse,
            sle.batch_no,
            sle.posting_date,
            fn.Sum(sle.actual_qty).as_("actual_qty"),
        )
        .where(
            (sle.docstatus < 2)
            & (sle.is_cancelled == 0)
            & (sle.batch_no.isnotnull())
            & (sle.batch_no != "")
            & (sle.posting_date <= filters.get("to_date"))
        )
        .groupby(sle.voucher_no, sle.batch_no, sle.item_code, sle.warehouse)
    )

    query = apply_warehouse_filter(query, sle, filters)

    if filters.get("warehouse_type") and not filters.get("warehouse"):
        warehouses = frappe.get_all(
            "Warehouse",
            filters={"warehouse_type": filters.warehouse_type, "is_group": 0},
            pluck="name",
        )
        if warehouses:
            query = query.where(sle.warehouse.isin(warehouses))

    for field in ["item_code", "batch_no", "company"]:
        if filters.get(field):
            query = query.where(sle[field] == filters.get(field))

    return query.run(as_dict=True) or []


def get_stock_ledger_entries_for_batch_bundle(filters):
    """Serial and Batch Bundle entries (newer ERPNext)"""
    sle = frappe.qb.DocType("Stock Ledger Entry")
    batch_package = frappe.qb.DocType("Serial and Batch Entry")

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
        )
        .where(
            (sle.docstatus < 2)
            & (sle.is_cancelled == 0)
            & (sle.has_batch_no == 1)
            & (sle.posting_date <= filters.get("to_date"))
        )
        .groupby(sle.voucher_no, batch_package.batch_no, batch_package.warehouse)
    )

    query = apply_warehouse_filter(query, sle, filters)

    if filters.get("warehouse_type") and not filters.get("warehouse"):
        warehouses = frappe.get_all(
            "Warehouse",
            filters={"warehouse_type": filters.warehouse_type, "is_group": 0},
            pluck="name",
        )
        if warehouses:
            query = query.where(sle.warehouse.isin(warehouses))

    for field in ["item_code", "company"]:
        if filters.get(field):
            query = query.where(sle[field] == filters.get(field))

    if filters.get("batch_no"):
        query = query.where(batch_package.batch_no == filters.get("batch_no"))

    try:
        return query.run(as_dict=True) or []
    except Exception:
        # Serial and Batch Entry table might not exist in older versions
        return []


def get_item_warehouse_batch_map(filters, float_precision):
    """Build item -> warehouse -> batch map with qty calculations"""
    sle = get_stock_ledger_entries(filters)
    iwb_map = {}

    from_date = getdate(filters["from_date"])
    to_date = getdate(filters["to_date"])

    for d in sle:
        iwb_map.setdefault(d.item_code, {}).setdefault(d.warehouse, {}).setdefault(
            d.batch_no,
            frappe._dict({
                "opening_qty": 0.0,
                "in_qty": 0.0,
                "out_qty": 0.0,
                "bal_qty": 0.0,
                "free_stock": 0.0,
            })
        )
        qty_dict = iwb_map[d.item_code][d.warehouse][d.batch_no]

        posting_date = getdate(d.posting_date)

        if posting_date < from_date:
            qty_dict.opening_qty = flt(qty_dict.opening_qty, float_precision) + flt(
                d.actual_qty, float_precision
            )
        elif posting_date >= from_date and posting_date <= to_date:
            if flt(d.actual_qty) > 0:
                qty_dict.in_qty = flt(qty_dict.in_qty, float_precision) + flt(d.actual_qty, float_precision)
            else:
                qty_dict.out_qty = flt(qty_dict.out_qty, float_precision) + abs(
                    flt(d.actual_qty, float_precision)
                )

        qty_dict.bal_qty = flt(qty_dict.bal_qty, float_precision) + flt(d.actual_qty, float_precision)

    return iwb_map


def get_item_details(filters):
    """Fetch item details"""
    item_map = {}
    for d in frappe.get_all(
        "Item",
        fields=["name", "item_name", "description", "stock_uom"],
    ):
        item_map[d.name] = d
    return item_map


def get_batch_details():
    """Fetch batch details with manufacturing date"""
    batch_map = {}
    for d in frappe.get_all(
        "Batch",
        fields=["name", "manufacturing_date", "creation"],
    ):
        batch_map[d.name] = d
    return batch_map


def get_so_reserved_qty(filters):
    """
    Get undelivered quantities from Sales Orders per item (across all warehouses).
    Undelivered Qty = stock_qty - delivered_qty
    Only considers submitted SOs that are not closed/cancelled.
    """
    so = frappe.qb.DocType("Sales Order")
    soi = frappe.qb.DocType("Sales Order Item")

    query = (
        frappe.qb.from_(soi)
        .inner_join(so)
        .on(so.name == soi.parent)
        .select(
            soi.item_code,
            fn.Sum(soi.stock_qty - soi.delivered_qty).as_("reserved_qty"),
        )
        .where(
            (so.docstatus == 1)
            & (so.status.notin(["Closed", "Completed"]))
            & (soi.stock_qty > soi.delivered_qty)
        )
        .groupby(soi.item_code)
    )

    if filters.get("company"):
        query = query.where(so.company == filters.get("company"))

    if filters.get("item_code"):
        query = query.where(soi.item_code == filters.get("item_code"))

    result = query.run(as_dict=True) or []

    # Build map: item_code -> reserved_qty
    reserved_map = {}
    for row in result:
        reserved_map[row.item_code] = flt(row.reserved_qty)

    return reserved_map


def calculate_free_stock(iwb_map, so_reserved_map, batch_map, float_precision):
    """
    Calculate free stock for each batch using FIFO (oldest manufacturing date first).
    Reserved qty from Sales Orders is allocated against oldest batches first.
    Reservation is at ITEM level (across all warehouses).
    Batches with negative balance are skipped.
    """
    for item_code in iwb_map:
        # Get reserved qty for this item (across all warehouses)
        reserved_qty = so_reserved_map.get(item_code, 0)

        # Get all batches for this item across ALL warehouses, sorted by manufacturing_date (oldest first)
        batches = []
        for warehouse in iwb_map[item_code]:
            for batch_no, qty_dict in iwb_map[item_code][warehouse].items():
                batch_details = batch_map.get(batch_no, {})
                mfg_date = batch_details.get("manufacturing_date") or batch_details.get("creation")
                batches.append({
                    "batch_no": batch_no,
                    "warehouse": warehouse,
                    "qty_dict": qty_dict,
                    "mfg_date": mfg_date,
                })

        # Sort by manufacturing date (oldest first), None dates go to end
        batches.sort(key=lambda x: (x["mfg_date"] is None, x["mfg_date"] or ""))

        remaining_reserved = flt(reserved_qty, float_precision)

        for batch in batches:
            qty_dict = batch["qty_dict"]
            bal_qty = flt(qty_dict.bal_qty, float_precision)

            # Skip negative balance batches
            if bal_qty <= 0:
                qty_dict.free_stock = 0
                continue

            if remaining_reserved >= bal_qty:
                # Entire batch is reserved
                qty_dict.free_stock = 0
                remaining_reserved -= bal_qty
            else:
                # Partial or no reservation
                qty_dict.free_stock = flt(bal_qty - remaining_reserved, float_precision)
                remaining_reserved = 0
