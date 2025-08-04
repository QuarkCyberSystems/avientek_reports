# -*- coding: utf-8 -*-
#
#  Avientek – shared utilities
#

import json
import frappe
from frappe.utils.background_jobs import enqueue

# ----------------------------------------------------------------------
#  Prepared-Report auto-rebuild helper
# ----------------------------------------------------------------------

REPORT_NAME = "Avientek Stock Allocation"   # must match Report.title
DEFAULT_FILTERS = {}                        # same defaults used in UI


def rebuild_stock_allocation(login_manager=None, doc=None, method=None):
    """
    Delete any cached Prepared Report for this user and enqueue a new one.
    Can be called from  ⸺
      • the on_session_creation hook            (login_manager argument)
      • a User Permission doc_event             (doc argument)
    """

    # --------------------------------------------------------------
    #  1) work out which user needs the rebuild
    # --------------------------------------------------------------
    if login_manager:                     # called via on_session_creation
        user = login_manager.user
    elif doc and hasattr(doc, "user"):    # called via User Permission hook
        user = doc.user
    else:                                 # fallback (shouldn't happen)
        user = frappe.session.user

    # Ensure we run as Administrator to bypass owner filtering
    if frappe.session.user != "Administrator":
        frappe.set_user("Administrator")

    # --------------------------------------------------------------
    #  2) purge any existing prepared-report rows
    # --------------------------------------------------------------
    frappe.db.delete(
        "Prepared Report",
        {"report_name": REPORT_NAME, "owner": user},
    )

    # If another rebuild job is already queued, skip double-enqueue
    if frappe.db.exists(
        "Prepared Report",
        {
            "report_name": REPORT_NAME,
            "owner": user,
            "status": ("in", ["Queued", "Started"]),
        },
    ):
        return

    # --------------------------------------------------------------
    #  3) enqueue a fresh background job
    # --------------------------------------------------------------
    enqueue(
        "frappe.core.doctype.prepared_report.prepared_report.enqueue_prepared_report",
        report_name=REPORT_NAME,
        filters=json.dumps(DEFAULT_FILTERS),
        user=user,
        queue="default",             # same queue Frappe uses in UI
    )

    # Optional: log for troubleshooting
    frappe.logger().info(f"[PreparedReport] Rebuild queued for {user}")
