// Copyright (c) 2025, QCS and contributors
// For license information, please see license.txt

frappe.query_reports["Avientek Sales Person Performance"] = {
	"filters": [
        {
            "fieldname": "brand",
            "label": __("Brand"),
            "fieldtype": "Link",
            "options": "Brand"
        },
        {
            "fieldname": "country",
            "label": __("Country"),
            "fieldtype": "Link",
            "options": "Territory"
        },
        {
            "fieldname": "from_date",
            "label": __("From Date"),
            "fieldtype": "Date",
            // "default": frappe.datetime.add_days(frappe.datetime.nowdate(), -30)
        },
        {
            "fieldname": "to_date",
            "label": __("To Date"),
            "fieldtype": "Date",
            // "default": frappe.datetime.nowdate()
        }
    ]
};
