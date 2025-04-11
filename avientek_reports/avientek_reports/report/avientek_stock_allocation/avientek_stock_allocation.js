// Copyright (c) 2025, QCS and contributors
// For license information, please see license.txt

frappe.query_reports["Avientek Stock Allocation"] = {
	"filters": [

		
        {
            "fieldname": "from_date",
            "label": __("From Date"),
            "fieldtype": "Date",
            // "default": frappe.datetime.add_days(frappe.datetime.get_today(), -30),
            
        },
        {
            "fieldname": "to_date",
            "label": __("To Date"),
            "fieldtype": "Date",
            // "default": frappe.datetime.get_today(),
        },
		{
            "fieldname": "company",
            "label": __("Company"),
            "fieldtype": "Link",
            "options": "Company",
        },
        {
            "fieldname": "part_number",
            "label": __("Part Number"),
            "fieldtype": "Link",
            "options": "Item",
        }
	]
};
