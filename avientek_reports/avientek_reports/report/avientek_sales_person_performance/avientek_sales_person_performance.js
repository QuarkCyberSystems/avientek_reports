// Copyright (c) 2025, QCS
// License: see license.txt

frappe.query_reports["Avientek Sales Person Performance"] = {
    filters: [
        {
            fieldname: "group_by",
            label: __("Group By"),
            fieldtype: "Select",
            options: ["Brand", "All Brands"],
            default: "Brand"
        },
        {
            fieldname: "brand",
            label: __("Brand"),
            fieldtype: "Link",
            options: "Brand",
            // hide the brand filter when “All Brands” is selected
            depends_on: "eval:doc.group_by == 'Brand'"
        },
        {
            fieldname: "country",
            label: __("Country"),
            fieldtype: "Link",
            options: "Territory"
        },
        {
            fieldname: "salesperson",
            label: __("Salesperson"),
            fieldtype: "Link",
            options: "Sales Person"
        },
        {
            fieldname: "from_date",
            label: __("From Date"),
            fieldtype: "Date"
        },
        {
            fieldname: "to_date",
            label: __("To Date"),
            fieldtype: "Date"
        }
    ]
};
