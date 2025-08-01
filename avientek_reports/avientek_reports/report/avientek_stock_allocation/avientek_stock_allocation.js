// Copyright (c) 2025, QCS
// For license information, please see licence.txt

frappe.query_reports["Avientek Stock Allocation"] = {
    filters: [
        {
            fieldname: "from_date",
            label: __("From Date"),
            fieldtype: "Date"
        },
        {
            fieldname: "to_date",
            label: __("To Date"),
            fieldtype: "Date"
        },
        {
            fieldname: "company",
            label: __("Company"),
            fieldtype: "Link",
            options: "Company"
        },
        {
            fieldname: "item_code",
            label: __("Item Code"),
            fieldtype: "Link",
            options: "Item"
        },
        {
            fieldname: "sales_person",
            label: __("Sales Person"),
            fieldtype: "Link",
            options: "Sales Person"            
        },
        {
            fieldname: "customer",
            label: __("Customer"),
            fieldtype: "Link",
            options: "Customer"
        },
        {
            fieldname: "customer_name",
            label: __("Customer Name"),
            fieldtype: "Data"
        },
        {
            fieldname: "parent_sales_person",
            label: __("Parent Sales Person"),
            fieldtype: "Link",
            options: "Sales Person"
        }
    ]
};
