// Copyright (c) 2025, QCS and contributors
// For license information, please see license.txt

frappe.query_reports["Avientek Free Stock - Landing Cost"] = {
	filters: [
		{
			fieldname: "item_code",
			label: __("Item"),
			fieldtype: "Link",
			options: "Item",
			get_query() {
				
				return {
					query: "erpnext.controllers.queries.item_query",
				};
			},
		},
		{
			fieldname: "item_group",
			label: __("Item Group"),
			fieldtype: "Link",
			options: "Item Group",
		},
		{
			fieldname: "brand",
			label: __("Brand"),
			fieldtype: "Link",
			options: "Brand",
		},
	],
};
