"""
production_planner.py
Creates ERPNext Production Plan from a Sales Order.

Scope:
  - One Production Plan per Sales Order
  - Items pulled directly from SO lines
  - No BOM required at this stage
  - Work Orders created manually later by factory team

ERPNext Production Plan structure:
  - sales_orders child table: links to the source SO
  - po_items child table: items to produce with qty and warehouse
"""

import frappe
from frappe.utils import today


def create_production_plan_for_so(so_name: str) -> str:
	"""
	Creates a Production Plan for a given Sales Order name.
	Returns the Production Plan name.
	"""
	so = frappe.get_doc("Sales Order", so_name)

	# ── Check if Production Plan already exists for this SO ───────────────
	existing = frappe.db.get_value(
		"Production Plan Sales Order",
		{"sales_order": so_name},
		"parent"
	)
	if existing:
		frappe.log_error(
			f"Production Plan already exists for SO {so_name}: {existing}",
			"WATI Production Plan - Duplicate"
		)
		return existing

	# ── Build Production Plan items from SO lines ─────────────────────────
	pp_items = []
	for item in so.items:
		# Skip if item is not a stock item (service items etc.)
		is_stock_item = frappe.db.get_value("Item", item.item_code, "is_stock_item")
		if not is_stock_item:
			continue

		pp_items.append({
			"item_code":          item.item_code,
			"item_name":          item.item_name,
			"qty":                item.qty,
			"planned_start_date": today(),
			"sales_order":        so_name,
			"sales_order_item":   item.name,
			"warehouse":          item.warehouse,
		})

	if not pp_items:
		frappe.log_error(
			f"No stock items found in SO {so_name} to plan production for.",
			"WATI Production Plan - No Items"
		)
		return ""

	# ── Create Production Plan ────────────────────────────────────────────
	pp = frappe.get_doc({
		"doctype":              "Production Plan",
		"company":              so.company,
		"posting_date":         today(),
		"get_items_from":       "Sales Order",
		"status":               "Draft",
		# Link to source SO
		"sales_orders": [
			{
				"sales_order":      so_name,
				"sales_order_date": so.transaction_date,
				"customer":         so.customer,
				"grand_total":      so.grand_total,
			}
		],
		# Items to produce
		"po_items": pp_items,
	})

	pp.flags.ignore_permissions = True
	pp.insert()
	frappe.db.commit()

	return pp.name


def create_production_plans_for_sos(so_names: list) -> dict:
	"""
	Creates Production Plans for a list of SO names.
	Returns dict: {so_name: production_plan_name}
	"""
	results = {}
	for so_name in so_names:
		try:
			pp_name = create_production_plan_for_so(so_name)
			results[so_name] = pp_name
		except Exception:
			frappe.log_error(
				frappe.get_traceback(),
				f"WATI Production Plan - Failed for {so_name}"
			)
			results[so_name] = None
	return results