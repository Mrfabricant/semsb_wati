"""
so_creator.py
Creates ERPNext Sales Orders from parsed PDF data.

Rules:
- One Sales Order per unique SO number found in the PDF
- Customer matched by name, auto-created if not found
- Warehouse = factory from location mapping
- Items skipped if not found in ERPNext Item master (logged)
"""

import frappe


def create_sales_orders(parsed_pdf, settings) -> list:
	"""
	Creates one Sales Order per unique SO number.
	Returns list of created SO names.
	"""
	created_sos = []

	# Group items by their source SO number
	so_groups = {}
	for item in parsed_pdf.items:
		so_no = item.source_so_no or (parsed_pdf.so_numbers[0] if parsed_pdf.so_numbers else "UNKNOWN")
		if so_no not in so_groups:
			so_groups[so_no] = []
		so_groups[so_no].append(item)

	for source_so_no, items in so_groups.items():
		so_name = _create_single_so(
			source_so_no=source_so_no,
			customer_raw=parsed_pdf.customer_raw,
			items=items,
			settings=settings,
		)
		created_sos.append(so_name)

	return created_sos


def _create_single_so(source_so_no, customer_raw, items, settings) -> str:
	"""Creates one ERPNext Sales Order."""

	# ── 1. Resolve customer ───────────────────────────────────────────────
	customer = _get_or_create_customer(customer_raw)

	# ── 2. Earliest delivery date from items ──────────────────────────────
	delivery_dates = [i.delivery_date for i in items if i.delivery_date]
	delivery_date = min(delivery_dates) if delivery_dates else frappe.utils.today()

	# ── 3. Build SO line items ────────────────────────────────────────────
	so_items = []
	skipped = []

	for item in items:
		# Check item exists in ERPNext
		if not frappe.db.exists("Item", item.item_code):
			skipped.append(item.item_code)
			frappe.log_error(
				f"Item '{item.item_code}' not found in ERPNext Item master. "
				f"Skipping for SO {source_so_no}.",
				"WATI SO Creation - Item Not Found"
			)
			continue

		so_items.append({
			"item_code":     item.item_code,
			"qty":           item.qty,
			"delivery_date": item.delivery_date or delivery_date,
			"warehouse":     item.factory,
		})

	if not so_items:
		frappe.throw(
			f"No valid items found for SO {source_so_no}. "
			f"Skipped items: {', '.join(skipped)}"
		)

	# ── 4. Get company currency ───────────────────────────────────────────
	currency = frappe.db.get_value(
		"Company", settings.default_company, "default_currency"
	) or "MYR"

	# ── 5. Create Sales Order ─────────────────────────────────────────────
	so = frappe.get_doc({
		"doctype":       "Sales Order",
		"company":       settings.default_company,
		"customer":      customer,
		"po_no":         source_so_no,       # customer's original SO ref
		"po_date":       frappe.utils.today(),
		"delivery_date": delivery_date,
		"order_type":    "Sales",
		"currency":      currency,
		"items":         so_items,
	})

	so.flags.ignore_permissions = True
	so.insert()

	# Auto-submit if configured in Wati Settings
	if settings.auto_submit_sales_orders:
		so.submit()

	frappe.db.commit()
	return so.name


def _get_or_create_customer(customer_raw: str) -> str:
	"""
	Finds existing ERPNext Customer or creates a new one.
	customer_raw: e.g. "TRENDCELL SDN BHD - DC1"
	"""
	# Exact match
	if frappe.db.exists("Customer", customer_raw):
		return customer_raw

	# Partial match on customer_name field
	match = frappe.db.get_value(
		"Customer",
		{"customer_name": customer_raw},
		"name"
	)
	if match:
		return match

	# Create new customer
	cust = frappe.get_doc({
		"doctype":        "Customer",
		"customer_name":  customer_raw,
		"customer_type":  "Company",
		"customer_group": frappe.db.get_single_value(
			"Selling Settings", "customer_group"
		) or "All Customer Groups",
		"territory": frappe.db.get_single_value(
			"Selling Settings", "territory"
		) or "All Territories",
	})
	cust.flags.ignore_permissions = True
	cust.insert()
	frappe.db.commit()
	return cust.name