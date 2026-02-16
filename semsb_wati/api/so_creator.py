"""
so_creator.py
Creates ERPNext Sales Orders from parsed PDF data.
"""

import frappe
from frappe.utils import getdate, today, add_days


def create_sales_orders(parsed_pdf, settings) -> list:
	"""
	Creates one Sales Order per unique SO number.
	Returns list of created SO names.
	"""
	created_sos = []

	# Group items by source SO number
	so_groups = {}
	for item in parsed_pdf.items:
		so_no = item.source_so_no or (
			parsed_pdf.so_numbers[0] if parsed_pdf.so_numbers else "UNKNOWN"
		)
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


def _sanitize_delivery_date(date_str: str) -> str:
	"""
	Ensures delivery date is not in the past.
	ERPNext rejects SO if delivery date < today.
	If date is in the past, use today + 1 day.
	"""
	try:
		parsed_date = getdate(date_str)
		today_date = getdate(today())
		if parsed_date < today_date:
			return add_days(today(), 1)
		return date_str
	except Exception:
		return add_days(today(), 1)


def _create_single_so(source_so_no, customer_raw, items, settings) -> str:
	"""Creates one ERPNext Sales Order."""

	# ── 1. Resolve customer ───────────────────────────────────────────────
	customer = _get_or_create_customer(customer_raw)

	# ── 2. Delivery date — must not be in the past ────────────────────────
	delivery_dates = [i.delivery_date for i in items if i.delivery_date]
	raw_delivery = min(delivery_dates) if delivery_dates else today()
	delivery_date = _sanitize_delivery_date(raw_delivery)

	# ── 3. Build SO line items ────────────────────────────────────────────
	so_items = []
	skipped = []

	for item in items:
		if not frappe.db.exists("Item", item.item_code):
			skipped.append(item.item_code)
			frappe.log_error(
				f"Item '{item.item_code}' not found in ERPNext. Skipping.",
				"WATI SO - Item Not Found"
			)
			continue

		item_delivery = _sanitize_delivery_date(item.delivery_date or raw_delivery)

		so_items.append({
			"item_code":     item.item_code,
			"qty":           item.qty,
			"delivery_date": item_delivery,
			"warehouse":     item.factory,
		})

	if not so_items:
		frappe.throw(
			f"No valid items for SO {source_so_no}. "
			f"Missing items: {', '.join(skipped)}"
		)

	# ── 4. Company currency ───────────────────────────────────────────────
	currency = frappe.db.get_value(
		"Company", settings.default_company, "default_currency"
	) or "MYR"

	# ── 5. Create Sales Order ─────────────────────────────────────────────
	so = frappe.get_doc({
		"doctype":       "Sales Order",
		"company":       settings.default_company,
		"customer":      customer,
		"po_no":         source_so_no,
		"po_date":       today(),
		"delivery_date": delivery_date,
		"order_type":    "Sales",
		"currency":      currency,
		"items":         so_items,
	})

	so.flags.ignore_permissions = True
	so.insert()

	if settings.auto_submit_sales_orders:
		so.submit()

	frappe.db.commit()
	return so.name


def _get_or_create_customer(customer_raw: str) -> str:
	"""Finds or creates ERPNext Customer."""

	# Exact name match
	if frappe.db.exists("Customer", customer_raw):
		return customer_raw

	# Match on customer_name field
	match = frappe.db.get_value(
		"Customer",
		{"customer_name": customer_raw},
		"name"
	)
	if match:
		return match

	# Create new
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