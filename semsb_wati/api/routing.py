"""
routing.py
Maps location codes from PDF to ERPNext Warehouses
using the Factory Location Mapping DocType.
"""

import frappe
from typing import Optional


def get_factory_for_location(location_code: str) -> Optional[str]:
	"""
	Returns the Warehouse name for a given location code.
	e.g. "AVINA14" -> "Avina 14 - SEMSB"
	Returns None if not found.
	"""
	return frappe.db.get_value(
		"Factory Location Mapping",
		{"location_code": location_code, "active": 1},
		"factory",
	)


def resolve_all_factories(items: list) -> tuple:
	"""
	Sets .factory on each ParsedLineItem.
	Returns (items, list_of_error_strings)
	"""
	errors = []
	missing = set()

	for item in items:
		factory = get_factory_for_location(item.location_code)
		if factory:
			item.factory = factory
		else:
			item.factory = ""
			if item.location_code not in missing:
				errors.append(
					f"Location '{item.location_code}' not found in Factory Location Mapping"
				)
				missing.add(item.location_code)

	return items, errors