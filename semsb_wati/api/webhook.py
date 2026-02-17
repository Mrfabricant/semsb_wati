"""
webhook.py
Full pipeline - receive, download, parse, route, create SO.
"""

import json
import frappe
from semsb_wati.api.wati_client import download_pdf, send_reply
from semsb_wati.api.pdf_parser import parse_so_pdf
from semsb_wati.api.routing import resolve_all_factories
from semsb_wati.api.so_creator import create_sales_orders


@frappe.whitelist(allow_guest=True)
def receive_wati_webhook():
	try:
		raw_body = frappe.request.data
		if not raw_body:
			return {"status": "error", "message": "Empty payload"}

		payload = json.loads(raw_body)

		event_type = payload.get("eventType", "")
		msg_type   = payload.get("type", "")
		wa_id      = payload.get("waId", "")
		message_id = payload.get("id", "")
		text       = payload.get("text", "") or ""
		data       = payload.get("data")

		# Save Webhook Log immediately
		log = frappe.get_doc({
			"doctype":         "Wati Webhook Log",
			"status":          "Received",
			"webhook_data":    json.dumps(payload, indent=2),
			"whatsapp_number": wa_id,
			"message_type":    msg_type,
			"wati_message_id": message_id,
		})
		log.insert(ignore_permissions=True)
		frappe.db.commit()

		# Filter: incoming messages only
		if event_type not in ("message", "messageReceived"):
			log.db_set("status", "Ignored")
			return {"status": "ignored", "reason": "event: " + event_type}

		# Filter: documents only
		if msg_type != "document":
			log.db_set("status", "Ignored")
			return {"status": "ignored", "reason": "not a document"}

		# Filter: PDF only
		is_pdf = (
			(isinstance(data, str) and ".pdf" in data.lower()) or
			text.lower().endswith(".pdf")
		)
		if not is_pdf:
			log.db_set("status", "Ignored")
			return {"status": "ignored", "reason": "not a PDF"}

		log.db_set("status", "Processing")

		# Download PDF
		pdf_bytes = download_pdf(data)
		filename = text if text.lower().endswith(".pdf") else "so_from_whatsapp.pdf"

		# Attach PDF to log
		file_doc = frappe.get_doc({
			"doctype":             "File",
			"file_name":           filename,
			"attached_to_doctype": "Wati Webhook Log",
			"attached_to_name":    log.name,
			"content":             pdf_bytes,
			"is_private":          1,
		})
		file_doc.flags.ignore_permissions = True
		file_doc.insert()
		frappe.db.commit()
		log.db_set("pdf_file", file_doc.file_url)

		# Parse PDF
		parsed = parse_so_pdf(pdf_bytes)

		if parsed.parse_errors:
			error_msg = ", ".join(parsed.parse_errors)
			log.db_set("status", "Error")
			log.db_set("error_log", "Parse errors: " + error_msg)
			frappe.db.commit()
			send_reply(wa_id, "Could not process your PDF. Issues: " + error_msg + ". Please contact the office.")
			return {"status": "error", "message": error_msg}

		# Factory routing
		parsed.items, routing_errors = resolve_all_factories(parsed.items)
		if routing_errors:
			log.db_set("error_log", "\n".join(routing_errors))
			frappe.db.commit()

		# Load settings
		settings = frappe.get_single("Wati Settings")

		# Test Mode
		if settings.test_mode:
			summary = "TEST MODE - Would create SOs for: " + ", ".join(parsed.so_numbers)
			summary += "\nCustomer: " + parsed.customer_raw
			summary += "\nLines: " + str(len(parsed.items))
			log.db_set("status", "Success")
			log.db_set("error_log", summary)
			frappe.db.commit()
			return {"status": "test_mode", "so_numbers": parsed.so_numbers}

		# Create Sales Orders
		try:
			created_sos = create_sales_orders(parsed, settings)
		except frappe.ValidationError as e:
			error_msg = str(e)
			log.db_set("status", "Error")
			log.db_set("error_log", error_msg)
			frappe.db.commit()
			send_reply(wa_id, "Sales Order could not be created. " + error_msg + ". Please contact the office.")
			return {"status": "error", "message": error_msg}

		so_names_str = ", ".join(created_sos)
		log.db_set("status", "Success")
		log.db_set("sales_orders_created", so_names_str)
		log.db_set("error_log", "")
		frappe.db.commit()

		# Notify sender
		if settings.notify_sender_on_success:
			template = settings.success_message_template or "Sales Order {so_name} received. Items: {item_count} lines | Delivery: {delivery_date}"
			msg = template.format(
				so_name=so_names_str,
				item_count=len(parsed.items),
				delivery_date=parsed.delivery_date,
			)
			send_reply(wa_id, msg)

		return {
			"status":     "success",
			"log":        log.name,
			"so_created": created_sos,
			"lines":      len(parsed.items),
		}

	except Exception:
		tb = frappe.get_traceback()
		frappe.log_error(tb, "WATI Webhook Error")
		try:
			log.db_set("status", "Error")
			log.db_set("error_log", tb)
			frappe.db.commit()
		except Exception:
			pass
		return {"status": "error", "message": "Server error"}