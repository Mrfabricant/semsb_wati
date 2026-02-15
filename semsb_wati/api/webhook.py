"""
webhook.py
Step 5: Receives webhook, downloads PDF, parses it, logs extracted data.
"""

import json
import frappe
from semsb_wati.api.wati_client import download_pdf
from semsb_wati.api.pdf_parser import parse_so_pdf


@frappe.whitelist(allow_guest=True)
def receive_wati_webhook():
	try:
		# ── Parse raw body ────────────────────────────────────────────────
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

		# ── Save Webhook Log immediately ──────────────────────────────────
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

		# ── Filter: incoming messages only ────────────────────────────────
		if event_type not in ("message", "messageReceived"):
			log.db_set("status", "Ignored")
			return {"status": "ignored", "reason": f"event: {event_type}"}

		# ── Filter: documents only ────────────────────────────────────────
		if msg_type != "document":
			log.db_set("status", "Ignored")
			return {"status": "ignored", "reason": f"not a document"}

		# ── Filter: PDF only ──────────────────────────────────────────────
		is_pdf = (
			(isinstance(data, str) and ".pdf" in data.lower()) or
			text.lower().endswith(".pdf")
		)
		if not is_pdf:
			log.db_set("status", "Ignored")
			return {"status": "ignored", "reason": "not a PDF"}

		log.db_set("status", "Processing")

		# ── Download PDF ──────────────────────────────────────────────────
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

		# ── Parse PDF ─────────────────────────────────────────────────────
		parsed = parse_so_pdf(pdf_bytes)

		# Build a summary to store in the log for review
		summary_lines = []
		summary_lines.append(f"SO Numbers: {', '.join(parsed.so_numbers)}")
		summary_lines.append(f"Customer: {parsed.customer_raw}")
		summary_lines.append(f"Delivery Date: {parsed.delivery_date}")
		summary_lines.append(f"Location: {parsed.location_code}")
		summary_lines.append(f"Lines extracted: {len(parsed.items)}")
		summary_lines.append("")

		for item in parsed.items:
			summary_lines.append(
				f"  [{item.source_so_no}] {item.item_code} | {item.description} | "
				f"Loc: {item.location_code} | Qty: {item.qty} | Del: {item.delivery_date}"
			)

		if parsed.parse_errors:
			summary_lines.append("")
			summary_lines.append("PARSE ERRORS:")
			summary_lines.extend(parsed.parse_errors)

		# Store parse result in error_log field for now (for review)
		log.db_set("error_log", "\n".join(summary_lines))
		log.db_set("status", "Success")
		frappe.db.commit()

		return {
			"status":        "success",
			"log":           log.name,
			"so_numbers":    parsed.so_numbers,
			"customer":      parsed.customer_raw,
			"lines_found":   len(parsed.items),
			"parse_errors":  parsed.parse_errors,
		}

	except Exception:
		frappe.log_error(frappe.get_traceback(), "WATI Webhook Error")
		try:
			log.db_set("status", "Error")
			log.db_set("error_log", frappe.get_traceback())
			frappe.db.commit()
		except Exception:
			pass
		return {"status": "error", "message": "Server error"}