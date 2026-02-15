"""
webhook.py
Receives incoming POST requests from WATI.io.
Step 4: Receives, logs, downloads PDF, attaches to log.

Real WATI payload for documents:
  "type": "document"
  "data": "https://live-mt-server.wati.io/.../file.pdf"  (direct URL string)
  "text": "filename.pdf"
"""

import json
import frappe
from semsb_wati.api.wati_client import download_pdf


@frappe.whitelist(allow_guest=True)
def receive_wati_webhook():
	"""
	WATI calls this URL when a WhatsApp message is received.
	Must respond within 5 seconds.
	"""
	try:
		# ── Parse the raw JSON body from WATI ─────────────────────────────
		raw_body = frappe.request.data
		if not raw_body:
			return {"status": "error", "message": "Empty payload"}

		payload = json.loads(raw_body)

		# ── Extract key fields ────────────────────────────────────────────
		event_type = payload.get("eventType", "")
		msg_type   = payload.get("type", "")
		wa_id      = payload.get("waId", "")
		message_id = payload.get("id", "")
		text       = payload.get("text", "") or ""
		data       = payload.get("data")

		# ── Always save a Webhook Log record first ────────────────────────
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

		# ── Filter: only incoming messages ────────────────────────────────
		valid_events = ("message", "messageReceived")
		if event_type not in valid_events:
			log.db_set("status", "Ignored")
			return {"status": "ignored", "reason": f"event: {event_type}"}

		# ── Filter: only document type ────────────────────────────────────
		if msg_type != "document":
			log.db_set("status", "Ignored")
			return {"status": "ignored", "reason": f"not a document (type={msg_type})"}

		# ── Filter: confirm it is a PDF ───────────────────────────────────
		is_pdf = False
		if isinstance(data, str) and ".pdf" in data.lower():
			is_pdf = True
		elif text.lower().endswith(".pdf"):
			is_pdf = True

		if not is_pdf:
			log.db_set("status", "Ignored")
			return {"status": "ignored", "reason": "not a PDF file"}

		# ── It is a PDF — download and attach ────────────────────────────
		log.db_set("status", "Processing")

		try:
			# Download PDF bytes from WATI
			pdf_bytes = download_pdf(data)

			# Determine filename
			filename = text if text.lower().endswith(".pdf") else "so_from_whatsapp.pdf"

			# Save as Frappe File attachment
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

			# Link the file URL to the log and mark Success
			log.db_set("pdf_file", file_doc.file_url)
			log.db_set("status", "Success")    # ← mark as done
			frappe.db.commit()

			return {
				"status":  "success",
				"log":     log.name,
				"file":    file_doc.file_url,
				"message": f"PDF '{filename}' downloaded and attached successfully"
			}

		except Exception:
			frappe.log_error(frappe.get_traceback(), "WATI PDF Download Error")
			log.db_set("status", "Error")
			log.db_set("error_log", frappe.get_traceback())
			frappe.db.commit()
			return {"status": "error", "message": "PDF download failed", "log": log.name}

	except json.JSONDecodeError:
		frappe.log_error("Invalid JSON body received", "WATI Webhook Error")
		return {"status": "error", "message": "Invalid JSON"}

	except Exception:
		frappe.log_error(frappe.get_traceback(), "WATI Webhook Error")
		return {"status": "error", "message": "Server error"}