"""
webhook.py
Receives incoming POST requests from WATI.io.

Real WATI payload for documents:
  "type": "document"
  "data": "https://live-mt-server.wati.io/.../file.pdf"  (string URL, not dict)
  "text": "filename.pdf"
"""

import json
import frappe


@frappe.whitelist(allow_guest=True)
def receive_wati_webhook():
	"""
	WATI calls this URL when a WhatsApp message is received.
	Must respond within 5 seconds — we just log and return 200.
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
		text       = payload.get("text", "")  # filename is in "text" for documents
		data       = payload.get("data")      # string URL for documents

		# ── Always save a Webhook Log record ──────────────────────────────
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
		# For real WATI messages: data = URL string, text = filename
		# Check filename in text field OR URL in data field
		is_pdf = False
		if isinstance(data, str) and ".pdf" in data.lower():
			is_pdf = True
		elif isinstance(text, str) and text.lower().endswith(".pdf"):
			is_pdf = True

		if not is_pdf:
			log.db_set("status", "Ignored")
			return {"status": "ignored", "reason": "not a PDF file"}

		# ── It is a PDF — mark as Processing ─────────────────────────────
		log.db_set("status", "Processing")

		return {"status": "queued", "log": log.name}

	except json.JSONDecodeError:
		frappe.log_error("Invalid JSON body received", "WATI Webhook Error")
		return {"status": "error", "message": "Invalid JSON"}

	except Exception:
		frappe.log_error(frappe.get_traceback(), "WATI Webhook Error")
		return {"status": "error", "message": "Server error"}