"""
webhook.py
Receives incoming POST requests from WATI.io.
Step 3: Just receives, logs, and returns 200.
No PDF processing yet.
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

		# ── Extract key fields from payload ───────────────────────────────
		event_type  = payload.get("eventType", "")
		msg_type    = payload.get("type", "")
		wa_id       = payload.get("waId", "")
		sender      = payload.get("senderName", "")
		message_id  = payload.get("id", "")
		data_block  = payload.get("data") or {}
		mime_type   = data_block.get("mimeType", "")

		# ── Always save a Webhook Log record ──────────────────────────────
		log = frappe.get_doc({
			"doctype":          "Wati Webhook Log",
			"status":           "Received",
			"webhook_data":     json.dumps(payload, indent=2),
			"whatsapp_number":  wa_id,
			"message_type":     msg_type,
			"wati_message_id":  message_id,
		})
		log.insert(ignore_permissions=True)
		frappe.db.commit()

		# ── Filter: only care about incoming PDF documents ────────────────
		if event_type != "messageReceived":
			log.db_set("status", "Ignored")
			return {"status": "ignored", "reason": f"event: {event_type}"}

		if msg_type != "document" or "pdf" not in mime_type.lower():
			log.db_set("status", "Ignored")
			return {"status": "ignored", "reason": f"not a PDF (type={msg_type}, mime={mime_type})"}

		# ── It is a PDF — mark as Processing (PDF handling comes next step)
		log.db_set("status", "Processing")

		return {"status": "queued", "log": log.name}

	except json.JSONDecodeError:
		frappe.log_error("Invalid JSON body received", "WATI Webhook Error")
		return {"status": "error", "message": "Invalid JSON"}

	except Exception:
		frappe.log_error(frappe.get_traceback(), "WATI Webhook Error")
		return {"status": "error", "message": "Server error"}