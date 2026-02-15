"""
wati_client.py
Downloads PDF from WATI using the direct URL from the webhook payload.
Also handles sending WhatsApp reply messages.
"""

import requests
import frappe


def get_settings():
	"""Returns Wati Settings single doc."""
	return frappe.get_single("Wati Settings")


def download_pdf(pdf_url: str) -> bytes:
	"""
	Downloads PDF bytes directly from the WATI file URL.
	The URL comes from the "data" field in the webhook payload.
	e.g. https://live-mt-server.wati.io/1073791/api/file/showFile?fileName=...
	
	Requires Authorization header with WATI API key.
	"""
	settings = get_settings()
	api_key = settings.get_password("api_key")

	headers = {
		"Authorization": f"Bearer {api_key}",
	}

	response = requests.get(pdf_url, headers=headers, timeout=60)
	response.raise_for_status()
	return response.content


def send_reply(wa_id: str, message: str) -> bool:
	"""
	Sends a plain text reply back to a WhatsApp number via WATI.
	wa_id: phone number e.g. 923248768677
	"""
	try:
		settings = get_settings()
		api_key = settings.get_password("api_key")
		api_endpoint = (settings.wati_api_endpoint or "").rstrip("/")

		url = f"{api_endpoint}/api/v1/sendSessionMessage/{wa_id}"
		headers = {
			"Authorization": f"Bearer {api_key}",
			"Content-Type": "application/json",
		}
		response = requests.post(
			url,
			json={"messageText": message},
			headers=headers,
			timeout=15
		)
		response.raise_for_status()
		return True

	except Exception:
		frappe.log_error(frappe.get_traceback(), "WATI Send Reply Error")
		return False