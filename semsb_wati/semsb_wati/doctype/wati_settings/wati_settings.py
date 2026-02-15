import frappe
from frappe.model.document import Document


class WatiSettings(Document):

	def before_save(self):
		site_url = frappe.utils.get_url()
		self.webhook_url = f"{site_url}/api/method/semsb_wati.api.webhook.receive_wati_webhook"