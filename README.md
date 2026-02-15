# SEMSB Wati Integration

WhatsApp PDF → ERPNext Sales Order automation for SRRI Easwari Mills SDN BHD.

## What it does

1. Receives WhatsApp PDF (Outstanding Sales Order Listing from SEM105)  
2. Parses SO numbers, customer, items, qty, location from PDF  
3. Maps location codes (AVINA14 etc.) to factory warehouses  
4. Auto-creates ERPNext Sales Orders  
5. Sends WhatsApp confirmation back to sender  

## Requirements

- ERPNext v15  
- Frappe v15  
- Python ≥ 3.10  

## Installation

```bash
bench get-app https://github.com/YOUR_ORG/semsb_wati
bench --site your.site install-app semsb_wati
bench --site your.site migrate
```

## Configuration

1. Go to **Wati Settings** and enter your WATI API endpoint and key  
2. Copy the generated **Webhook URL** into WATI dashboard  
3. Ensure **Factory Location Mapping** has all your AVINA codes mapped  
4. Uncheck **Test Mode** when ready to go live