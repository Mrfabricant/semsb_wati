"""
pdf_parser.py
Parses the SEM105 "Outstanding Sales Order Listing" PDF.

Observed PDF structure:
  Header:     "SO-35312  TRENDCELL SDN BHD - DC1"
  Line items: "1  D221-THAI-25KG/BAG  PULUT HITAM THAI 25KG  AVINA14  4.00  12/01/26  4.00"

Key fields per line:
  - Seq number
  - Item code (e.g. D221-THAI-25KG/BAG)
  - Description (e.g. PULUT HITAM THAI 25KG)
  - Location code (e.g. AVINA14)
  - Qty
  - Delivery date (DD/MM/YY)
"""

import re
import io
from dataclasses import dataclass, field
from typing import List, Optional

import pdfplumber
from dateutil import parser as date_parser


# ─── Data Classes ─────────────────────────────────────────────────────────────

@dataclass
class ParsedLineItem:
	seq: int
	source_so_no: str       # e.g. "SO-35312"
	item_code: str          # e.g. "D221-THAI-25KG/BAG"
	description: str        # e.g. "PULUT HITAM THAI 25KG"
	location_code: str      # e.g. "AVINA14"
	qty: float
	delivery_date: str      # ISO: YYYY-MM-DD
	factory: str = ""       # filled by routing module


@dataclass
class ParsedPDF:
	so_numbers: List[str] = field(default_factory=list)
	customer_raw: str = ""
	delivery_date: str = ""
	location_code: str = ""
	items: List[ParsedLineItem] = field(default_factory=list)
	raw_text: str = ""
	parse_errors: List[str] = field(default_factory=list)


# ─── Regex Patterns ────────────────────────────────────────────────────────────

# SO header: "SO-35312  TRENDCELL SDN BHD - DC1"
RE_SO_HEADER = re.compile(r"^(SO-\d+)\s{2,}(.+)$", re.MULTILINE)

# Location code: AVINA + 2-3 digits
RE_LOCATION = re.compile(r"\bAVINA\d{2,3}\b")

# Date: DD/MM/YY or DD/MM/YYYY
RE_DATE = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b")

# Sequence number at start of line
RE_SEQ_START = re.compile(r"^\d+\s+")


# ─── Parser ───────────────────────────────────────────────────────────────────

class SEM105PDFParser:

	def parse(self, pdf_bytes: bytes) -> ParsedPDF:
		result = ParsedPDF()

		with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
			pages_text = []
			for page in pdf.pages:
				text = page.extract_text() or ""
				pages_text.append(text)
			full_text = "\n".join(pages_text)

		result.raw_text = full_text

		# ── Step 1: Find SO numbers and customer ──────────────────────────
		so_headers = RE_SO_HEADER.findall(full_text)
		for so_no, customer in so_headers:
			so_no = so_no.strip()
			customer = customer.strip()
			if so_no not in result.so_numbers:
				result.so_numbers.append(so_no)
			if not result.customer_raw and customer:
				result.customer_raw = customer

		if not result.so_numbers:
			result.parse_errors.append("No SO numbers found (expected format: SO-XXXXX)")

		# ── Step 2: Parse line items ──────────────────────────────────────
		result.items = self._parse_all_lines(full_text, result.so_numbers)

		if not result.items:
			result.parse_errors.append("No line items extracted from PDF")

		# ── Step 3: Set summary fields ────────────────────────────────────
		if result.items:
			result.delivery_date = result.items[0].delivery_date
			result.location_code = result.items[0].location_code

		return result

	def _parse_all_lines(self, full_text: str, so_numbers: List[str]) -> List[ParsedLineItem]:
		"""
		Walk through every line.
		Track which SO we are currently under.
		Extract item rows.
		"""
		items = []
		current_so = so_numbers[0] if so_numbers else ""

		for line in full_text.splitlines():
			line = line.strip()
			if not line:
				continue

			# Detect SO header line — switch current SO context
			so_match = re.match(r"^(SO-\d+)\s+", line)
			if so_match:
				current_so = so_match.group(1)
				continue

			# Try to parse as a line item
			item = self._parse_item_line(line, current_so)
			if item:
				items.append(item)

		return items

	def _parse_item_line(self, line: str, current_so: str) -> Optional[ParsedLineItem]:
		"""
		Parses one line as an item row.

		Expected (after pdfplumber extraction):
		  "1 D221-THAI-25KG/BAG PULUT HITAM THAI 25KG AVINA14 4.00 12/01/26 4.00"
		  "3 D411-25KG/BAG 12/01/26 BUBUR CHA CHA 25KG AVINA14 1.00 1.00"

		Note: date sometimes appears before description due to PDF column order.
		"""
		# Must start with a sequence number
		if not RE_SEQ_START.match(line):
			return None

		# Must contain a location code
		loc_match = RE_LOCATION.search(line)
		if not loc_match:
			return None

		# Must contain a date
		date_match = RE_DATE.search(line)
		if not date_match:
			return None

		try:
			parts = line.split()
			if len(parts) < 5:
				return None

			seq = int(parts[0])
			item_code = parts[1]

			# Location code and its index in parts
			loc_code = loc_match.group()
			loc_idx = next(i for i, p in enumerate(parts) if p == loc_code)

			# Parse delivery date
			delivery_date = self._parse_date(date_match.group(1))

			# Qty: first float value AFTER location code
			qty = 0.0
			for p in parts[loc_idx + 1:]:
				try:
					val = float(p.replace(",", ""))
					qty = val
					break
				except ValueError:
					continue

			# Description: tokens between item_code and location_code
			# Skip any date tokens that crept in
			desc_parts = []
			for p in parts[2:loc_idx]:
				if RE_DATE.fullmatch(p):
					continue
				desc_parts.append(p)
			description = " ".join(desc_parts).strip()

			if not item_code or qty == 0:
				return None

			return ParsedLineItem(
				seq=seq,
				source_so_no=current_so,
				item_code=item_code,
				description=description,
				location_code=loc_code,
				qty=qty,
				delivery_date=delivery_date,
			)

		except Exception:
			return None

	def _parse_date(self, raw: str) -> str:
		"""Converts DD/MM/YY or DD/MM/YYYY → YYYY-MM-DD."""
		try:
			return date_parser.parse(raw, dayfirst=True).strftime("%Y-%m-%d")
		except Exception:
			return raw


# ─── Public function ──────────────────────────────────────────────────────────

def parse_so_pdf(pdf_bytes: bytes) -> ParsedPDF:
	"""Call this from webhook.py to parse a PDF."""
	return SEM105PDFParser().parse(pdf_bytes)