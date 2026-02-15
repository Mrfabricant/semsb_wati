"""
pdf_parser.py
Parses SEM105 "Outstanding Sales Order Listing" PDF.

Fixes applied:
  1. SO header regex now handles single space (pdfplumber collapses spaces)
  2. Item code regex stops at known suffixes (/BAG, /PKT, /G, -K etc.)
     to prevent merging with next column word
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
	source_so_no: str
	item_code: str
	description: str
	location_code: str
	qty: float
	delivery_date: str
	factory: str = ""


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

# SO header — single or multiple spaces after SO number
# e.g. "SO-35312 TRENDCELL SDN BHD - DC1"
RE_SO_HEADER = re.compile(r"^(SO-\d+)\s+(.+)$", re.MULTILINE)

# Location code
RE_LOCATION = re.compile(r"\bAVINA\d{2,3}\b")

# Date DD/MM/YY or DD/MM/YYYY
RE_DATE = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b")

# Item code: stops at known unit suffixes to avoid merging with next word
# Covers: /BAG, /PKT, /G, /KG, /BOX, /CTN, /PCS, -K, -25KG, etc.
# The item code ends when we hit a space followed by an uppercase description word
RE_ITEM_CODE = re.compile(
    r"^([A-Z][A-Z0-9\-]+(?:/"
    r"(?:BAG|PKT|BAG|KG|G|BOX|CTN|PCS|SET|TIN|BTL|BT|BTL|ROLL|PAC|PAK|UNIT)"
    r")?(?:-[A-Z0-9]+)?)"
)

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
			# Validate: SO number must be SO- followed by digits only
			if re.match(r"^SO-\d+$", so_no):
				if so_no not in result.so_numbers:
					result.so_numbers.append(so_no)
				if not result.customer_raw and customer:
					result.customer_raw = customer

		if not result.so_numbers:
			# Fallback: search anywhere in text for SO-XXXXX pattern
			fallback = re.findall(r"\bSO-\d+\b", full_text)
			for so_no in fallback:
				if so_no not in result.so_numbers:
					result.so_numbers.append(so_no)

		if not result.so_numbers:
			result.parse_errors.append("No SO numbers found")

		# ── Step 2: Parse line items ──────────────────────────────────────
		result.items = self._parse_all_lines(full_text, result.so_numbers)

		if not result.items:
			result.parse_errors.append("No line items extracted from PDF")

		# ── Step 3: Summary fields ────────────────────────────────────────
		if result.items:
			result.delivery_date = result.items[0].delivery_date
			result.location_code = result.items[0].location_code

		return result

	def _parse_all_lines(self, full_text: str, so_numbers: List[str]) -> List[ParsedLineItem]:
		items = []
		current_so = so_numbers[0] if so_numbers else ""

		for line in full_text.splitlines():
			line = line.strip()
			if not line:
				continue

			# Switch SO context when header line detected
			so_match = re.match(r"^(SO-\d+)\s+", line)
			if so_match:
				current_so = so_match.group(1)
				continue

			item = self._parse_item_line(line, current_so)
			if item:
				items.append(item)

		return items

	def _parse_item_line(self, line: str, current_so: str) -> Optional[ParsedLineItem]:
		# Must start with sequence number
		if not RE_SEQ_START.match(line):
			return None

		# Must have location code
		loc_match = RE_LOCATION.search(line)
		if not loc_match:
			return None

		# Must have date
		date_match = RE_DATE.search(line)
		if not date_match:
			return None

		try:
			parts = line.split()
			if len(parts) < 5:
				return None

			seq = int(parts[0])
			raw_item_token = parts[1]

			# ── Clean item code ───────────────────────────────────────────
			# pdfplumber sometimes merges item code with next word
			# e.g. "TCD029-20PKT/BAGTRENDCELL" → "TCD029-20PKT/BAG"
			# Strategy: known suffixes that end an item code
			item_code = self._clean_item_code(raw_item_token)

			# ── Location and its position ─────────────────────────────────
			loc_code = loc_match.group()
			loc_idx = next(
				(i for i, p in enumerate(parts) if p == loc_code),
				None
			)
			if loc_idx is None:
				return None

			# ── Delivery date ─────────────────────────────────────────────
			delivery_date = self._parse_date(date_match.group(1))

			# ── Qty: first numeric value after location ───────────────────
			qty = 0.0
			for p in parts[loc_idx + 1:]:
				try:
					val = float(p.replace(",", ""))
					qty = val
					break
				except ValueError:
					continue

			# ── Description: tokens between item code and location ────────
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

	def _clean_item_code(self, raw: str) -> str:
		"""
		Cleans merged item codes from pdfplumber column merging.
		e.g. "TCD029-20PKT/BAGTRENDCELL" → "TCD029-20PKT/BAG"
		e.g. "D221-THAI-25KG/PBUALGUT"   → "D221-THAI-25KG/BAG"

		Strategy: item codes end with known unit suffixes.
		After the suffix, any attached text is discarded.
		"""
		# Known unit suffixes that terminate an item code
		unit_suffixes = [
			"/BAG", "/PKT", "/KG", "/BOX", "/CTN",
			"/PCS", "/SET", "/TIN", "/BTL", "/ROLL",
			"/PAC", "/PAK", "/UNIT", "/G",
		]
		upper = raw.upper()
		for suffix in unit_suffixes:
			idx = upper.find(suffix)
			if idx != -1:
				# Keep everything up to and including the suffix
				return raw[:idx + len(suffix)]

		# No suffix found — check if ends with -K or similar short suffix
		# e.g. "TCS852-K" is valid as-is
		# Just return as-is if it looks clean
		return raw

	def _parse_date(self, raw: str) -> str:
		try:
			return date_parser.parse(raw, dayfirst=True).strftime("%Y-%m-%d")
		except Exception:
			return raw


# ─── Public function ──────────────────────────────────────────────────────────

def parse_so_pdf(pdf_bytes: bytes) -> ParsedPDF:
	return SEM105PDFParser().parse(pdf_bytes)