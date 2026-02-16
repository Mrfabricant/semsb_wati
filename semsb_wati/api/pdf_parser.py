"""
pdf_parser.py
Parses SEM105 "Outstanding Sales Order Listing" PDF.
"""

import re
import io
from dataclasses import dataclass, field
from typing import List, Optional

import pdfplumber
from dateutil import parser as date_parser


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


RE_SO_HEADER = re.compile(r"^(SO-\d+)\s+(.+)$", re.MULTILINE)
RE_LOCATION  = re.compile(r"\bAVINA\d{2,3}\b")
RE_DATE      = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b")
RE_SEQ_START = re.compile(r"^\d+\s+")

KNOWN_SUFFIXES = sorted([
	"BAG", "PKT", "BOX", "CTN", "PCS",
	"SET", "TIN", "BTL", "ROLL", "PAC",
	"PAK", "UNIT", "KG", "G",
], key=len, reverse=True)


class SEM105PDFParser:

	def parse(self, pdf_bytes: bytes) -> ParsedPDF:
		result = ParsedPDF()

		with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
			full_text = "\n".join(page.extract_text() or "" for page in pdf.pages)

		result.raw_text = full_text

		# ── SO numbers and customer ───────────────────────────────────────
		for so_no, customer in RE_SO_HEADER.findall(full_text):
			so_no = so_no.strip()
			customer = customer.strip()
			if re.match(r"^SO-\d+$", so_no) and so_no not in result.so_numbers:
				result.so_numbers.append(so_no)
				if not result.customer_raw:
					result.customer_raw = customer

		# Fallback: scan anywhere in text
		if not result.so_numbers:
			for so_no in re.findall(r"\bSO-\d+\b", full_text):
				if so_no not in result.so_numbers:
					result.so_numbers.append(so_no)

		if not result.so_numbers:
			result.parse_errors.append("No SO numbers found")

		# ── Line items ────────────────────────────────────────────────────
		result.items = self._parse_all_lines(full_text, result.so_numbers)

		if not result.items:
			result.parse_errors.append("No line items extracted")

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
			so_match = re.match(r"^(SO-\d+)\s+", line)
			if so_match:
				current_so = so_match.group(1)
				continue
			item = self._parse_item_line(line, current_so)
			if item:
				items.append(item)

		return items

	def _parse_item_line(self, line: str, current_so: str) -> Optional[ParsedLineItem]:
		if not RE_SEQ_START.match(line):
			return None
		loc_match = RE_LOCATION.search(line)
		if not loc_match:
			return None
		date_match = RE_DATE.search(line)
		if not date_match:
			return None

		try:
			parts = line.split()
			if len(parts) < 5:
				return None

			seq = int(parts[0])
			item_code = self._clean_item_code(parts[1])

			loc_code = loc_match.group()
			loc_idx = next((i for i, p in enumerate(parts) if p == loc_code), None)
			if loc_idx is None:
				return None

			delivery_date = self._parse_date(date_match.group(1))

			qty = 0.0
			for p in parts[loc_idx + 1:]:
				try:
					qty = float(p.replace(",", ""))
					break
				except ValueError:
					continue

			desc_parts = [
				p for p in parts[2:loc_idx]
				if not RE_DATE.fullmatch(p)
			]
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
		Fixes pdfplumber column-merging where item code gets joined
		with text from next column. e.g. "/PBUALGUT" → "/BAG"

		Uses three strategies:
		  1. Exact prefix match  (clean PDFs)
		  2. Anagram match       (letters present but scrambled)
		  3. Subsequence match   (letters present in order)
		"""
		slash_idx = raw.find("/")
		if slash_idx == -1:
			return raw

		prefix = raw[:slash_idx + 1]
		after_slash = raw[slash_idx + 1:].upper()

		# Strategy 1: exact prefix
		for suffix in KNOWN_SUFFIXES:
			if after_slash.startswith(suffix):
				return prefix + suffix

		# Strategy 2: anagram of first N chars
		for suffix in KNOWN_SUFFIXES:
			n = len(suffix)
			candidate = after_slash[:n]
			if len(candidate) == n and sorted(candidate) == sorted(suffix):
				return prefix + suffix

		# Strategy 3: all letters of suffix appear in order
		for suffix in KNOWN_SUFFIXES:
			remaining = after_slash
			found_all = True
			for ch in suffix:
				idx = remaining.find(ch)
				if idx == -1:
					found_all = False
					break
				remaining = remaining[idx + 1:]
			if found_all and len(suffix) >= 2:
				return prefix + suffix

		# Fallback: keep up to 5 alphanumeric chars
		m = re.match(r"[A-Z0-9]{1,5}", after_slash)
		return prefix + m.group() if m else raw

	def _parse_date(self, raw: str) -> str:
		try:
			return date_parser.parse(raw, dayfirst=True).strftime("%Y-%m-%d")
		except Exception:
			return raw


def parse_so_pdf(pdf_bytes: bytes) -> ParsedPDF:
	return SEM105PDFParser().parse(pdf_bytes)