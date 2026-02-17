"""
pdf_parser.py
Parses SEM105 "Outstanding Sales Order Listing" PDF.

Handles two observed layouts:

Layout A (multi-SO per page):
  SO-35312  TRENDCELL SDN BHD - DC1
  1  D221-THAI-25KG/BAG  PULUT HITAM THAI 25KG  AVINA14  4.00  12/01/26  4.00

Layout B (single item, split lines):
  U02.5-UNCLE BOB TEPUNG UBI KAYU 5KG X 4PKT  AVINA15  1,000.00
  1  A999-01-20KG/BAG  1,000.00
  SO-35156  GOLDEN STAR NATURE S/B
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


RE_LOCATION  = re.compile(r"\bAVINA\d{2,3}\b")
RE_DATE      = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b")
RE_SEQ_START = re.compile(r"^\d+\s+")
RE_SO_LINE   = re.compile(r"\bSO-\d+\b")

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

		# Find all SO numbers anywhere in the text
		all_so = re.findall(r"\bSO-\d+\b", full_text)
		for so_no in all_so:
			if so_no not in result.so_numbers:
				result.so_numbers.append(so_no)

		# Find customer name — text after SO number on same line
		for line in full_text.splitlines():
			so_match = re.search(r"\bSO-\d+\b", line)
			if so_match and not result.customer_raw:
				after = line[so_match.end():].strip()
				if after:
					result.customer_raw = after
					break

		if not result.so_numbers:
			result.parse_errors.append("No SO numbers found")

		# Parse items using combined strategy
		result.items = self._parse_items(full_text, result.so_numbers)

		if not result.items:
			result.parse_errors.append("No line items extracted")

		if result.items:
			result.delivery_date = result.items[0].delivery_date
			result.location_code = result.items[0].location_code

		return result

	def _parse_items(self, full_text: str, so_numbers: List[str]) -> List[ParsedLineItem]:
		"""
		Two-pass strategy:
		Pass 1: Try Layout A (seq + item code + description + location on same line)
		Pass 2: Try Layout B (description + location on one line, seq + item code on next)
		"""
		items = self._parse_layout_a(full_text, so_numbers)
		if items:
			return items

		items = self._parse_layout_b(full_text, so_numbers)
		return items

	def _parse_layout_a(self, full_text: str, so_numbers: List[str]) -> List[ParsedLineItem]:
		"""
		Layout A: all fields on one line.
		e.g. "1 D221-THAI-25KG/BAG PULUT HITAM THAI 25KG AVINA14 4.00 12/01/26 4.00"
		"""
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

			item = self._try_parse_single_line(line, current_so)
			if item:
				items.append(item)

		return items

	def _parse_layout_b(self, full_text: str, so_numbers: List[str]) -> List[ParsedLineItem]:
		"""
		Layout B: description+location on one line, seq+item code on next line.

		Line N:   "U02.5-UNCLE BOB TEPUNG UBI KAYU 5KG X 4PKT  AVINA15  1,000.00"
		Line N+1: "1  A999-01-20KG/BAG  1,000.00"
		SO line:  "SO-35156  GOLDEN STAR NATURE S/B"  (may appear before or after)
		"""
		items = []
		lines = [l.strip() for l in full_text.splitlines()]
		current_so = so_numbers[0] if so_numbers else ""

		i = 0
		while i < len(lines):
			line = lines[i]

			# Track SO number context
			so_match = re.search(r"\b(SO-\d+)\b", line)
			if so_match:
				current_so = so_match.group(1)

			# Look for a line that has location code but NO seq number at start
			loc_match = RE_LOCATION.search(line)
			if loc_match and not RE_SEQ_START.match(line):
				# This could be the description+location line
				# Look ahead for the seq+item code line
				if i + 1 < len(lines):
					next_line = lines[i + 1]
					if RE_SEQ_START.match(next_line):
						item = self._try_parse_layout_b_pair(line, next_line, current_so)
						if item:
							items.append(item)
							i += 2
							continue
			i += 1

		return items

	def _try_parse_single_line(self, line: str, current_so: str) -> Optional[ParsedLineItem]:
		"""Parse Layout A: single line with all fields."""
		if not RE_SEQ_START.match(line):
			return None
		loc_match = RE_LOCATION.search(line)
		if not loc_match:
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

			date_match = RE_DATE.search(line)
			delivery_date = self._parse_date(date_match.group(1)) if date_match else ""

			qty = 0.0
			for p in parts[loc_idx + 1:]:
				try:
					qty = float(p.replace(",", ""))
					break
				except ValueError:
					continue

			desc_parts = [p for p in parts[2:loc_idx] if not RE_DATE.fullmatch(p)]
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

	def _try_parse_layout_b_pair(self, desc_line: str, seq_line: str, current_so: str) -> Optional[ParsedLineItem]:
		"""
		Parse Layout B pair:
		desc_line: "U02.5-UNCLE BOB TEPUNG UBI KAYU 5KG X 4PKT  AVINA15  1,000.00"
		seq_line:  "1  A999-01-20KG/BAG  1,000.00"
		"""
		try:
			# From desc_line: get location, description, delivery date, qty
			loc_match = RE_LOCATION.search(desc_line)
			if not loc_match:
				return None

			loc_code = loc_match.group()
			desc_parts_raw = desc_line[:loc_match.start()].strip()
			after_loc = desc_line[loc_match.end():].strip()

			# Get delivery date from desc_line if present
			date_match = RE_DATE.search(desc_line)
			delivery_date = self._parse_date(date_match.group(1)) if date_match else ""

			# Description is everything before the location code
			description = desc_parts_raw.strip()

			# From seq_line: get seq number, item code, qty
			seq_parts = seq_line.split()
			if len(seq_parts) < 2:
				return None

			seq = int(seq_parts[0])
			item_code = self._clean_item_code(seq_parts[1])

			# Qty: from desc_line after location, or from seq_line
			qty = 0.0
			for p in after_loc.split():
				try:
					qty = float(p.replace(",", ""))
					break
				except ValueError:
					continue

			if qty == 0:
				for p in seq_parts[2:]:
					try:
						qty = float(p.replace(",", ""))
						break
					except ValueError:
						continue

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
		"""Fix pdfplumber column merging e.g. /PBUALGUT -> /BAG"""
		slash_idx = raw.find("/")
		if slash_idx == -1:
			return raw

		prefix = raw[:slash_idx + 1]
		after_slash = raw[slash_idx + 1:].upper()

		# Exact prefix match
		for suffix in KNOWN_SUFFIXES:
			if after_slash.startswith(suffix):
				return prefix + suffix

		# Anagram match
		for suffix in KNOWN_SUFFIXES:
			n = len(suffix)
			candidate = after_slash[:n]
			if len(candidate) == n and sorted(candidate) == sorted(suffix):
				return prefix + suffix

		# Subsequence match
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

		m = re.match(r"[A-Z0-9]{1,5}", after_slash)
		return prefix + m.group() if m else raw

	def _parse_date(self, raw: str) -> str:
		try:
			return date_parser.parse(raw, dayfirst=True).strftime("%Y-%m-%d")
		except Exception:
			return raw


def parse_so_pdf(pdf_bytes: bytes) -> ParsedPDF:
	return SEM105PDFParser().parse(pdf_bytes)