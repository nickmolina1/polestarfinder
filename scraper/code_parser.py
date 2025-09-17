"""Utility to parse option codes from Polestar stock image URLs and classify them.

Current scope is intentionally narrow: only exterior, interior, and motor codes
are identified from image URL path segments. Wheel and package inference proved
too noisy to treat as a reliable source of truth and has been removed.

Exposed functions:
- extract_option_codes(image_urls) -> set[str]
- classify_codes(codes) -> dict with keys: exterior_code, interior_code, motor_code, raw_option_codes (set)
- build_reverse_maps(filters_dict) -> (code_to_label, label_to_code)

Classification rules:
- Motor codes: EG, FE, FD, ED, ET
- Exterior: 5-digit numeric token that matches a known exterior code from filters
- Interior: explicit interior codes from filters (R60000, R6B000, etc, plus BST230)

All tokens are still returned in raw_option_codes to allow future correlation
work, but downstream logic should not attempt to infer wheels or packages from
these tokens without stronger validation.
"""

from __future__ import annotations

import re
from typing import Dict, Iterable, Set, Tuple

MOTOR_CODES = {"EG", "FE", "FD", "ED", "ET"}

# Accept tokens of interest: alphanumerics or digits, ignore placeholders like 'summary-transparent-v1'
TOKEN_SPLIT_RE = re.compile(r"[\\/]")
FILE_END_RE = re.compile(r"\.png$", re.IGNORECASE)

NUMERIC_5_RE = re.compile(r"^\d{5}$")
INTERIOR_RE = re.compile(r"^(R[A-Z0-9]{5}|BST230)$")  # includes special BST interior code


def extract_option_codes(image_urls: Iterable[str]) -> Set[str]:
    """Extract raw code tokens from image URL paths.

    Strategy: split path into segments, discard obvious non-code segments, collect candidate tokens.
    """
    codes: Set[str] = set()
    for url in image_urls:
        if not url or "//" not in url:
            continue
        try:
            path = url.split("//", 1)[1]
            # drop domain
            path = path.split("/", 1)[1]
        except IndexError:
            continue
        segments = TOKEN_SPLIT_RE.split(path)
        for seg in segments:
            if not seg or seg in {"_", "summary-transparent-v1", "summary-transparent-v2"}:
                continue
            if FILE_END_RE.search(seg):
                continue
            # Keep plausible segments; final classification later.
            # Avoid very long segments (like MY24_2335) unless they are codes we care about; skip those now.
            if len(seg) > 12:
                continue
            codes.add(seg)
    return codes


def build_reverse_maps(
    filters_dict: Dict[str, Dict[str, str]],
) -> Tuple[Dict[str, Tuple[str, str]], Dict[Tuple[str, str], str]]:
    """From the provided filters structure build reverse lookups.

    Returns:
      code_to_label: code -> (category, human_label)
      label_category_to_code: (category, human_label) -> code
    """
    code_to_label: Dict[str, Tuple[str, str]] = {}
    label_category_to_code: Dict[Tuple[str, str], str] = {}
    for label, mapping in filters_dict.items():
        for category, code in mapping.items():
            code_to_label[code] = (category, label)
            label_category_to_code[(category, label)] = code
    return code_to_label, label_category_to_code


def classify_codes(codes: Set[str], code_to_label: Dict[str, Tuple[str, str]]):
    """Classify a raw set of tokens into known attributes.

    Returns a dict with discovered codes + raw tokens for downstream heuristics.
    Preference order: first matching token wins per category.
    """
    exterior_code = None
    interior_code = None
    motor_code = None

    # Pre-filter raw tokens into categories we care about
    # We rely on reverse map for authoritative membership.
    for token in codes:
        if token in MOTOR_CODES and motor_code is None:
            motor_code = token
        elif NUMERIC_5_RE.match(token) and token in code_to_label and exterior_code is None:
            # token is 5-digit numeric and recognized
            cat, _ = code_to_label[token]
            if cat.lower() == "exterior":
                exterior_code = token
        elif INTERIOR_RE.match(token) and token in code_to_label and interior_code is None:
            cat, _ = code_to_label[token]
            if cat.lower() == "interior":
                interior_code = token
        # Wheel detection intentionally removed (not reliable from URL tokens)

    return {
        "exterior_code": exterior_code,
        "interior_code": interior_code,
        "motor_code": motor_code,
        "raw_option_codes": codes,
    }


def enrich_labels(classified: Dict[str, object], code_to_label: Dict[str, Tuple[str, str]]):
    """Return a new dict including human-readable labels alongside codes.

    For each *_code field present in classified, look up label from code_to_label where category matches.
    Fields added:
      exterior_label, interior_label, motor_label, wheel_label (if resolvable)
    """
    result = dict(classified)  # shallow copy

    def label_for(code: str | None, expected_category: str):
        if not code:
            return None
        cat_label = code_to_label.get(code)
        if not cat_label:
            return None
        cat, label = cat_label
        if cat.lower() == expected_category:
            return label
        return None

    result["exterior_label"] = label_for(result.get("exterior_code"), "exterior")
    result["interior_label"] = label_for(result.get("interior_code"), "interior")
    # Motor codes: in filters they appear under category keys (e.g., "Motor")
    result["motor_label"] = label_for(result.get("motor_code"), "motor") or label_for(
        result.get("motor_code"), "Motor"
    )
    # Wheel label enrichment removed
    return result


# Convenience: ephemeral test harness (manual run)
if __name__ == "__main__":
    from scraper.filters import filters as FILTERS  # type: ignore

    sample = [
        "https://cas.polestar.com/image/dynamic/MY24_2335/534/summary-transparent-v1/FE/1/31/72900/R60000/LR01/_/default.png?market=us&angle=3&bg=00000000"
    ]
    raw = extract_option_codes(sample)
    code_to_label, _ = build_reverse_maps(FILTERS)
    result = classify_codes(raw, code_to_label)
    enriched = enrich_labels(result, code_to_label)
    from pprint import pprint

    print("RAW:", raw)
    pprint(enriched)
