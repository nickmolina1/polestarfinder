from scraper.code_parser import (
    extract_option_codes,
    build_reverse_maps,
    classify_codes,
    enrich_labels,
)
from scraper.filters import filters as FILTERS


def test_code_parser_sample():
    sample_url = (
        "https://cas.polestar.com/image/dynamic/MY24_2335/534/summary-transparent-v1/FE/1/31/72900/"
        "R60000/LR01/_/default.png?market=us&angle=3&bg=00000000"
    )
    raw = extract_option_codes([sample_url])
    code_to_label, _ = build_reverse_maps(FILTERS)
    classified = classify_codes(raw, code_to_label)
    enriched = enrich_labels(classified, code_to_label)

    # Basic expectations
    assert "72900" in raw  # exterior code Magnesium
    assert enriched["exterior_label"] == "Magnesium"
    assert enriched["interior_label"] == "Charcoal Embossed Textile with 3D Etched deco"
    # Wheel parsing removed; no wheel_label expected
    assert "wheel_label" not in enriched or enriched["wheel_label"] is None
    assert enriched["motor_label"] in {
        "Long range Single motor ",
        "Long range Single motor - Rear Wheel Drive (RWD) ",
    }  # depends which code matched

    # Ensure raw_option_codes retained
    assert "raw_option_codes" in classified
