import json
from types import SimpleNamespace

import scraper.scraper as scraper


class DummyResp:
    def __init__(self, payload_json):
        self._payload_json = payload_json
        self.status_code = 200
        self.headers = {}
        self.content = json.dumps(payload_json).encode()
        self.text = json.dumps(payload_json)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError("bad status")


# Capture posted payloads
posted = []


def dummy_post(url, json=None, timeout=0):  # noqa: A002 (shadowing built-in ok for test)
    posted.append(json)
    # Return a minimal successful GraphQL-like response with no results
    return DummyResp(
        {
            "data": {
                "searchVehicleAds": {
                    "metadata": {
                        "limit": json["variables"]["limit"],
                        "offset": json["variables"]["offset"],
                        "resultCount": 0,
                        "totalCount": 0,
                    },
                    "vehicleAds": [],
                }
            }
        }
    )


def test_fetch_ids_for_filter_builds_expected_payload(monkeypatch):
    # Monkeypatch the session factory to return an object with .post = dummy_post
    class DummySession:
        def __init__(self):
            self.headers = {}

        def post(self, *a, **kw):
            return dummy_post(*a, **kw)

    monkeypatch.setattr(scraper, "_session", lambda: DummySession())

    ids = scraper.fetch_ids_for_filter(
        "Wheels",
        "R184",
        model=scraper.DEFAULT_MODELS[0],
        market=scraper.DEFAULT_MARKET,
        page_limit=50,
    )
    assert ids == set(), "Expected empty set for dummy response"
    assert posted, "Expected at least one payload to be posted"

    payload = posted[0]
    vars_ = payload["variables"]
    assert vars_["limit"] == 50
    assert vars_["offset"] == 0
    # Ensure equalFilters contains only our target filter
    eq = vars_["equalFilters"]
    assert len(eq) == 1 and eq[0]["filterType"] == "Wheels" and eq[0]["value"] == "R184"
    # Ensure default exclude for New cycle state applied
    ex = vars_["excludeFilters"]
    assert any(f.get("filterType") == "CycleState" and f.get("value") == "New" for f in ex)
