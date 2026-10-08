"""Silver CLI dependency order."""

from scripts.run_silver import ordered_jobs


def test_administrative_boundaries_always_precede_dengue_history():
    assert ordered_jobs(None) == ["administrative_boundaries", "dengue_history", "news"]
    assert ordered_jobs(["dengue_history"]) == [
        "administrative_boundaries", "dengue_history",
    ]
    assert ordered_jobs(["dengue_history", "administrative_boundaries"]) == [
        "administrative_boundaries", "dengue_history",
    ]
    assert ordered_jobs(["administrative_boundaries"]) == ["administrative_boundaries"]
    assert ordered_jobs(["news"]) == ["news"]
