import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "jobs", "utils"))

from fraud_scorer import compute_risk_score, build_alert_reason
from geo_utils import haversine_km


class TestHaversine:
    def test_same_point_is_zero(self):
        assert haversine_km(30.0, 31.0, 30.0, 31.0) == 0.0

    def test_cairo_to_alexandria(self):
        dist = haversine_km(30.06, 31.24, 31.20, 29.92)
        assert 160 < dist < 195

    def test_returns_float(self):
        result = haversine_km(0.0, 0.0, 1.0, 1.0)
        assert isinstance(result, float)


class TestRiskScore:
    def test_high_amount_high_distance_is_high_risk(self):
        score = compute_risk_score(1500.0, 600.0, "shopping_net")
        assert score >= 0.8

    def test_small_local_grocery_is_low_risk(self):
        score = compute_risk_score(15.0, 5.0, "health_fitness")
        assert score < 0.3

    def test_score_bounded_0_to_1(self):
        for amt, dist, cat in [
            (9999, 9999, "shopping_net"),
            (0, 0, "health_fitness"),
            (500, 100, "entertainment"),
        ]:
            score = compute_risk_score(amt, dist, cat)
            assert 0.0 <= score <= 1.0

    def test_medium_risk_scenario(self):
        score = compute_risk_score(600.0, 250.0, "entertainment")
        assert 0.4 <= score <= 0.8


class TestAlertReason:
    def test_high_distance_appears_in_reason(self):
        reason = build_alert_reason(100.0, 500.0, "grocery_pos")
        assert "distance" in reason

    def test_high_amount_appears_in_reason(self):
        reason = build_alert_reason(800.0, 10.0, "health_fitness")
        assert "amount" in reason

    def test_high_risk_category_appears(self):
        reason = build_alert_reason(50.0, 10.0, "shopping_net")
        assert "shopping_net" in reason

    def test_no_signals_returns_default(self):
        reason = build_alert_reason(50.0, 10.0, "health_fitness")
        assert reason == "rule-based flag"
