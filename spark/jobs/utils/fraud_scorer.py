HIGH_RISK_CATEGORIES = {
    "shopping_net", "misc_net", "grocery_pos",
    "shopping_pos", "misc_pos",
}
MEDIUM_RISK_CATEGORIES = {
    "entertainment", "gas_transport", "food_dining",
}

def compute_risk_score(amount: float, distance_km: float, category: str) -> float:
    if amount > 1000:
        amount_score = 1.0
    elif amount > 500:
        amount_score = 0.7
    elif amount > 200:
        amount_score = 0.4
    else:
        amount_score = 0.1

    if distance_km > 500:
        distance_score = 1.0
    elif distance_km > 200:
        distance_score = 0.7
    elif distance_km > 50:
        distance_score = 0.3
    else:
        distance_score = 0.0

    cat = (category or "").lower().strip()
    if cat in HIGH_RISK_CATEGORIES:
        category_score = 0.6
    elif cat in MEDIUM_RISK_CATEGORIES:
        category_score = 0.3
    else:
        category_score = 0.1

    score = (0.4 * amount_score) + (0.4 * distance_score) + (0.2 * category_score)
    return round(min(score, 1.0), 4)


def build_alert_reason(amount: float, distance_km: float, category: str) -> str:
    reasons = []
    if distance_km > 200:
        reasons.append(f"distance {distance_km:.0f}km from home")
    if amount > 500:
        reasons.append(f"high amount ${amount:.2f}")
    if (category or "").lower().strip() in HIGH_RISK_CATEGORIES:
        reasons.append(f"high-risk category: {category}")
    return " | ".join(reasons) if reasons else "rule-based flag"
