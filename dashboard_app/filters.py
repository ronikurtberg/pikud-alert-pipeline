"""Filter clause builders for dashboard queries."""

from flask import request


def build_filter_clause(prefix: str = "m") -> tuple[str, list]:
    """Build WHERE clause from query params: date_from, date_to, alert_type, zone, city."""
    clauses: list[str] = []
    params: list = []
    if request.args.get("date_from"):
        clauses.append(f"{prefix}.datetime_israel >= ?")
        params.append(request.args["date_from"])
    if request.args.get("date_to"):
        clauses.append(f"{prefix}.datetime_israel <= ?")
        params.append(request.args["date_to"] + " 23:59:59")
    if request.args.get("alert_type"):
        clauses.append(f"{prefix}.alert_type = ?")
        params.append(request.args["alert_type"])
    if request.args.get("message_type"):
        clauses.append(f"{prefix}.message_type = ?")
        params.append(request.args["message_type"])
    if request.args.get("city"):
        clauses.append(
            f"{prefix}.msg_id IN (SELECT ad.msg_id FROM alert_details ad JOIN cities c ON ad.city_id=c.city_id WHERE c.city_name=?)"
        )
        params.append(request.args["city"])
    if request.args.get("zone"):
        clauses.append(
            f"{prefix}.msg_id IN (SELECT ad.msg_id FROM alert_details ad JOIN zones z ON ad.zone_id=z.zone_id WHERE z.zone_name=?)"
        )
        params.append(request.args["zone"])
    return (" AND " + " AND ".join(clauses)) if clauses else "", params


def build_detail_filter_clause() -> tuple[str, list]:
    """Build WHERE clauses for alert_details-level queries."""
    clauses: list[str] = []
    params: list = []
    if request.args.get("city"):
        clauses.append("c.city_name = ?")
        params.append(request.args["city"])
    if request.args.get("zone"):
        clauses.append("z.zone_name = ?")
        params.append(request.args["zone"])
    return (" AND " + " AND ".join(clauses)) if clauses else "", params
