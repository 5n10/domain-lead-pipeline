from __future__ import annotations

import csv
from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from domain_pipeline.models import (
    Business,
    BusinessContact,
    BusinessDomainLink,
    BusinessOutreachExport,
    Domain,
)
from domain_pipeline.workers.business_leads import export_business_leads


def utc_now():
    return datetime.now(timezone.utc)


def _make_business(db: Session, name: str, score: float, city_id):
    row = Business(
        source="osm",
        source_id=f"seed-{name.lower().replace(' ', '-')}",
        name=name,
        category="trades",
        website_url=None,
        lead_score=score,
        scored_at=utc_now(),
        city_id=city_id,
    )
    db.add(row)
    db.flush()
    return row


def test_business_leads_query_applies_eligibility_before_limit(client, db_session: Session, city):
    top_no_contact = _make_business(db_session, "Top No Contact", 100, city.id)
    top_hosted = _make_business(db_session, "Top Hosted", 95, city.id)
    eligible = _make_business(db_session, "Eligible Business", 90, city.id)

    db_session.add(BusinessContact(business_id=top_hosted.id, contact_type="phone", value="+971500000001"))
    db_session.add(BusinessContact(business_id=eligible.id, contact_type="phone", value="+971500000002"))

    hosted_domain = Domain(domain="top-hosted.example", status="hosted")
    db_session.add(hosted_domain)
    db_session.flush()
    db_session.add(BusinessDomainLink(business_id=top_hosted.id, domain_id=hosted_domain.id, source="website"))
    db_session.commit()

    response = client.get(
        "/api/leads/business",
        params={
            "min_score": 80,
            "require_contact": "true",
            "require_domain_qualification": "false",
            "require_unhosted_domain": "false",
            "limit": 1,
            "offset": 0,
        },
    )
    assert response.status_code == 200
    payload = response.json()

    assert payload["total_candidates"] == 1
    assert payload["returned"] == 1
    assert payload["items"][0]["name"] == "Eligible Business"
    assert payload["items"][0]["hosted_domains"] == []

    # Sanity: ineligible rows really exist above the eligible row by score.
    assert top_no_contact.lead_score > eligible.lead_score
    assert top_hosted.lead_score > eligible.lead_score


def test_export_business_leads_fills_limit_after_sql_filters(monkeypatch, tmp_path, db_session: Session, city):
    monkeypatch.setenv("EXPORT_DIR", str(tmp_path))

    _make_business(db_session, "Top No Contact", 100, city.id)
    eligible = _make_business(db_session, "Eligible Export", 90, city.id)
    db_session.add(BusinessContact(business_id=eligible.id, contact_type="phone", value="+971500000003"))
    db_session.commit()

    path = export_business_leads(
        platform="pytest_business",
        min_score=80,
        limit=1,
        require_contact=True,
        require_unhosted_domain=False,
        require_domain_qualification=False,
    )

    assert path is not None
    with path.open("r", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    assert len(rows) == 2
    assert rows[1][0] == "Eligible Export"

    exported_count = int(
        db_session.execute(
            select(func.count(BusinessOutreachExport.id)).where(BusinessOutreachExport.platform == "pytest_business")
        ).scalar()
        or 0
    )
    assert exported_count == 1


def test_mutation_endpoints_require_api_key_when_local_bypass_disabled(client, monkeypatch):
    monkeypatch.setenv("MUTATION_LOCALHOST_BYPASS", "false")
    monkeypatch.setenv("MUTATION_API_KEY", "test-secret")

    unauthorized = client.post("/api/actions/business-score", json={"limit": 0})
    assert unauthorized.status_code == 401

    authorized = client.post("/api/actions/business-score", json={"limit": 0}, headers={"X-API-Key": "test-secret"})
    assert authorized.status_code == 200
    assert authorized.json() == {"processed": 0}
