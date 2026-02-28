from __future__ import annotations

import uuid
from typing import Optional
from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, Numeric, Text, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import UUID, JSONB, CITEXT

from .db import Base


class Domain(Base):
    __tablename__ = "domains"
    __table_args__ = (
        Index("domains_status_idx", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    domain: Mapped[str] = mapped_column(CITEXT, unique=True, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="new")
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    whois_checks: Mapped[list[WhoisCheck]] = relationship("WhoisCheck", back_populates="domain", cascade="all, delete-orphan")
    domaintools_checks: Mapped[list[DomainToolsCheck]] = relationship("DomainToolsCheck", back_populates="domain", cascade="all, delete-orphan")
    organizations: Mapped[list[Organization]] = relationship("Organization", back_populates="domain", cascade="all, delete-orphan")
    business_links: Mapped[list[BusinessDomainLink]] = relationship("BusinessDomainLink", back_populates="domain", cascade="all, delete-orphan")


class City(Base):
    __tablename__ = "cities"
    __table_args__ = (
        Index("cities_name_idx", "name"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    country: Mapped[Optional[str]] = mapped_column(Text)
    region: Mapped[Optional[str]] = mapped_column(Text)
    min_lat: Mapped[Optional[float]] = mapped_column(Numeric)
    min_lon: Mapped[Optional[float]] = mapped_column(Numeric)
    max_lat: Mapped[Optional[float]] = mapped_column(Numeric)
    max_lon: Mapped[Optional[float]] = mapped_column(Numeric)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    businesses: Mapped[list[Business]] = relationship("Business", back_populates="city", cascade="all, delete-orphan")


class WhoisCheck(Base):
    __tablename__ = "whois_checks"
    __table_args__ = (
        Index("whois_domain_idx", "domain_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    domain_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("domains.id", ondelete="CASCADE"), nullable=False)
    is_registered: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_parked: Mapped[Optional[bool]] = mapped_column(Boolean)
    has_a: Mapped[Optional[bool]] = mapped_column(Boolean)
    has_aaaa: Mapped[Optional[bool]] = mapped_column(Boolean)
    has_cname: Mapped[Optional[bool]] = mapped_column(Boolean)
    has_mx: Mapped[Optional[bool]] = mapped_column(Boolean)
    has_http: Mapped[Optional[bool]] = mapped_column(Boolean)
    http_status: Mapped[Optional[int]] = mapped_column(Integer)
    registrar: Mapped[Optional[str]] = mapped_column(Text)
    raw: Mapped[Optional[dict]] = mapped_column(JSONB)
    checked_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    domain: Mapped[Domain] = relationship("Domain", back_populates="whois_checks")


class DomainToolsCheck(Base):
    __tablename__ = "domaintools_checks"
    __table_args__ = (
        Index("domaintools_domain_idx", "domain_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    domain_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("domains.id", ondelete="CASCADE"), nullable=False)
    investor_flag: Mapped[Optional[bool]] = mapped_column(Boolean)
    score: Mapped[Optional[float]] = mapped_column(Numeric)
    raw: Mapped[Optional[dict]] = mapped_column(JSONB)
    checked_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    domain: Mapped[Domain] = relationship("Domain", back_populates="domaintools_checks")


class Organization(Base):
    __tablename__ = "organizations"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    domain_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("domains.id", ondelete="CASCADE"), nullable=False)
    name: Mapped[Optional[str]] = mapped_column(Text)
    raw: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    domain: Mapped[Domain] = relationship("Domain", back_populates="organizations")
    contacts: Mapped[list[Contact]] = relationship("Contact", back_populates="organization", cascade="all, delete-orphan")


class Contact(Base):
    __tablename__ = "contacts"
    __table_args__ = (
        UniqueConstraint("org_id", "email", name="contacts_org_email_uidx"),
        Index("contacts_email_idx", "email"),
        Index("contacts_lead_score_idx", "lead_score"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False)
    email: Mapped[Optional[str]] = mapped_column(CITEXT)
    first_name: Mapped[Optional[str]] = mapped_column(Text)
    last_name: Mapped[Optional[str]] = mapped_column(Text)
    title: Mapped[Optional[str]] = mapped_column(Text)
    source: Mapped[Optional[str]] = mapped_column(Text)
    confidence: Mapped[Optional[float]] = mapped_column(Numeric)
    lead_score: Mapped[Optional[float]] = mapped_column(Numeric)
    score_reasons: Mapped[Optional[dict]] = mapped_column(JSONB)
    scored_at: Mapped[Optional[DateTime]] = mapped_column(DateTime(timezone=True))
    raw: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    organization: Mapped[Organization] = relationship("Organization", back_populates="contacts")
    exports: Mapped[list[OutreachExport]] = relationship("OutreachExport", back_populates="contact", cascade="all, delete-orphan")


class OutreachExport(Base):
    __tablename__ = "outreach_exports"
    __table_args__ = (
        UniqueConstraint("contact_id", "platform", name="outreach_exports_contact_platform_uidx"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    contact_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("contacts.id", ondelete="CASCADE"), nullable=False)
    platform: Mapped[str] = mapped_column(Text, nullable=False)
    campaign_id: Mapped[Optional[str]] = mapped_column(Text)
    exported_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    status: Mapped[str] = mapped_column(Text, nullable=False, default="queued")
    raw: Mapped[Optional[dict]] = mapped_column(JSONB)

    contact: Mapped[Contact] = relationship("Contact", back_populates="exports")


class Business(Base):
    __tablename__ = "businesses"
    __table_args__ = (
        Index("businesses_website_score_idx", "website_url", "lead_score"),
        Index("businesses_name_score_idx", "name", "lead_score"),
        UniqueConstraint("source", "source_id", name="businesses_source_uidx"),
        Index("businesses_lead_score_idx", "lead_score"),
        Index("businesses_city_idx", "city_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    source_id: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[Optional[str]] = mapped_column(Text)
    category: Mapped[Optional[str]] = mapped_column(Text)
    website_url: Mapped[Optional[str]] = mapped_column(Text)
    address: Mapped[Optional[str]] = mapped_column(Text)
    lead_score: Mapped[Optional[float]] = mapped_column(Numeric)
    score_reasons: Mapped[Optional[dict]] = mapped_column(JSONB)
    scored_at: Mapped[Optional[DateTime]] = mapped_column(DateTime(timezone=True))
    lat: Mapped[Optional[float]] = mapped_column(Numeric)
    lon: Mapped[Optional[float]] = mapped_column(Numeric)
    raw: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    city_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("cities.id", ondelete="SET NULL"))
    city: Mapped[Optional[City]] = relationship("City", back_populates="businesses")
    contacts: Mapped[list[BusinessContact]] = relationship("BusinessContact", back_populates="business", cascade="all, delete-orphan")
    domain_links: Mapped[list[BusinessDomainLink]] = relationship("BusinessDomainLink", back_populates="business", cascade="all, delete-orphan")
    exports: Mapped[list[BusinessOutreachExport]] = relationship("BusinessOutreachExport", back_populates="business", cascade="all, delete-orphan")


class BusinessOutreachExport(Base):
    __tablename__ = "business_outreach_exports"
    __table_args__ = (
        UniqueConstraint("business_id", "platform", name="business_outreach_exports_business_platform_uidx"),
        Index("business_outreach_exports_platform_status_idx", "platform", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    business_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("businesses.id", ondelete="CASCADE"), nullable=False)
    platform: Mapped[str] = mapped_column(Text, nullable=False)
    campaign_id: Mapped[Optional[str]] = mapped_column(Text)
    exported_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    status: Mapped[str] = mapped_column(Text, nullable=False, default="queued")
    raw: Mapped[Optional[dict]] = mapped_column(JSONB)

    business: Mapped[Business] = relationship("Business", back_populates="exports")


class BusinessContact(Base):
    __tablename__ = "business_contacts"
    __table_args__ = (
        UniqueConstraint("business_id", "contact_type", "value", name="business_contacts_business_type_value_uidx"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    business_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("businesses.id", ondelete="CASCADE"), nullable=False)
    contact_type: Mapped[str] = mapped_column(Text, nullable=False)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    business: Mapped[Business] = relationship("Business", back_populates="contacts")


class BusinessDomainLink(Base):
    __tablename__ = "business_domain_links"
    __table_args__ = (
        UniqueConstraint("business_id", "domain_id", name="business_domain_links_business_domain_uidx"),
        Index("business_domain_links_domain_idx", "domain_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    business_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("businesses.id", ondelete="CASCADE"), nullable=False)
    domain_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("domains.id", ondelete="CASCADE"), nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    business: Mapped[Business] = relationship("Business", back_populates="domain_links")
    domain: Mapped[Domain] = relationship("Domain", back_populates="business_links")


class JobRun(Base):
    __tablename__ = "job_runs"
    __table_args__ = (
        Index("job_runs_name_status_idx", "job_name", "status"),
        Index("job_runs_started_at_idx", "started_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_name: Mapped[str] = mapped_column(Text, nullable=False)
    scope: Mapped[Optional[str]] = mapped_column(Text)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    finished_at: Mapped[Optional[DateTime]] = mapped_column(DateTime(timezone=True))
    processed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    details: Mapped[Optional[dict]] = mapped_column(JSONB)
    error: Mapped[Optional[str]] = mapped_column(Text)

    checkpoints: Mapped[list[JobCheckpoint]] = relationship("JobCheckpoint", back_populates="job_run", cascade="all, delete-orphan")


class JobCheckpoint(Base):
    __tablename__ = "job_checkpoints"
    __table_args__ = (
        UniqueConstraint("job_name", "scope", "checkpoint_key", name="job_checkpoints_unique_scope_key_uidx"),
        Index("job_checkpoints_name_scope_idx", "job_name", "scope"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_run_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("job_runs.id", ondelete="SET NULL"))
    job_name: Mapped[str] = mapped_column(Text, nullable=False)
    scope: Mapped[str] = mapped_column(Text, nullable=False, server_default="__global__")
    checkpoint_key: Mapped[str] = mapped_column(Text, nullable=False)
    checkpoint_value: Mapped[str] = mapped_column(Text, nullable=False)
    details: Mapped[Optional[dict]] = mapped_column(JSONB)
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    job_run: Mapped[Optional[JobRun]] = relationship("JobRun", back_populates="checkpoints")
