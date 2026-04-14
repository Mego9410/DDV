from __future__ import annotations

from datetime import date, datetime
from typing import Any, Optional
from uuid import UUID, uuid4

from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel


class Practice(SQLModel, table=True):
    __tablename__ = "practices"

    id: UUID = Field(default_factory=uuid4, primary_key=True, index=True)

    # Best-effort identity keys (can be corrected later via review UI)
    display_name: str = Field(index=True)
    address_text: Optional[str] = None
    postcode: Optional[str] = Field(default=None, index=True)
    city: Optional[str] = Field(default=None, index=True)
    address_line1: Optional[str] = Field(default=None, index=True)
    address_line2: Optional[str] = Field(default=None)
    visited_on: Optional[date] = Field(default=None, index=True)

    # Stable upsert key (latest-only ingestion)
    practice_key: str = Field(index=True, unique=True)

    # Analytics-friendly fields
    practice_name: Optional[str] = Field(default=None, index=True)
    county: Optional[str] = Field(default=None, index=True)
    surgery_count: Optional[int] = Field(default=None, index=True)

    # Core valuation metrics
    goodwill: Optional[float] = Field(default=None)
    efandf: Optional[float] = Field(default=None)
    total: Optional[float] = Field(default=None)
    freehold: Optional[float] = Field(default=None)
    grand_total: Optional[float] = Field(default=None)

    # NHS contract details (UDA block)
    nhs_contract_number: Optional[str] = Field(default=None, index=True)
    uda_contract_value_gbp: Optional[float] = Field(default=None)
    uda_count: Optional[float] = Field(default=None)
    uda_rate_gbp: Optional[float] = Field(default=None)
    uda_uplift_value_gbp: Optional[float] = Field(default=None)

    # Split of income (selected common types)
    income_split_fpi_percent: Optional[float] = Field(default=None)
    income_split_fpi_value: Optional[float] = Field(default=None)
    income_split_fpi_applied_percent: Optional[float] = Field(default=None)
    income_split_fpi_applied_value: Optional[float] = Field(default=None)

    income_split_nhs_percent: Optional[float] = Field(default=None)
    income_split_nhs_value: Optional[float] = Field(default=None)
    income_split_nhs_applied_percent: Optional[float] = Field(default=None)
    income_split_nhs_applied_value: Optional[float] = Field(default=None)

    income_split_denplan_percent: Optional[float] = Field(default=None)
    income_split_denplan_value: Optional[float] = Field(default=None)
    income_split_denplan_applied_percent: Optional[float] = Field(default=None)
    income_split_denplan_applied_value: Optional[float] = Field(default=None)

    income_split_rent_percent: Optional[float] = Field(default=None)
    income_split_rent_value: Optional[float] = Field(default=None)
    income_split_rent_applied_percent: Optional[float] = Field(default=None)
    income_split_rent_applied_value: Optional[float] = Field(default=None)

    associate_cost_amount: Optional[float] = Field(default=None)
    associate_cost_pct: Optional[float] = Field(default=None)  # 0..100
    accounts_period_end: Optional[date] = Field(default=None, index=True)

    # Certified accounts (latest + previous year end)
    certified_accounts_period_end_prev: Optional[date] = Field(default=None, index=True)

    cert_income_gbp: Optional[float] = Field(default=None)
    cert_income_percent: Optional[float] = Field(default=None)
    cert_income_gbp_prev: Optional[float] = Field(default=None)
    cert_income_percent_prev: Optional[float] = Field(default=None)

    cert_other_inc_gbp: Optional[float] = Field(default=None)
    cert_other_inc_percent: Optional[float] = Field(default=None)
    cert_other_inc_gbp_prev: Optional[float] = Field(default=None)
    cert_other_inc_percent_prev: Optional[float] = Field(default=None)

    cert_associates_gbp: Optional[float] = Field(default=None)
    cert_associates_percent: Optional[float] = Field(default=None)
    cert_associates_gbp_prev: Optional[float] = Field(default=None)
    cert_associates_percent_prev: Optional[float] = Field(default=None)

    cert_wages_gbp: Optional[float] = Field(default=None)
    cert_wages_percent: Optional[float] = Field(default=None)
    cert_wages_gbp_prev: Optional[float] = Field(default=None)
    cert_wages_percent_prev: Optional[float] = Field(default=None)

    cert_hygiene_gbp: Optional[float] = Field(default=None)
    cert_hygiene_percent: Optional[float] = Field(default=None)
    cert_hygiene_gbp_prev: Optional[float] = Field(default=None)
    cert_hygiene_percent_prev: Optional[float] = Field(default=None)

    cert_materials_gbp: Optional[float] = Field(default=None)
    cert_materials_percent: Optional[float] = Field(default=None)
    cert_materials_gbp_prev: Optional[float] = Field(default=None)
    cert_materials_percent_prev: Optional[float] = Field(default=None)

    cert_labs_gbp: Optional[float] = Field(default=None)
    cert_labs_percent: Optional[float] = Field(default=None)
    cert_labs_gbp_prev: Optional[float] = Field(default=None)
    cert_labs_percent_prev: Optional[float] = Field(default=None)

    cert_net_profit_gbp: Optional[float] = Field(default=None)
    cert_net_profit_percent: Optional[float] = Field(default=None)
    cert_net_profit_gbp_prev: Optional[float] = Field(default=None)
    cert_net_profit_percent_prev: Optional[float] = Field(default=None)

    source_file: Optional[str] = None

    raw_json: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSONB, nullable=False))

    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
    updated_at: datetime = Field(default_factory=datetime.utcnow, index=True)

