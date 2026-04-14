from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from app.models.enums import IssueSeverity


@dataclass(frozen=True)
class ValidationIssue:
    severity: IssueSeverity
    code: str
    message: str
    field_name: Optional[str] = None


class RecordValidator:
    def validate(
        self,
        *,
        reporting_date_present: bool,
        entity_name_present: bool,
        revenue: Optional[Decimal],
        cost: Optional[Decimal],
        gross_profit: Optional[Decimal],
        margin_0_1: Optional[float],
    ) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []

        if not reporting_date_present:
            issues.append(
                ValidationIssue(
                    severity=IssueSeverity.warning,
                    code="missing_reporting_date",
                    message="Reporting date missing or unparseable.",
                    field_name="reporting_date",
                )
            )
        if not entity_name_present:
            issues.append(
                ValidationIssue(
                    severity=IssueSeverity.warning,
                    code="missing_entity_name",
                    message="Entity name missing.",
                    field_name="entity_name",
                )
            )

        if margin_0_1 is not None and not (0.0 <= margin_0_1 <= 1.0):
            issues.append(
                ValidationIssue(
                    severity=IssueSeverity.warning,
                    code="margin_out_of_range",
                    message="Margin is outside expected range 0..1 after normalization.",
                    field_name="margin",
                )
            )

        # Relationship checks when possible
        if revenue is not None and cost is not None:
            if revenue < 0:
                issues.append(
                    ValidationIssue(
                        severity=IssueSeverity.warning,
                        code="revenue_negative",
                        message="Revenue is negative.",
                        field_name="revenue",
                    )
                )
            if cost < 0:
                issues.append(
                    ValidationIssue(
                        severity=IssueSeverity.warning,
                        code="cost_negative",
                        message="Cost is negative.",
                        field_name="cost",
                    )
                )

        if revenue is not None and cost is not None and gross_profit is not None:
            # loose consistency check (tolerance)
            expected = revenue - cost
            if expected != gross_profit:
                diff = abs(expected - gross_profit)
                if diff > max(Decimal("0.01"), abs(expected) * Decimal("0.01")):
                    issues.append(
                        ValidationIssue(
                            severity=IssueSeverity.info,
                            code="gross_profit_mismatch",
                            message="Gross profit differs from revenue - cost (tolerance applied).",
                            field_name="gross_profit",
                        )
                    )

        return issues

