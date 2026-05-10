from __future__ import annotations

from sqlite3 import Connection

from podcast_frequency_list.tokens.models import (
    CandidateMetricsResult,
    CandidateMetricsValidationResult,
)

from .association import _AssociationStore
from .boundary import _BoundaryStore
from .containment import _ContainmentStore
from .display import _DisplayStore
from .identity import _UnitIdentityStore
from .store import _MetricsStore

_METRIC_RESULT_FIELDS = (
    ("raw_frequency_mismatch_count", "raw_frequency"),
    ("episode_dispersion_mismatch_count", "episode_dispersion"),
    ("show_dispersion_mismatch_count", "show_dispersion"),
)


class _CandidateMetricsWorkflow:
    def __init__(self, *, connection: Connection, inventory_version: str) -> None:
        self.connection = connection
        self.inventory_version = inventory_version
        self.metric_store = _MetricsStore(
            connection=connection,
            inventory_version=inventory_version,
        )
        self.association_store = _AssociationStore(
            connection=connection,
            inventory_version=inventory_version,
        )
        self.boundary_store = _BoundaryStore(
            connection=connection,
            inventory_version=inventory_version,
        )
        self.containment_store = _ContainmentStore(
            connection=connection,
            inventory_version=inventory_version,
        )
        self.identity_store = _UnitIdentityStore(
            connection=connection,
            inventory_version=inventory_version,
        )
        self.display_store = _DisplayStore(
            connection=connection,
            inventory_version=inventory_version,
        )

    def count_candidates(self) -> int:
        return self.metric_store.count_candidates()

    def summarize(self, *, selected_candidates: int) -> CandidateMetricsResult:
        summary = self.metric_store.load_summary()
        return CandidateMetricsResult(
            inventory_version=self.inventory_version,
            selected_candidates=selected_candidates,
            refreshed_candidates=summary["candidate_count"],
            deleted_orphan_candidates=0,
            occurrence_count=summary["occurrence_count"],
            raw_frequency_total=summary["raw_frequency_total"],
            episode_dispersion_total=summary["episode_dispersion_total"],
            show_dispersion_total=summary["show_dispersion_total"],
            display_text_updates=0,
        )

    def validate(self) -> CandidateMetricsValidationResult:
        summary = self.metric_store.load_summary()
        metric_mismatches = self.metric_store.count_mismatches()

        return CandidateMetricsValidationResult(
            inventory_version=self.inventory_version,
            candidate_count=summary["candidate_count"],
            occurrence_count=summary["occurrence_count"],
            display_text_mismatch_count=self.display_store.count_mismatches(),
            foreign_key_issue_count=_count_foreign_key_issues(self.connection),
            **{
                result_field: metric_mismatches[metric_column]
                for result_field, metric_column in _METRIC_RESULT_FIELDS
            },
        )

    def refresh(self, *, selected_candidates: int) -> CandidateMetricsResult:
        deleted_orphan_candidates = _delete_orphan_candidates(
            self.connection,
            inventory_version=self.inventory_version,
        )
        self.metric_store.refresh()
        self.association_store.refresh()
        self.boundary_store.refresh()
        self.containment_store.refresh()
        self.identity_store.refresh()
        display_text_updates = self.display_store.refresh()
        summary = self.metric_store.load_summary()

        return CandidateMetricsResult(
            inventory_version=self.inventory_version,
            selected_candidates=selected_candidates,
            refreshed_candidates=summary["candidate_count"],
            deleted_orphan_candidates=deleted_orphan_candidates,
            occurrence_count=summary["occurrence_count"],
            raw_frequency_total=summary["raw_frequency_total"],
            episode_dispersion_total=summary["episode_dispersion_total"],
            show_dispersion_total=summary["show_dispersion_total"],
            display_text_updates=display_text_updates,
        )


def _delete_orphan_candidates(connection: Connection, *, inventory_version: str) -> int:
    cursor = connection.execute(
        """
        DELETE FROM token_candidates
        WHERE inventory_version = ?
        AND NOT EXISTS (
            SELECT 1
            FROM token_occurrences occ
            WHERE occ.candidate_id = token_candidates.candidate_id
            AND occ.inventory_version = token_candidates.inventory_version
        )
        """,
        (inventory_version,),
    )
    return cursor.rowcount


def _count_foreign_key_issues(connection: Connection) -> int:
    return len(connection.execute("PRAGMA foreign_key_check").fetchall())
