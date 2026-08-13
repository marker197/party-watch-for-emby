"""Add dismissed_issues table so resolved/ignored scan issues stay hidden.

Revision ID: 011_dismissed_issues
Revises: 010_watchlist_items
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect

revision = "011_dismissed_issues"
down_revision = "010_watchlist_items"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa_inspect(bind)
    existing_tables = inspector.get_table_names()

    if "dismissed_issues" not in existing_tables:
        op.create_table(
            "dismissed_issues",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column(
                "user_id", sa.Integer,
                sa.ForeignKey("users.id", ondelete="CASCADE"),
                nullable=False, index=True,
            ),
            # orphaned_history | missing_metadata | duplicate_library
            sa.Column("issue_type", sa.String(64), nullable=False),
            # Stable identity for the issue, e.g. 'orphaned:movie:tt0111161'
            # or 'orphaned:episode:breaking bad'
            sa.Column("issue_key", sa.String(512), nullable=False),
            sa.Column("title", sa.String(512), nullable=True),
            sa.Column("note", sa.String(512), nullable=True),
            sa.Column("dismissed_at", sa.DateTime, nullable=True),
            sa.UniqueConstraint(
                "user_id", "issue_type", "issue_key",
                name="uq_dismissed_issue_user_type_key",
            ),
        )


def downgrade() -> None:
    op.drop_table("dismissed_issues")
