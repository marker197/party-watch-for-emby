"""add feature_count to ml_models

Revision ID: session17_001
Revises: 0013_schema_reconcile
Create Date: 2026-07-04
"""

from alembic import op
import sqlalchemy as sa

revision = "session17_001"
down_revision = "0013_schema_reconcile"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("ml_models", sa.Column("feature_count", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("ml_models", "feature_count")
