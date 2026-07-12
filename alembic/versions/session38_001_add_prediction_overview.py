"""Add overview column to predictions table.

Revision ID: session38_001
Revises: session29_001
"""

from alembic import op
import sqlalchemy as sa

revision = "session38_001"
down_revision = "session29_001"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("predictions", sa.Column("overview", sa.Text(), nullable=True))


def downgrade():
    op.drop_column("predictions", "overview")
