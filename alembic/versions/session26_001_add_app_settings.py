"""Add app_settings table for runtime-configurable settings.

Revision ID: session26_001
Revises: session18_001
"""

from alembic import op
import sqlalchemy as sa

revision = "session26_001"
down_revision = "session18_001"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "app_settings",
        sa.Column("key", sa.String(128), primary_key=True),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("updated_at", sa.DateTime()),
    )


def downgrade():
    op.drop_table("app_settings")
