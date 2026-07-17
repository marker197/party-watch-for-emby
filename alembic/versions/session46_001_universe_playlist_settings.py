"""Add playlist_enabled and custom_name to universes table.

Revision ID: session46_001
Revises: session41_001
"""

import sqlalchemy as sa
from alembic import op

revision = "session46_001"
down_revision = "session41_001"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("universes", sa.Column("playlist_enabled", sa.Boolean(), server_default=sa.text("false")))
    op.add_column("universes", sa.Column("custom_name", sa.String(256), nullable=True))


def downgrade():
    op.drop_column("universes", "custom_name")
    op.drop_column("universes", "playlist_enabled")
