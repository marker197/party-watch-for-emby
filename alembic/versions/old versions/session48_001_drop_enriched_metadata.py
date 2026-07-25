"""Drop orphaned enriched_metadata table.

Revision ID: session48_001
Revises: session46_001
"""

import sqlalchemy as sa
from alembic import op

revision = "session48_001"
down_revision = "session46_001"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_table("enriched_metadata")


def downgrade():
    op.create_table(
        "enriched_metadata",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("emby_item_id", sa.String(64), unique=True, nullable=False, index=True),
        sa.Column("trakt_id", sa.String(64)),
        sa.Column("trakt_slug", sa.String(256)),
        sa.Column("title", sa.String(512)),
        sa.Column("tagline", sa.String(512)),
        sa.Column("themes", sa.JSON()),
        sa.Column("quotes", sa.JSON()),
        sa.Column("social_score", sa.Float()),
        sa.Column("trakt_rating", sa.Float()),
        sa.Column("trakt_votes", sa.Integer()),
        sa.Column("themes_from_trakt", sa.Boolean(), server_default=sa.text("false")),
        sa.Column("enriched_at", sa.DateTime()),
        sa.Column("expires_at", sa.DateTime()),
        sa.Column("metadata_json", sa.JSON()),
    )
