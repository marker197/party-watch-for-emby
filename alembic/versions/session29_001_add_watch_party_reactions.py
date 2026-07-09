"""Add watch_party_reactions table.

Revision ID: session29_001
Revises: session26_002
"""

from alembic import op
import sqlalchemy as sa

revision = "session29_001"
down_revision = "session26_002"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "watch_party_reactions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("party_id", sa.Integer(), sa.ForeignKey("watch_parties.id", ondelete="CASCADE"), nullable=False),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=True),
        sa.Column("emoji", sa.String(16), nullable=False),
        sa.Column("position_ticks", sa.Integer(), server_default="0"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()")),
    )
    op.create_index("ix_watch_party_reactions_party_id", "watch_party_reactions", ["party_id"])


def downgrade():
    op.drop_index("ix_watch_party_reactions_party_id", table_name="watch_party_reactions")
    op.drop_table("watch_party_reactions")
