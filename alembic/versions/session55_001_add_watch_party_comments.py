"""Add watch_party_comments table for persisting user comments during parties.

Revision ID: session55_001
Revises: session54_001
"""

from alembic import op
import sqlalchemy as sa

revision = "session55_001"
down_revision = "session54_001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "watch_party_comments",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("party_id", sa.Integer(), sa.ForeignKey("watch_parties.id", ondelete="CASCADE"), nullable=False, index=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id"), nullable=True),
        sa.Column("username", sa.String(128)),
        sa.Column("comment_text", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime()),
    )


def downgrade() -> None:
    op.drop_table("watch_party_comments")
