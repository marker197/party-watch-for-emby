"""Add in_library column to queue_items; make emby_item_id nullable.

Revision ID: session26_002
Revises: session26_001
"""

from alembic import op
import sqlalchemy as sa

revision = "session26_002"
down_revision = "session26_001"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("queue_items",
                  sa.Column("in_library", sa.Boolean(), server_default=sa.text("true")))
    op.alter_column("queue_items", "emby_item_id", nullable=True)


def downgrade():
    op.drop_column("queue_items", "in_library")
    op.alter_column("queue_items", "emby_item_id", nullable=False)
