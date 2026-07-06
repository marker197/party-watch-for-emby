"""add queue feedback fields

Revision ID: a2b3c4d5e6f7
Revises: 186401ba8047
Create Date: 2026-06-29

Adds played_at and played_duration_ticks to queue_items for the feedback loop.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'a2b3c4d5e6f7'
down_revision: Union[str, Sequence[str], None] = '186401ba8047'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('queue_items', sa.Column('played_at', sa.DateTime(), nullable=True))
    op.add_column('queue_items', sa.Column('played_duration_ticks', sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column('queue_items', 'played_duration_ticks')
    op.drop_column('queue_items', 'played_at')
