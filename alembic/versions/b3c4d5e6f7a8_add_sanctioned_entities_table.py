"""add sanctioned_entities table

Revision ID: b3c4d5e6f7a8
Revises: 4eca6a74321d
Create Date: 2026-03-24 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b3c4d5e6f7a8'
down_revision: Union[str, Sequence[str], None] = '4eca6a74321d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'sanctioned_entities',
        sa.Column('id',          sa.Integer(),     nullable=False),
        sa.Column('source_id',   sa.String(50),    nullable=False),
        sa.Column('name',        sa.String(500),   nullable=False),
        sa.Column('entity_type', sa.Enum('vessel', 'company', 'person', 'aircraft', 'other',
                                         name='entitytype'), nullable=False),
        sa.Column('identifiers', sa.JSON(),         nullable=True),
        sa.Column('source',      sa.String(20),    nullable=False),
        sa.Column('country',     sa.String(100),   nullable=True),
        sa.Column('programs',    sa.String(500),   nullable=True),
        sa.Column('remarks',     sa.Text(),         nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('source', 'source_id', name='uq_sanctioned_source_id'),
    )
    op.create_index('ix_sanctioned_entities_name', 'sanctioned_entities', ['name'])
    op.create_index('ix_sanctioned_entities_type', 'sanctioned_entities', ['entity_type'])
    op.create_index(op.f('ix_sanctioned_entities_source_id'), 'sanctioned_entities', ['source_id'])


def downgrade() -> None:
    op.drop_index(op.f('ix_sanctioned_entities_source_id'), table_name='sanctioned_entities')
    op.drop_index('ix_sanctioned_entities_type', table_name='sanctioned_entities')
    op.drop_index('ix_sanctioned_entities_name', table_name='sanctioned_entities')
    op.drop_table('sanctioned_entities')
    op.execute("DROP TYPE IF EXISTS entitytype")
