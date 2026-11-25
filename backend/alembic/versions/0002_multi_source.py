"""multi-source instruments: crypto assets, per-instrument data mode, fractional volume

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-25

- instruments.data_mode records where an instrument's data comes from (simulated / live / delayed).
  It is set once (seed or first data) and never flipped, so one instrument never mixes simulated
  and real history; the processor rejects events of the other kind.
- crypto pairs (Binance) need asset_type 'crypto', 4-letter quote currencies (USDT) and fractional
  volumes (0.0012 BTC), so volume moves from BIGINT to NUMERIC(28, 8).
"""

import sqlalchemy as sa
from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("instruments", "currency", type_=sa.String(8), existing_type=sa.String(3), existing_nullable=False)
    op.drop_constraint(op.f("ck_instruments_asset_type"), "instruments", type_="check")
    op.create_check_constraint(op.f("ck_instruments_asset_type"), "instruments", "asset_type IN ('equity', 'etf', 'crypto')")
    op.add_column("instruments", sa.Column("data_mode", sa.String(16), nullable=True))
    op.create_check_constraint(op.f("ck_instruments_data_mode"), "instruments", "data_mode IN ('simulated', 'live', 'delayed')")
    # Existing databases: an instrument's mode is that of its latest stored event (NULL if none;
    # the seed step then fills it from the configured providers).
    op.execute(
        """
        UPDATE instruments i SET data_mode = (
            SELECT m.data_mode FROM market_events m WHERE m.instrument_id = i.id
            ORDER BY m.event_timestamp DESC LIMIT 1)
        """
    )
    for table in ("market_events", "historical_aggregates"):
        op.alter_column(table, "volume", type_=sa.Numeric(28, 8), existing_type=sa.BigInteger, existing_nullable=False)


def downgrade() -> None:
    # Fails by design if crypto instruments exist (asset_type check): remove them first.
    for table in ("market_events", "historical_aggregates"):
        op.alter_column(table, "volume", type_=sa.BigInteger, existing_type=sa.Numeric(28, 8),
                        existing_nullable=False, postgresql_using="round(volume)::bigint")
    op.drop_constraint(op.f("ck_instruments_data_mode"), "instruments", type_="check")
    op.drop_column("instruments", "data_mode")
    op.drop_constraint(op.f("ck_instruments_asset_type"), "instruments", type_="check")
    op.create_check_constraint(op.f("ck_instruments_asset_type"), "instruments", "asset_type IN ('equity', 'etf')")
    op.alter_column("instruments", "currency", type_=sa.String(3), existing_type=sa.String(8), existing_nullable=False)
