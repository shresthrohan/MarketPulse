"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-11-25
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

TS = sa.DateTime(timezone=True)
PRICE = sa.Numeric(18, 6)
ERROR_TYPES = (
    "malformed_json", "missing_field", "invalid_field", "null_price", "negative_price", "invalid_price",
    "invalid_volume", "invalid_timestamp", "invalid_ohlc", "unknown_symbol", "duplicate_event", "db_rejected",
)


def upgrade() -> None:
    op.create_table(
        "instruments",
        sa.Column("id", sa.Integer, nullable=False),
        sa.Column("symbol", sa.String(10), nullable=False),
        sa.Column("company_name", sa.String(200), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("asset_type", sa.String(16), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("created_at", TS, nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_instruments"),
        sa.UniqueConstraint("symbol", name="uq_instruments_symbol"),
        sa.CheckConstraint(r"symbol ~ '^[A-Z][A-Z0-9.\-]{0,9}$'", name=op.f("ck_instruments_symbol_format")),
        sa.CheckConstraint("asset_type IN ('equity', 'etf')", name=op.f("ck_instruments_asset_type")),
    )

    op.create_table(
        "market_events",
        sa.Column("id", sa.BigInteger, nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("instrument_id", sa.Integer, nullable=False),
        sa.Column("event_timestamp", TS, nullable=False),
        sa.Column("price", PRICE, nullable=False),
        sa.Column("volume", sa.BigInteger, nullable=False),
        sa.Column("open", PRICE, nullable=False),
        sa.Column("high", PRICE, nullable=False),
        sa.Column("low", PRICE, nullable=False),
        sa.Column("close", PRICE, nullable=False),
        sa.Column("source", sa.String(64), nullable=False),
        sa.Column("data_mode", sa.String(16), nullable=False),
        sa.Column("processing_status", sa.String(16), nullable=False),
        sa.Column("attempts", sa.SmallInteger, nullable=False, server_default="1"),
        sa.Column("ingested_at", TS),
        sa.Column("processed_at", TS, nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_market_events"),
        # Plain table, not a hypertable: a hypertable's unique indexes must include the
        # time column, which would weaken event_id deduplication to (event_id, time).
        sa.UniqueConstraint("event_id", name="uq_market_events_event_id"),
        sa.ForeignKeyConstraint(["instrument_id"], ["instruments.id"], ondelete="RESTRICT",
                                name="fk_market_events_instrument_id_instruments"),
        sa.CheckConstraint("price > 0", name=op.f("ck_market_events_price_positive")),
        sa.CheckConstraint("volume >= 0", name=op.f("ck_market_events_volume_non_negative")),
        sa.CheckConstraint("low <= high AND open > 0 AND close > 0 AND low > 0", name=op.f("ck_market_events_ohlc")),
        sa.CheckConstraint("data_mode IN ('simulated', 'live', 'delayed')", name=op.f("ck_market_events_data_mode")),
        sa.CheckConstraint("processing_status IN ('valid', 'retried')", name=op.f("ck_market_events_processing_status")),
        sa.CheckConstraint("attempts >= 1", name=op.f("ck_market_events_attempts")),
    )
    op.create_index("ix_market_events_instrument_id_event_timestamp", "market_events", ["instrument_id", "event_timestamp"])
    op.create_index("ix_market_events_event_timestamp", "market_events", ["event_timestamp"])
    op.create_index("ix_market_events_processing_status_event_timestamp", "market_events", ["processing_status", "event_timestamp"])

    op.create_table(
        "historical_aggregates",
        sa.Column("id", sa.BigInteger, autoincrement=True, nullable=False),
        sa.Column("instrument_id", sa.Integer, nullable=False),
        sa.Column("interval", sa.String(4), nullable=False),
        sa.Column("timestamp", TS, nullable=False),
        sa.Column("open", PRICE, nullable=False),
        sa.Column("high", PRICE, nullable=False),
        sa.Column("low", PRICE, nullable=False),
        sa.Column("close", PRICE, nullable=False),
        sa.Column("volume", sa.BigInteger, nullable=False),
        sa.Column("first_event_at", TS, nullable=False),
        sa.Column("last_event_at", TS, nullable=False),
        sa.PrimaryKeyConstraint("id", "timestamp", name="pk_historical_aggregates"),
        sa.UniqueConstraint("instrument_id", "interval", "timestamp",
                            name="uq_historical_aggregates_instrument_id_interval_timestamp"),
        sa.ForeignKeyConstraint(["instrument_id"], ["instruments.id"], ondelete="RESTRICT",
                                name="fk_historical_aggregates_instrument_id_instruments"),
        sa.CheckConstraint("interval IN ('1m', '1d')", name=op.f("ck_historical_aggregates_interval")),
        sa.CheckConstraint("low <= high AND low > 0 AND volume >= 0", name=op.f("ck_historical_aggregates_ohlcv")),
    )

    op.create_table(
        "watchlists",
        sa.Column("id", sa.Integer, nullable=False),
        sa.Column("name", sa.String(64), nullable=False),
        sa.Column("created_at", TS, nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_watchlists"),
        sa.UniqueConstraint("name", name="uq_watchlists_name"),
        sa.CheckConstraint("char_length(btrim(name)) BETWEEN 1 AND 64", name=op.f("ck_watchlists_name_length")),
    )
    op.create_table(
        "watchlist_instruments",
        sa.Column("id", sa.Integer, nullable=False),
        sa.Column("watchlist_id", sa.Integer, nullable=False),
        sa.Column("instrument_id", sa.Integer, nullable=False),
        sa.Column("added_at", TS, nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_watchlist_instruments"),
        sa.UniqueConstraint("watchlist_id", "instrument_id", name="uq_watchlist_instruments_watchlist_id_instrument_id"),
        sa.ForeignKeyConstraint(["watchlist_id"], ["watchlists.id"], ondelete="CASCADE",
                                name="fk_watchlist_instruments_watchlist_id_watchlists"),
        sa.ForeignKeyConstraint(["instrument_id"], ["instruments.id"], ondelete="CASCADE",
                                name="fk_watchlist_instruments_instrument_id_instruments"),
    )

    op.create_table(
        "pipeline_metrics",
        sa.Column("id", sa.BigInteger, nullable=False),
        sa.Column("service_name", sa.String(64), nullable=False),
        sa.Column("metric_name", sa.String(64), nullable=False),
        sa.Column("metric_value", sa.Float, nullable=False),
        sa.Column("timestamp", TS, nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_pipeline_metrics"),
    )
    op.create_index("ix_pipeline_metrics_service_name_metric_name_timestamp", "pipeline_metrics",
                    ["service_name", "metric_name", "timestamp"])
    op.create_index("ix_pipeline_metrics_timestamp", "pipeline_metrics", ["timestamp"])

    op.create_table(
        "processing_errors",
        sa.Column("id", sa.BigInteger, nullable=False),
        sa.Column("event_id", sa.String(64)),
        sa.Column("symbol", sa.String(16)),
        sa.Column("error_type", sa.String(32), nullable=False),
        sa.Column("error_message", sa.Text, nullable=False),
        sa.Column("severity", sa.String(8), nullable=False),
        sa.Column("source", sa.String(64)),
        sa.Column("raw_payload", sa.Text),
        sa.Column("timestamp", TS, nullable=False, server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("id", name="pk_processing_errors"),
        sa.CheckConstraint("error_type IN (" + ", ".join(f"'{t}'" for t in ERROR_TYPES) + ")",
                           name=op.f("ck_processing_errors_error_type")),
        sa.CheckConstraint("severity IN ('warning', 'error')", name=op.f("ck_processing_errors_severity")),
    )
    op.create_index("ix_processing_errors_timestamp", "processing_errors", ["timestamp"])
    op.create_index("ix_processing_errors_error_type_timestamp", "processing_errors", ["error_type", "timestamp"])
    op.create_index("ix_processing_errors_event_id", "processing_errors", ["event_id"])

    # TimescaleDB where available: chunk the bar table by time. Plain PostgreSQL works unchanged.
    op.execute(
        """
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'timescaledb') THEN
            CREATE EXTENSION IF NOT EXISTS timescaledb;
            PERFORM create_hypertable('historical_aggregates', 'timestamp',
                                      chunk_time_interval => INTERVAL '30 days',
                                      create_default_indexes => false);
          END IF;
        END $$;
        """
    )


def downgrade() -> None:
    for table in ("processing_errors", "pipeline_metrics", "watchlist_instruments", "watchlists",
                  "historical_aggregates", "market_events", "instruments"):
        op.drop_table(table)
