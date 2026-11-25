"""ORM models. The Alembic migration in alembic/versions is the schema of record;
`alembic check` in CI fails if these drift from it."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Numeric,
    PrimaryKeyConstraint,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

NAMING = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

ERROR_TYPES = (
    "malformed_json", "missing_field", "invalid_field", "null_price", "negative_price", "invalid_price",
    "invalid_volume", "invalid_timestamp", "invalid_ohlc", "unknown_symbol", "duplicate_event", "db_rejected",
)
PRICE = Numeric(18, 6)
VOLUME = Numeric(28, 8)  # fractional for crypto (0.0012 BTC)
TS = DateTime(timezone=True)


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING)


class Instrument(Base):
    __tablename__ = "instruments"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(10), nullable=False)
    company_name: Mapped[str] = mapped_column(String(200), nullable=False)
    exchange: Mapped[str] = mapped_column(String(32), nullable=False)
    asset_type: Mapped[str] = mapped_column(String(16), nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    # Where this instrument's data comes from; set once, never mixed (see migration 0002).
    data_mode: Mapped[str | None] = mapped_column(String(16))
    created_at: Mapped[datetime] = mapped_column(TS, nullable=False, server_default=func.now())
    __table_args__ = (
        UniqueConstraint("symbol"),
        CheckConstraint(r"symbol ~ '^[A-Z][A-Z0-9.\-]{0,9}$'", name="symbol_format"),
        CheckConstraint("asset_type IN ('equity', 'etf', 'crypto')", name="asset_type"),
        CheckConstraint("data_mode IN ('simulated', 'live', 'delayed')", name="data_mode"),
    )


class MarketEvent(Base):
    __tablename__ = "market_events"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    instrument_id: Mapped[int] = mapped_column(ForeignKey("instruments.id", ondelete="RESTRICT"), nullable=False)
    event_timestamp: Mapped[datetime] = mapped_column(TS, nullable=False)
    price: Mapped[Decimal] = mapped_column(PRICE, nullable=False)
    volume: Mapped[Decimal] = mapped_column(VOLUME, nullable=False)
    open: Mapped[Decimal] = mapped_column(PRICE, nullable=False)
    high: Mapped[Decimal] = mapped_column(PRICE, nullable=False)
    low: Mapped[Decimal] = mapped_column(PRICE, nullable=False)
    close: Mapped[Decimal] = mapped_column(PRICE, nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    data_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    processing_status: Mapped[str] = mapped_column(String(16), nullable=False)
    attempts: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default="1")
    ingested_at: Mapped[datetime | None] = mapped_column(TS)
    processed_at: Mapped[datetime] = mapped_column(TS, nullable=False, server_default=func.now())
    __table_args__ = (
        UniqueConstraint("event_id"),
        CheckConstraint("price > 0", name="price_positive"),
        CheckConstraint("volume >= 0", name="volume_non_negative"),
        CheckConstraint("low <= high AND open > 0 AND close > 0 AND low > 0", name="ohlc"),
        CheckConstraint("data_mode IN ('simulated', 'live', 'delayed')", name="data_mode"),
        CheckConstraint("processing_status IN ('valid', 'retried')", name="processing_status"),
        CheckConstraint("attempts >= 1", name="attempts"),
        Index("ix_market_events_instrument_id_event_timestamp", "instrument_id", "event_timestamp"),
        Index("ix_market_events_event_timestamp", "event_timestamp"),
        Index("ix_market_events_processing_status_event_timestamp", "processing_status", "event_timestamp"),
    )


class HistoricalAggregate(Base):
    __tablename__ = "historical_aggregates"
    # Composite PK because TimescaleDB requires the time column in every unique index.
    id: Mapped[int] = mapped_column(BigInteger, autoincrement=True, nullable=False)
    instrument_id: Mapped[int] = mapped_column(ForeignKey("instruments.id", ondelete="RESTRICT"), nullable=False)
    interval: Mapped[str] = mapped_column(String(4), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(TS, nullable=False)
    open: Mapped[Decimal] = mapped_column(PRICE, nullable=False)
    high: Mapped[Decimal] = mapped_column(PRICE, nullable=False)
    low: Mapped[Decimal] = mapped_column(PRICE, nullable=False)
    close: Mapped[Decimal] = mapped_column(PRICE, nullable=False)
    volume: Mapped[Decimal] = mapped_column(VOLUME, nullable=False)
    first_event_at: Mapped[datetime] = mapped_column(TS, nullable=False)
    last_event_at: Mapped[datetime] = mapped_column(TS, nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint("id", "timestamp"),
        UniqueConstraint("instrument_id", "interval", "timestamp"),
        CheckConstraint("interval IN ('1m', '1d')", name="interval"),
        CheckConstraint("low <= high AND low > 0 AND volume >= 0", name="ohlcv"),
    )


class Watchlist(Base):
    __tablename__ = "watchlists"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(TS, nullable=False, server_default=func.now())
    __table_args__ = (
        UniqueConstraint("name"),
        CheckConstraint("char_length(btrim(name)) BETWEEN 1 AND 64", name="name_length"),
    )


class WatchlistInstrument(Base):
    __tablename__ = "watchlist_instruments"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    watchlist_id: Mapped[int] = mapped_column(ForeignKey("watchlists.id", ondelete="CASCADE"), nullable=False)
    instrument_id: Mapped[int] = mapped_column(ForeignKey("instruments.id", ondelete="CASCADE"), nullable=False)
    added_at: Mapped[datetime] = mapped_column(TS, nullable=False, server_default=func.now())
    __table_args__ = (UniqueConstraint("watchlist_id", "instrument_id"),)


class PipelineMetric(Base):
    __tablename__ = "pipeline_metrics"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    service_name: Mapped[str] = mapped_column(String(64), nullable=False)
    metric_name: Mapped[str] = mapped_column(String(64), nullable=False)
    metric_value: Mapped[float] = mapped_column(Float, nullable=False)
    timestamp: Mapped[datetime] = mapped_column(TS, nullable=False, server_default=func.now())
    __table_args__ = (
        Index("ix_pipeline_metrics_service_name_metric_name_timestamp", "service_name", "metric_name", "timestamp"),
        Index("ix_pipeline_metrics_timestamp", "timestamp"),
    )


class ProcessingError(Base):
    __tablename__ = "processing_errors"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    event_id: Mapped[str | None] = mapped_column(String(64))
    symbol: Mapped[str | None] = mapped_column(String(16))
    error_type: Mapped[str] = mapped_column(String(32), nullable=False)
    error_message: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str] = mapped_column(String(8), nullable=False)
    source: Mapped[str | None] = mapped_column(String(64))
    raw_payload: Mapped[str | None] = mapped_column(Text)
    timestamp: Mapped[datetime] = mapped_column(TS, nullable=False, server_default=func.now())
    __table_args__ = (
        CheckConstraint("error_type IN (" + ", ".join(f"'{t}'" for t in ERROR_TYPES) + ")", name="error_type"),
        CheckConstraint("severity IN ('warning', 'error')", name="severity"),
        Index("ix_processing_errors_timestamp", "timestamp"),
        Index("ix_processing_errors_error_type_timestamp", "error_type", "timestamp"),
        Index("ix_processing_errors_event_id", "event_id"),
    )
