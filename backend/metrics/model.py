from pathlib import Path
from datetime import datetime

from sqlalchemy import DateTime, Float, Integer, String, UniqueConstraint, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = BASE_DIR / 'backend' / 'data' / 'metrics.db'


class Base(DeclarativeBase):
    pass


class PredictionMetric(Base):
    __tablename__ = 'prediction_metrics'
    __table_args__ = (
        UniqueConstraint('prediction_id', 'symbol', 'timeframe', 'window', name='uq_prediction_metric'),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    prediction_id: Mapped[int] = mapped_column(Integer, nullable=False)
    prediction_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    predicted_volatility: Mapped[float] = mapped_column(Float, nullable=False)
    realized_volatility: Mapped[float] = mapped_column(Float, nullable=False)
    abs_error: Mapped[float] = mapped_column(Float, nullable=False)
    squared_error: Mapped[float] = mapped_column(Float, nullable=False)
    ape: Mapped[float | None] = mapped_column(Float, nullable=True)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    timeframe: Mapped[str] = mapped_column(String, nullable=False)
    window: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    def to_dict(self):
        return {
            'id': self.id,
            'prediction_id': self.prediction_id,
            'prediction_time': self.prediction_time.isoformat() if self.prediction_time else None,
            'predicted_volatility': self.predicted_volatility,
            'realized_volatility': self.realized_volatility,
            'abs_error': self.abs_error,
            'squared_error': self.squared_error,
            'ape': self.ape,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }


class MetricSnapshot(Base):
    __tablename__ = 'metric_snapshots'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    timeframe: Mapped[str] = mapped_column(String, nullable=False)
    window: Mapped[int] = mapped_column(Integer, nullable=False)
    prediction_count: Mapped[int] = mapped_column(Integer, nullable=False)
    mae: Mapped[float] = mapped_column(Float, nullable=False)
    rmse: Mapped[float] = mapped_column(Float, nullable=False)
    mape: Mapped[float | None] = mapped_column(Float, nullable=True)
    directional_accuracy: Mapped[float | None] = mapped_column(Float, nullable=True)

    def to_dict(self):
        return {
            'id': self.id,
            'computed_at': self.computed_at.isoformat() if self.computed_at else None,
            'symbol': self.symbol,
            'timeframe': self.timeframe,
            'window': self.window,
            'prediction_count': self.prediction_count,
            'mae': self.mae,
            'rmse': self.rmse,
            'mape': self.mape,
            'directional_accuracy': self.directional_accuracy,
        }


def _database_url(db_path: Path) -> str:
    return f"sqlite:///{db_path}"


def get_metrics_engine(db_path: Path = DEFAULT_DB_PATH):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(_database_url(db_path), future=True)


def init_metrics_db(db_path: Path = DEFAULT_DB_PATH):
    engine = get_metrics_engine(db_path)
    Base.metadata.create_all(engine)
    return engine


def get_metrics_session_factory(db_path: Path = DEFAULT_DB_PATH):
    engine = get_metrics_engine(db_path)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
