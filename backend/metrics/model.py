from pathlib import Path

from sqlalchemy import Float, Integer, String, UniqueConstraint, create_engine
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
    prediction_time: Mapped[str] = mapped_column(String, nullable=False)
    predicted_volatility: Mapped[float] = mapped_column(Float, nullable=False)
    realized_volatility: Mapped[float] = mapped_column(Float, nullable=False)
    abs_error: Mapped[float] = mapped_column(Float, nullable=False)
    squared_error: Mapped[float] = mapped_column(Float, nullable=False)
    ape: Mapped[float | None] = mapped_column(Float, nullable=True)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    timeframe: Mapped[str] = mapped_column(String, nullable=False)
    window: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[str] = mapped_column(String, nullable=False)


class MetricSnapshot(Base):
    __tablename__ = 'metric_snapshots'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    computed_at: Mapped[str] = mapped_column(String, nullable=False)
    symbol: Mapped[str] = mapped_column(String, nullable=False)
    timeframe: Mapped[str] = mapped_column(String, nullable=False)
    window: Mapped[int] = mapped_column(Integer, nullable=False)
    prediction_count: Mapped[int] = mapped_column(Integer, nullable=False)
    mae: Mapped[float] = mapped_column(Float, nullable=False)
    rmse: Mapped[float] = mapped_column(Float, nullable=False)
    mape: Mapped[float | None] = mapped_column(Float, nullable=True)
    directional_accuracy: Mapped[float | None] = mapped_column(Float, nullable=True)


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
