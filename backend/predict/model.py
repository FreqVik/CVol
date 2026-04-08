from pathlib import Path
from datetime import datetime

from sqlalchemy import DateTime, Float, Integer, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


BASE_DIR = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = BASE_DIR / 'backend' / 'data' / 'predictions.db'


class Base(DeclarativeBase):
    pass


class Prediction(Base):
    __tablename__ = 'predictions'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    prediction_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    predicted_volatility: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    def to_dict(self):
        return {
            "id": self.id,
            "prediction_time": self.prediction_time.isoformat() if self.prediction_time else None,
            "predicted_volatility": self.predicted_volatility,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


def _database_url(db_path: Path) -> str:
    return f"sqlite:///{db_path}"


def get_prediction_engine(db_path: Path = DEFAULT_DB_PATH):
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(_database_url(db_path), future=True)


def init_prediction_db(db_path: Path = DEFAULT_DB_PATH):
    engine = get_prediction_engine(db_path)
    Base.metadata.create_all(engine)
    return engine


def get_prediction_session_factory(db_path: Path = DEFAULT_DB_PATH):
    engine = get_prediction_engine(db_path)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
