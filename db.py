from sqlalchemy import create_engine, Column, Integer, String, DateTime, UniqueConstraint
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime
from config import DATABASE_URL

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class AfmEntry(Base):
    __tablename__ = "afm_entries"

    id = Column(Integer, primary_key=True, index=True)
    afm_key = Column(String, unique=True, index=True)
    emittent = Column(String)
    melder = Column(String)
    meldingsdatum = Column(String)

    # new: store percentages as text (e.g. "3.12%")
    kapitaal_pct = Column(String, nullable=True)
    stem_pct     = Column(String, nullable=True)
    prev_kapitaal_pct = Column(String, nullable=True)
    prev_stem_pct     = Column(String, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (UniqueConstraint("afm_key", name="uq_afm_key"),)

def init_db():
    Base.metadata.create_all(bind=engine)
