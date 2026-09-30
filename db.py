from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    DateTime,
    UniqueConstraint,
)
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime
from typing import Dict, List, Optional
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

    # store percentages as text (e.g. "3.12%")
    kapitaal_pct = Column(String, nullable=True)
    stem_pct = Column(String, nullable=True)
    prev_kapitaal_pct = Column(String, nullable=True)
    prev_stem_pct = Column(String, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (UniqueConstraint("afm_key", name="uq_afm_key"),)


class ShortRankingSnapshot(Base):
    """One aggregated AFM short-interest snapshot row per issuer per ISO week."""

    __tablename__ = "short_ranking_snapshots"

    id = Column(Integer, primary_key=True, index=True)
    week_uid = Column(String, index=True, nullable=False)
    snapshot_date = Column(String, nullable=False)
    security_key = Column(String, nullable=False)
    issuer = Column(String, nullable=False)
    isin = Column(String, nullable=True)
    ticker = Column(String, nullable=True)
    total_pct = Column(Float, nullable=False)
    holder_count = Column(Integer, nullable=False)
    rank = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "week_uid",
            "security_key",
            name="uq_short_snapshot_week_security",
        ),
    )


class DailyShortOverviewState(Base):
    """Publication state for one daily AFM overview per AFM position date."""

    __tablename__ = "daily_short_overview_state"

    id = Column(Integer, primary_key=True, index=True)
    report_date = Column(String, unique=True, index=True, nullable=False)
    fingerprint = Column(String, nullable=False)
    post_slug = Column(String, nullable=False)
    updated_at = Column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )

    __table_args__ = (
        UniqueConstraint(
            "report_date",
            name="uq_daily_short_overview_report_date",
        ),
    )


def init_db():
    Base.metadata.create_all(bind=engine)


def get_short_ranking_snapshot(week_uid: str) -> List[Dict]:
    """Return the complete stored ranking for one exact ISO week."""

    session = SessionLocal()

    try:
        rows = (
            session.query(ShortRankingSnapshot)
            .filter(ShortRankingSnapshot.week_uid == week_uid)
            .order_by(ShortRankingSnapshot.rank.asc())
            .all()
        )

        return [
            {
                "week_uid": row.week_uid,
                "snapshot_date": row.snapshot_date,
                "security_key": row.security_key,
                "issuer": row.issuer,
                "isin": row.isin,
                "ticker": row.ticker,
                "total_pct": row.total_pct,
                "holder_count": row.holder_count,
                "rank": row.rank,
            }
            for row in rows
        ]

    finally:
        session.close()


def short_ranking_snapshot_exists(week_uid: str) -> bool:
    """Return True when this week's successful publication was already stored."""

    session = SessionLocal()

    try:
        return (
            session.query(ShortRankingSnapshot.id)
            .filter(ShortRankingSnapshot.week_uid == week_uid)
            .first()
            is not None
        )

    finally:
        session.close()


def save_short_ranking_snapshot(
    week_uid: str,
    snapshot_date: str,
    ranking: List[Dict],
) -> int:
    """Persist all aggregated issuers for the week; safe to call repeatedly."""

    session = SessionLocal()
    inserted = 0

    try:
        existing_keys = {
            key
            for (key,) in (
                session.query(ShortRankingSnapshot.security_key)
                .filter(ShortRankingSnapshot.week_uid == week_uid)
                .all()
            )
        }

        for row in ranking:
            security_key = str(
                row.get("security_key") or ""
            ).strip()

            if not security_key or security_key in existing_keys:
                continue

            session.add(
                ShortRankingSnapshot(
                    week_uid=week_uid,
                    snapshot_date=snapshot_date,
                    security_key=security_key,
                    issuer=str(row.get("issuer") or ""),
                    isin=row.get("isin"),
                    ticker=row.get("ticker"),
                    total_pct=float(
                        row.get("total_pct") or 0.0
                    ),
                    holder_count=int(
                        row.get("holder_count") or 0
                    ),
                    rank=int(
                        row.get("rank") or 0
                    ),
                )
            )

            existing_keys.add(
                security_key
            )

            inserted += 1

        session.commit()

        return inserted

    except Exception:
        session.rollback()
        raise

    finally:
        session.close()


def get_daily_overview_fingerprint(
    report_date: str,
) -> Optional[str]:
    """
    Return the last successfully published fingerprint
    for report_date.
    """

    session = SessionLocal()

    try:
        row = (
            session.query(DailyShortOverviewState)
            .filter(
                DailyShortOverviewState.report_date
                == report_date
            )
            .first()
        )

        return (
            row.fingerprint
            if row
            else None
        )

    finally:
        session.close()


def save_daily_overview_state(
    report_date: str,
    fingerprint: str,
    post_slug: str,
) -> None:
    """
    Insert or update state only after a successful
    WordPress create/update.
    """

    session = SessionLocal()

    try:
        row = (
            session.query(DailyShortOverviewState)
            .filter(
                DailyShortOverviewState.report_date
                == report_date
            )
            .first()
        )

        if row:
            row.fingerprint = fingerprint
            row.post_slug = post_slug
            row.updated_at = datetime.utcnow()

        else:
            session.add(
                DailyShortOverviewState(
                    report_date=report_date,
                    fingerprint=fingerprint,
                    post_slug=post_slug,
                )
            )

        session.commit()

    except Exception:
        session.rollback()
        raise

    finally:
        session.close()
