# models.py
from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    Enum,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    Index,
)
from sqlalchemy.orm import relationship, declarative_base
from datetime import datetime
import enum

Base = declarative_base()

# ---------------------------
# Enums
# ---------------------------
class EDLType(enum.Enum):
    IPV4 = "IPV4"
    IPV6 = "IPV6"
    FQDN = "FQDN"
    URL = "URL"
    CIDR = "CIDR"

# ---------------------------
# EDL entries
# ---------------------------
class EDLEntry(Base):
    __tablename__ = "edl_entries"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_name = Column(String(100), nullable=False)   # e.g., Azure IPv4
    entry = Column(Text, nullable=False)               # the EDL line
    type = Column(Enum(EDLType), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("entry", "type", name="uq_entry_type"),  # avoid duplicates
        Index("idx_type", "type"),
        Index("idx_source_name", "source_name"),
    )

# ---------------------------
# Firewalls
# ---------------------------
class Firewall(Base):
    __tablename__ = "firewalls"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True)   # e.g., Firewall01
    description = Column(Text)
    last_seen = Column(DateTime, default=datetime.utcnow)

    # Relationship to consumption
    consumptions = relationship("EDLConsumption", back_populates="firewall")

# ---------------------------
# Track what each firewall has consumed
# ---------------------------
class EDLConsumption(Base):
    __tablename__ = "edl_consumption"
    id = Column(Integer, primary_key=True, autoincrement=True)
    firewall_id = Column(Integer, ForeignKey("firewalls.id"), nullable=False)
    edl_entry_id = Column(Integer, ForeignKey("edl_entries.id"), nullable=False)
    consumed_at = Column(DateTime, default=datetime.utcnow)

    firewall = relationship("Firewall", back_populates="consumptions")
    edl_entry = relationship("EDLEntry")

    __table_args__ = (
        UniqueConstraint("firewall_id", "edl_entry_id", name="uq_firewall_entry"),
        Index("idx_firewall_id", "firewall_id"),
        Index("idx_edl_entry_id", "edl_entry_id"),
    )
