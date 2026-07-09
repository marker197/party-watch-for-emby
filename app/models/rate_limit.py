"""Rate limit configuration model."""

from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean

from app.utils.database import Base


class RateLimitConfig(Base):
    """Store rate limit configurations per endpoint type."""
    
    __tablename__ = "rate_limit_configs"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # Endpoint type (auth, read, write, heavy, general, search)
    endpoint_type = Column(String(50), unique=True, index=True, nullable=False)
    
    # Rate limit string: "100/minute" or "5/hour"
    limit_value = Column(String(50), nullable=False)
    
    # Parsed rate (requests per time period)
    requests_per_period = Column(Integer, nullable=False)
    
    # Time period in seconds (60 for minute, 3600 for hour)
    period_seconds = Column(Integer, nullable=False)
    
    # Description for dashboard
    description = Column(String(255), nullable=True)
    
    # Whether this limit is enabled
    enabled = Column(Boolean, default=True, nullable=False)
    
    # Who last modified it
    modified_by = Column(String(255), nullable=True)
    
    # When it was last modified
    modified_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    created_at = Column(DateTime, default=datetime.utcnow)


__all__ = ["RateLimitConfig"]
