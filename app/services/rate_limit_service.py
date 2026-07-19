"""Rate limit configuration service."""

from typing import Optional, List
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timezone
import structlog

from app.models.rate_limit import RateLimitConfig

log = structlog.get_logger()


class RateLimitService:
    """Manage rate limit configurations in database."""
    
    DEFAULT_LIMITS = {
        "general": {"value": "100/minute", "requests": 100, "period": 60, "desc": "General endpoints"},
        "auth": {"value": "10/minute", "requests": 10, "period": 60, "desc": "Authentication endpoints (brute force protection)"},
        "heavy": {"value": "5/minute", "requests": 5, "period": 60, "desc": "Heavy operations (ML, universe scan)"},
        "search": {"value": "30/minute", "requests": 30, "period": 60, "desc": "Search/filter endpoints"},
        "write": {"value": "50/minute", "requests": 50, "period": 60, "desc": "POST/PUT operations"},
        "read": {"value": "150/minute", "requests": 150, "period": 60, "desc": "GET operations"},
    }
    
    @staticmethod
    async def initialize_defaults(db: AsyncSession):
        """Create default rate limit configs if they don't exist."""
        try:
            for endpoint_type, config in RateLimitService.DEFAULT_LIMITS.items():
                # Check if exists
                result = await db.execute(
                    select(RateLimitConfig).where(
                        RateLimitConfig.endpoint_type == endpoint_type
                    )
                )
                existing = result.scalar_one_or_none()
                
                if not existing:
                    new_config = RateLimitConfig(
                        endpoint_type=endpoint_type,
                        limit_value=config["value"],
                        requests_per_period=config["requests"],
                        period_seconds=config["period"],
                        description=config["desc"],
                        enabled=True,
                        modified_by="system_init",
                    )
                    db.add(new_config)
            
            await db.commit()
            log.info("rate_limits.defaults_initialized")
        except Exception as e:
            log.error("rate_limits.init_failed", error=str(e))
            await db.rollback()
    
    @staticmethod
    async def get_all_limits(db: AsyncSession) -> List[RateLimitConfig]:
        """Get all rate limit configurations."""
        result = await db.execute(select(RateLimitConfig).order_by(RateLimitConfig.endpoint_type))
        return result.scalars().all()
    
    @staticmethod
    async def get_limit(db: AsyncSession, endpoint_type: str) -> Optional[RateLimitConfig]:
        """Get specific rate limit configuration."""
        result = await db.execute(
            select(RateLimitConfig).where(
                RateLimitConfig.endpoint_type == endpoint_type
            )
        )
        return result.scalar_one_or_none()
    
    @staticmethod
    async def update_limit(
        db: AsyncSession,
        endpoint_type: str,
        requests_per_period: int,
        period_seconds: int,
        modified_by: str = "admin"
    ) -> RateLimitConfig:
        """Update a rate limit configuration."""
        # Parse limit string
        if period_seconds == 60:
            period_name = "minute"
        elif period_seconds == 3600:
            period_name = "hour"
        else:
            period_name = "period"
        
        limit_value = f"{requests_per_period}/{period_name}"
        
        config = await RateLimitService.get_limit(db, endpoint_type)
        if not config:
            raise ValueError(f"Rate limit config for '{endpoint_type}' not found")
        
        config.requests_per_period = requests_per_period
        config.period_seconds = period_seconds
        config.limit_value = limit_value
        config.modified_by = modified_by
        config.modified_at = datetime.now(timezone.utc)
        
        await db.commit()
        await db.refresh(config)
        
        log.info(
            "rate_limit.updated",
            endpoint_type=endpoint_type,
            new_limit=limit_value,
            modified_by=modified_by
        )
        
        return config
    
    @staticmethod
    async def toggle_limit(
        db: AsyncSession,
        endpoint_type: str,
        enabled: bool,
        modified_by: str = "admin"
    ) -> RateLimitConfig:
        """Enable or disable a rate limit."""
        config = await RateLimitService.get_limit(db, endpoint_type)
        if not config:
            raise ValueError(f"Rate limit config for '{endpoint_type}' not found")
        
        config.enabled = enabled
        config.modified_by = modified_by
        config.modified_at = datetime.now(timezone.utc)
        
        await db.commit()
        await db.refresh(config)
        
        log.info(
            "rate_limit.toggled",
            endpoint_type=endpoint_type,
            enabled=enabled,
            modified_by=modified_by
        )
        
        return config
    
    @staticmethod
    async def reset_to_defaults(db: AsyncSession, modified_by: str = "admin"):
        """Reset all rate limits to defaults."""
        for endpoint_type, config in RateLimitService.DEFAULT_LIMITS.items():
            existing = await RateLimitService.get_limit(db, endpoint_type)
            if existing:
                existing.requests_per_period = config["requests"]
                existing.period_seconds = config["period"]
                existing.limit_value = config["value"]
                existing.enabled = True
                existing.modified_by = modified_by
                existing.modified_at = datetime.now(timezone.utc)
        
        await db.commit()
        log.info("rate_limits.reset_to_defaults", modified_by=modified_by)
    
    @staticmethod
    def build_limit_string(config: RateLimitConfig) -> str:
        """Build slowapi limit string from config."""
        if config.period_seconds == 60:
            return f"{config.requests_per_period}/minute"
        elif config.period_seconds == 3600:
            return f"{config.requests_per_period}/hour"
        else:
            return f"{config.requests_per_period}/{config.period_seconds//60}minutes"


__all__ = ["RateLimitService"]
