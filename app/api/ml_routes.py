"""Routes extracted from routes.py — ml_routes.py."""

from __future__ import annotations

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema import MLModel, User
from app.utils.database import get_db
from app.security.auth import get_current_user, require_user_ownership
from app.services.ml_predictor.service import MLPredictorService

log = structlog.get_logger()

router = APIRouter()

ml_predictor_svc = MLPredictorService()


@router.post("/ml/train/{user_id}")
async def train_model(
    user_id: int,
    current_user: User = Depends(get_current_user),  # ✅ SECURITY
    db: AsyncSession = Depends(get_db),
):
    """Trigger model training for a specific user."""
    require_user_ownership(current_user.id, user_id, "ml_training")  # ✅ SECURITY
    
    user = (await db.execute(select(User).where(User.id == user_id))).scalar_one_or_none()
    if not user:
        raise HTTPException(404, "User not found")
    if not user.simkl_access_token:
        raise HTTPException(400, "User not linked to Simkl")
    result = await ml_predictor_svc.train_for_user(user)
    return result


@router.get("/ml/predictions/{user_id}")
async def get_predictions(
    user_id: int,
    limit: int = Query(50, ge=1, le=500),  # ✅ SECURITY: Input validation
    current_user: User = Depends(get_current_user),  # ✅ SECURITY
):
    require_user_ownership(current_user.id, user_id, "predictions")  # ✅ SECURITY
    return await ml_predictor_svc.get_predictions(user_id, limit)


@router.get("/ml/model/{user_id}")
async def get_model_info(
    user_id: int,
    current_user: User = Depends(get_current_user),  # ✅ SECURITY
    db: AsyncSession = Depends(get_db),
):
    require_user_ownership(current_user.id, user_id, "model_info")  # ✅ SECURITY
    
    model = (await db.execute(
        select(MLModel).where(MLModel.user_id == user_id, MLModel.is_active == True)
    )).scalar_one_or_none()
    if not model:
        return {"status": "no_model"}

    # Genre insights from the latest bias analysis, if one exists
    genre_insights = None
    try:
        from app.models.schema import RatingBias
        bias = (await db.execute(
            select(RatingBias).where(RatingBias.user_id == user_id)
        )).scalar_one_or_none()
        if bias and bias.analysis_json:
            genre_stats = bias.analysis_json.get("genre_biases", {})
            top = sorted(genre_stats.items(), key=lambda kv: kv[1].get("count", 0), reverse=True)[:6]
            genre_insights = {
                genre: {
                    "delta": s.get("bias_score", 0.0),
                    "user_avg": s.get("avg", 0.0),
                    "simkl_avg": round(s.get("avg", 0.0) - s.get("bias_score", 0.0), 1),
                }
                for genre, s in top
            }
    except Exception:
        genre_insights = None

    import math
    def _finite(v):
        return v if (v is not None and isinstance(v, (int, float)) and math.isfinite(v)) else None
    mae_val = _finite(model.mae)
    r2_val = _finite(model.r2)

    return {
        "version": model.version,
        "training_samples": model.training_samples,
        "mae": mae_val,
        "r2": r2_val,
        "accuracy": r2_val,
        "feature_count": model.feature_count,
        "genre_insights": genre_insights,
        "trained_at": model.trained_at.isoformat() if model.trained_at else None,
    }


@router.get("/ml/drift/{user_id}")
async def get_rating_drift(
    user_id: int,
    current_user: User = Depends(get_current_user),
):
    """Return taste drift data — how feature importances shifted over training runs."""
    require_user_ownership(current_user.id, user_id, "drift")
    return await ml_predictor_svc.get_drift(user_id)


# ═══════════════════════════════════════════════════════════════════════════
