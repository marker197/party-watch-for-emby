"""Service #2 — ML Rating Predictor.

1. Fetches user's complete Trakt rating history
2. Extracts feature vectors:
   - Genres (one-hot, 21 genres)
   - Year, decade, runtime
   - Item type (movie/show)
   - Actors (top-N frequent from user's history, one-hot)
   - Directors (top-N frequent, one-hot)
   - Studios/Networks (top-N frequent, one-hot)
   - Trakt community rating
3. Trains a gradient-boosted regressor per user
4. Predicts ratings for every unwatched item in Emby library
5. Stores predictions + confidence + human-readable explanations
"""

from __future__ import annotations

import os
import json
from collections import Counter
from datetime import datetime, timezone
from typing import Any

import joblib
import numpy as np
import pandas as pd
import structlog
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import cross_val_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MultiLabelBinarizer, StandardScaler, OneHotEncoder
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.schema import MLModel, Prediction, User, UserRating
from app.utils.trakt_client import TraktClient
from app.utils.emby_client import EmbyClient
from app.utils.library_cache import LibraryCache
from app.utils.redis_cache import get_redis
from app.utils.database import async_session

log = structlog.get_logger()

MODELS_DIR = "/app/models"
ALL_GENRES = [
    "action", "adventure", "animation", "anime", "comedy", "crime",
    "documentary", "drama", "family", "fantasy", "history", "horror",
    "music", "mystery", "romance", "science-fiction", "sport",
    "superhero", "thriller", "war", "western",
]
TOP_N_ACTORS = 30      # one-hot encode top-30 most frequent actors
TOP_N_DIRECTORS = 15   # one-hot encode top-15 most frequent directors
TOP_N_STUDIOS = 10     # one-hot encode top-10 most frequent studios

# Emby/TMDB genre labels → canonical Trakt-style tokens used by the model
GENRE_ALIASES = {
    "science fiction": ["science-fiction"],
    "sci-fi": ["science-fiction"],
    "sci-fi & fantasy": ["science-fiction", "fantasy"],
    "action & adventure": ["action", "adventure"],
    "war & politics": ["war"],
    "children": ["family"],
    "kids": ["family"],
    "musical": ["music"],
    "suspense": ["thriller"],
    "tv movie": [],
    "mini-series": [],
    "reality": [],
    "talk": [],
    "news": [],
}


def normalize_genres(genres: list) -> list[str]:
    """Lowercase and map genre labels onto the model's canonical vocabulary."""
    out: list[str] = []
    for g in genres or []:
        if not isinstance(g, str):
            continue
        g = g.lower().strip()
        mapped = GENRE_ALIASES.get(g, [g])
        for m in mapped:
            if m in ALL_GENRES and m not in out:
                out.append(m)
    return out


class MLPredictorService:
    def __init__(self):
        self.emby = EmbyClient()
        self._genre_binarizer = MultiLabelBinarizer(classes=ALL_GENRES)
        self._genre_binarizer.fit([ALL_GENRES])
        # These get populated per-user during training
        self._top_actors: list[str] = []
        self._top_directors: list[str] = []
        self._top_studios: list[str] = []

    # -----------------------------------------------------------------------
    # Public entry points
    # -----------------------------------------------------------------------

    async def train_for_all_users(self):
        """Scheduler entry point — retrain models for every linked user."""
        log.info("ml_predictor.train_start")
        async with async_session() as db:
            users = (await db.execute(
                select(User).where(User.trakt_access_token.isnot(None))
            )).scalars().all()

        for user in users:
            try:
                await self.train_for_user(user)
            except Exception:
                log.exception("ml_predictor.train_error", user_id=user.id)

        log.info("ml_predictor.train_complete", users=len(users))

    async def train_for_user(self, user: User) -> dict:
        """Full pipeline: fetch → cache → featurize → train → predict → persist."""
        # Token refresh callback
        async def on_token_refresh(access, refresh, expires):
            async with async_session() as db:
                u = (await db.execute(select(User).where(User.id == user.id))).scalar_one()
                u.trakt_access_token = access
                u.trakt_refresh_token = refresh
                u.trakt_token_expires = expires
                await db.commit()

        trakt = TraktClient(
            access_token=user.trakt_access_token,
            refresh_token=user.trakt_refresh_token,
            token_expires=user.trakt_token_expires,
            token_refresh_callback=on_token_refresh,
        )
        try:
            # 1. Fetch and cache ratings
            ratings = await self._fetch_and_cache_ratings(trakt, user)
            # Minimum ratings for training: configurable via ML_MIN_RATINGS
            # (default 5, hard floor 3 for cross-validation). More ratings =
            # better predictions; below ~15 expect rough results.
            min_ratings = max(3, int(os.environ.get("ML_MIN_RATINGS", "5")))
            if len(ratings) < min_ratings:
                log.warning("ml_predictor.too_few_ratings", user=user.emby_username, count=len(ratings))
                return {
                    "status": "skipped",
                    "reason": f"only {len(ratings)} rating(s) found — need at least {min_ratings}. "
                              f"Rate more items on Trakt, then retrain.",
                }

            # 2. Enrich with Emby metadata (actors, directors, studios)
            enriched = await self._enrich_with_emby(ratings)

            # 3. Discover top actors/directors/studios from user's history
            self._compute_top_people(enriched)

            # 4. Build features + labels
            df = self._build_dataframe(enriched)
            X, y, feature_names = self._featurize(df)

            # 5. Train model
            pipeline = self._create_pipeline(feature_names)
            pipeline.fit(X, y)

            # 6. Cross-validate (metrics can be NaN with very few samples —
            # sanitize to None so DB values and JSON responses stay valid)
            cv_folds = min(5, len(y))
            scores = cross_val_score(pipeline, X, y, cv=cv_folds, scoring="neg_mean_absolute_error")
            mae = -scores.mean()
            r2_scores = cross_val_score(pipeline, X, y, cv=cv_folds, scoring="r2")
            r2 = r2_scores.mean()
            mae = round(float(mae), 3) if np.isfinite(mae) else None
            r2 = round(float(r2), 3) if np.isfinite(r2) else None

            # 7. Save model + metadata
            os.makedirs(MODELS_DIR, exist_ok=True)
            model_path = os.path.join(MODELS_DIR, f"user_{user.id}.pkl")
            joblib.dump({
                "pipeline": pipeline,
                "feature_names": feature_names,
                "top_actors": self._top_actors,
                "top_directors": self._top_directors,
                "top_studios": self._top_studios,
            }, model_path)

            # 8. Record in DB
            async with async_session() as db:
                old = (await db.execute(
                    select(MLModel).where(MLModel.user_id == user.id, MLModel.is_active == True)
                )).scalars().all()
                for m in old:
                    m.is_active = False
                db.add(MLModel(
                    user_id=user.id,
                    training_samples=len(y),
                    mae=mae,
                    r2=r2,
                    feature_count=len(feature_names),
                    model_path=model_path,
                    is_active=True,
                ))
                await db.commit()

            # 9. Snapshot feature importances for drift tracking
            await self._snapshot_drift(user.id, pipeline, feature_names)

            # 10. Predict for unwatched library
            await self._predict_library(user, pipeline, feature_names)

            result = {
                "status": "trained",
                "samples": len(y),
                "mae": mae,
                "r2": r2,
                "features": len(feature_names),
                "top_actors": self._top_actors[:5],
            }
            log.info("ml_predictor.trained", user=user.emby_username, **result)
            return result
        finally:
            await trakt.close()

    async def get_predictions(self, user_id: int, limit: int = 50) -> list[dict]:
        async with async_session() as db:
            rows = (await db.execute(
                select(Prediction)
                .where(Prediction.user_id == user_id)
                .order_by(
                    Prediction.predicted_rating.desc(),
                    Prediction.confidence.desc(),
                    Prediction.title.asc(),
                )
                .limit(limit)
            )).scalars().all()
        return [
            {
                "emby_item_id": r.emby_item_id,
                "title": r.title,
                "predicted_rating": r.predicted_rating,
                "confidence": r.confidence,
                "explanation": r.explanation,
                "overview": r.overview or "",
            }
            for r in rows
        ]

    # -----------------------------------------------------------------------
    # Fetch & cache Trakt ratings
    # -----------------------------------------------------------------------

    async def _fetch_and_cache_ratings(self, trakt: TraktClient, user: User) -> list[dict]:
        raw_ratings = await trakt.get_user_ratings(kind="all")

        rows = []
        for entry in raw_ratings:
            item = entry.get("movie") or entry.get("show") or {}
            rows.append({
                "trakt_id": str(item.get("ids", {}).get("trakt", "")),
                "trakt_slug": item.get("ids", {}).get("slug", ""),
                "title": item.get("title", ""),
                "item_type": "movie" if "movie" in entry else "show",
                "rating": entry.get("rating", 0),
                "genres": item.get("genres", []),
                "year": item.get("year"),
                "runtime": item.get("runtime"),
                "trakt_rating": item.get("rating"),  # community rating
                "ids": item.get("ids", {}),
                "rated_at": entry.get("rated_at"),
            })

        # persist to DB
        async with async_session() as db:
            await db.execute(delete(UserRating).where(UserRating.user_id == user.id))
            for r in rows:
                db.add(UserRating(
                    user_id=user.id,
                    trakt_id=r["trakt_id"],
                    trakt_slug=r["trakt_slug"],
                    title=r["title"],
                    item_type=r["item_type"],
                    rating=r["rating"],
                    genres=r["genres"],
                    year=r["year"],
                    runtime=r["runtime"],
                    trakt_rating=r.get("trakt_rating"),
                    rated_at=(
                        datetime.fromisoformat(r["rated_at"].replace("Z", "+00:00"))
                        .astimezone(timezone.utc)
                        .replace(tzinfo=None)
                        if r.get("rated_at") else None
                    ),
                ))
            await db.commit()

        return rows

    # -----------------------------------------------------------------------
    # Enrich with Emby metadata (actors, directors, studios)
    # -----------------------------------------------------------------------

    async def _enrich_with_emby(self, ratings: list[dict]) -> list[dict]:
        """Look up each rated item in Emby and extract People + Studios."""
        enriched = []
        for r in ratings:
            item_data = {
                **r,
                "actors": [], "directors": [], "studios": [],
                # Trakt community rating (extended=full) covers items not in the library
                "community_rating": r.get("trakt_rating") or 0,
            }

            # Try library cache first
            ids = r.get("ids", {})
            emby_match = None
            for provider_type, key in [("Tmdb", "tmdb"), ("Imdb", "imdb"), ("Tvdb", "tvdb")]:
                pid = ids.get(key)
                if pid:
                    emby_match = await LibraryCache.find_by_provider_id(provider_type, str(pid))
                    if emby_match:
                        break

            if not emby_match and r.get("title"):
                emby_match = await LibraryCache.find_by_title(r["title"], year=r.get("year"))

            # If in library, fetch full Emby metadata for People/Studios
            if emby_match:
                try:
                    full_item = await self.emby.get_item_safe(emby_match["emby_id"])
                    if not full_item:
                        raise ValueError("item lookup failed")
                    people = full_item.get("People", [])
                    item_data["actors"] = [
                        p.get("Name", "") for p in people
                        if p.get("Type") == "Actor"
                    ][:5]
                    item_data["directors"] = [
                        p.get("Name", "") for p in people
                        if p.get("Type") == "Director"
                    ]
                    item_data["studios"] = [
                        s.get("Name", "") for s in full_item.get("Studios", [])
                    ][:3]
                    if full_item.get("CommunityRating"):
                        item_data["community_rating"] = full_item["CommunityRating"]
                except Exception:
                    pass

            enriched.append(item_data)

        enriched_count = sum(1 for e in enriched if e["actors"])
        log.info("ml_predictor.enriched", total=len(enriched), with_people=enriched_count)
        return enriched

    def _compute_top_people(self, enriched: list[dict]):
        """Find the most frequent actors/directors/studios in user's ratings."""
        actor_counts = Counter()
        director_counts = Counter()
        studio_counts = Counter()

        for item in enriched:
            for a in item.get("actors", []):
                if a:
                    actor_counts[a] += 1
            for d in item.get("directors", []):
                if d:
                    director_counts[d] += 1
            for s in item.get("studios", []):
                if s:
                    studio_counts[s] += 1

        self._top_actors = [name for name, _ in actor_counts.most_common(TOP_N_ACTORS)]
        self._top_directors = [name for name, _ in director_counts.most_common(TOP_N_DIRECTORS)]
        self._top_studios = [name for name, _ in studio_counts.most_common(TOP_N_STUDIOS)]

        log.debug(
            "ml_predictor.top_people",
            actors=self._top_actors[:5],
            directors=self._top_directors[:3],
            studios=self._top_studios[:3],
        )

    # -----------------------------------------------------------------------
    # Feature engineering
    # -----------------------------------------------------------------------

    def _build_dataframe(self, ratings: list[dict]) -> pd.DataFrame:
        df = pd.DataFrame(ratings)
        df["year"] = pd.to_numeric(df["year"], errors="coerce").fillna(2000).astype(int)
        df["runtime"] = pd.to_numeric(df["runtime"], errors="coerce").fillna(90).astype(int)
        df["decade"] = (df["year"] // 10) * 10
        df["is_movie"] = (df["item_type"] == "movie").astype(int)
        df["community_rating"] = pd.to_numeric(
            df.get("community_rating", pd.Series([0] * len(df))),
            errors="coerce"
        ).fillna(0)
        return df

    def _featurize(self, df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray, list[str]]:
        # Genre one-hot
        genre_matrix = self._genre_binarizer.transform(
            df["genres"].apply(lambda g: normalize_genres(g) if isinstance(g, list) else [])
        )
        genre_df = pd.DataFrame(genre_matrix, columns=[f"genre_{g}" for g in ALL_GENRES])

        # Numeric features
        numeric = df[["year", "runtime", "decade", "is_movie", "community_rating"]].reset_index(drop=True)

        # Actor one-hot
        actor_cols = []
        for actor_name in self._top_actors:
            col_name = f"actor_{actor_name.replace(' ', '_').lower()}"
            col_values = df["actors"].apply(
                lambda a: 1 if isinstance(a, list) and actor_name in a else 0
            ).reset_index(drop=True)
            actor_cols.append(col_values.rename(col_name))

        # Director one-hot
        director_cols = []
        for dir_name in self._top_directors:
            col_name = f"director_{dir_name.replace(' ', '_').lower()}"
            col_values = df["directors"].apply(
                lambda d: 1 if isinstance(d, list) and dir_name in d else 0
            ).reset_index(drop=True)
            director_cols.append(col_values.rename(col_name))

        # Studio one-hot
        studio_cols = []
        for studio_name in self._top_studios:
            col_name = f"studio_{studio_name.replace(' ', '_').lower()}"
            col_values = df["studios"].apply(
                lambda s: 1 if isinstance(s, list) and studio_name in s else 0
            ).reset_index(drop=True)
            studio_cols.append(col_values.rename(col_name))

        # Combine all features
        parts = [numeric, genre_df.reset_index(drop=True)]
        if actor_cols:
            parts.append(pd.concat(actor_cols, axis=1))
        if director_cols:
            parts.append(pd.concat(director_cols, axis=1))
        if studio_cols:
            parts.append(pd.concat(studio_cols, axis=1))

        features = pd.concat(parts, axis=1)
        feature_names = list(features.columns)

        X = features.values.astype(float)
        y = df["rating"].values.astype(float)

        log.info("ml_predictor.features", count=len(feature_names),
                 genres=len(ALL_GENRES), actors=len(self._top_actors),
                 directors=len(self._top_directors), studios=len(self._top_studios))
        return X, y, feature_names

    def _create_pipeline(self, feature_names: list[str]) -> Pipeline:
        return Pipeline([
            ("scaler", StandardScaler()),
            ("model", GradientBoostingRegressor(
                n_estimators=200,
                max_depth=4,
                learning_rate=0.1,
                subsample=0.8,
                random_state=42,
            )),
        ])

    # -----------------------------------------------------------------------
    # Predict for every unwatched library item
    # -----------------------------------------------------------------------

    async def _predict_library(self, user: User, pipeline: Pipeline, feature_names: list[str]):
        movies = await self.emby.get_all_movies(user_id=user.emby_user_id)
        series = await self.emby.get_all_series(user_id=user.emby_user_id)
        all_items = movies + series

        # Global feature importances from the trained model — used so each
        # item's explanation reflects what this user's model actually learned
        # matters, instead of always listing the same fixed metadata fields.
        try:
            importances = pipeline.named_steps["model"].feature_importances_
        except Exception:
            importances = np.zeros(len(feature_names))

        predictions = []
        for item in all_items:
            if item.get("UserData", {}).get("Played", False):
                continue

            features = self._item_to_features(item, feature_names)
            if features is None:
                continue

            pred = pipeline.predict([features])[0]
            pred = max(1.0, min(10.0, pred))

            explanation = self._explain(item, pred, features, importances, feature_names)

            predictions.append({
                "emby_item_id": item["Id"],
                "title": item.get("Name", ""),
                "predicted_rating": round(pred, 1),
                "confidence": self._confidence(features, pipeline),
                "explanation": explanation,
                "overview": item.get("Overview", ""),
                "features": features.tolist(),
            })

        async with async_session() as db:
            await db.execute(delete(Prediction).where(Prediction.user_id == user.id))
            for p in predictions:
                db.add(Prediction(
                    user_id=user.id,
                    emby_item_id=p["emby_item_id"],
                    title=p["title"],
                    predicted_rating=p["predicted_rating"],
                    confidence=p["confidence"],
                    explanation=p["explanation"],
                    overview=p.get("overview", ""),
                    features_json=p["features"],
                ))
            await db.commit()

        log.info("ml_predictor.predictions_saved", user=user.emby_username, count=len(predictions))

    def _item_to_features(self, item: dict, feature_names: list[str]) -> np.ndarray | None:
        try:
            year = item.get("ProductionYear", 2000) or 2000
            runtime = (item.get("RunTimeTicks", 0) or 0) // 600_000_000
            if runtime == 0:
                runtime = 90
            decade = (year // 10) * 10
            is_movie = 1 if item.get("Type") == "Movie" else 0
            community_rating = item.get("CommunityRating", 0) or 0

            genres_raw = normalize_genres(item.get("Genres", []))
            genre_vec = self._genre_binarizer.transform([genres_raw])[0]

            numeric = np.array([year, runtime, decade, is_movie, community_rating], dtype=float)

            # People features
            people = item.get("People", [])
            actors = [p.get("Name", "") for p in people if p.get("Type") == "Actor"]
            directors = [p.get("Name", "") for p in people if p.get("Type") == "Director"]
            studios = [s.get("Name", "") for s in item.get("Studios", [])]

            actor_vec = np.array([1 if a in actors else 0 for a in self._top_actors], dtype=float)
            director_vec = np.array([1 if d in directors else 0 for d in self._top_directors], dtype=float)
            studio_vec = np.array([1 if s in studios else 0 for s in self._top_studios], dtype=float)

            full_vec = np.concatenate([numeric, genre_vec, actor_vec, director_vec, studio_vec])

            # Ensure vector length matches feature_names
            if len(full_vec) != len(feature_names):
                # Pad or truncate to match
                if len(full_vec) < len(feature_names):
                    full_vec = np.pad(full_vec, (0, len(feature_names) - len(full_vec)))
                else:
                    full_vec = full_vec[:len(feature_names)]

            return full_vec
        except Exception:
            return None

    def _confidence(self, features: np.ndarray, pipeline: Pipeline) -> float:
        """Rough confidence based on ensemble's staged prediction variance."""
        try:
            scaler = pipeline.named_steps["scaler"]
            model = pipeline.named_steps["model"]
            scaled = scaler.transform([features])
            staged = list(model.staged_predict(scaled))
            if len(staged) > 10:
                last_10 = [s[0] for s in staged[-10:]]
                variance = np.var(last_10)
                confidence = max(0.3, min(1.0, 1.0 - variance))
                return round(confidence, 2)
        except Exception:
            pass
        return 0.5

    def _explain(
        self,
        item: dict,
        predicted: float,
        features: np.ndarray | None = None,
        importances: np.ndarray | None = None,
        feature_names: list[str] | None = None,
    ) -> str:
        """Human-readable explanation of the prediction.

        Ranks this item's *active* categorical features (genre/actor/director/
        studio one-hot columns that are actually set to 1 for this item) by
        the trained model's global feature_importances_, so the explanation
        names the factors this user's model has actually learned to weight
        heavily — rather than always reciting the same fixed metadata fields
        regardless of whether they influenced the score.
        """
        reasons: list[tuple[float, str]] = []

        if features is not None and importances is not None and feature_names is not None \
                and len(features) == len(importances) == len(feature_names):
            for name, value, importance in zip(feature_names, features, importances):
                if value <= 0 or importance <= 0:
                    continue
                if name.startswith("genre_"):
                    label = f"the {name[len('genre_'):].replace('-', ' ')} genre"
                elif name.startswith("actor_"):
                    label = f"actor {name[len('actor_'):].replace('_', ' ').title()}"
                elif name.startswith("director_"):
                    label = f"director {name[len('director_'):].replace('_', ' ').title()}"
                elif name.startswith("studio_"):
                    label = f"studio {name[len('studio_'):].replace('_', ' ').title()}"
                else:
                    continue  # numeric features (year/runtime/decade/community) handled separately below
                reasons.append((float(importance), label))

        reasons.sort(key=lambda r: r[0], reverse=True)
        top_reasons = [label for _, label in reasons[:3]]

        parts = []
        if top_reasons:
            parts.append(f"driven mostly by {', '.join(top_reasons)}")
        else:
            # Fallback when nothing scored (e.g. cold-start model, no matched people/genres)
            genres = item.get("Genres", [])
            if genres:
                parts.append(f"genres: {', '.join(genres[:3])}")

        community = item.get("CommunityRating")
        if community:
            parts.append(f"community rating {community:.1f}/10")

        year = item.get("ProductionYear")
        if year:
            parts.append(f"released {year}")

        parts.append(f"predicted {predicted:.1f}/10")
        return f"Based on {'; '.join(parts)}"

    # -----------------------------------------------------------------------
    # Rating Drift Tracker
    # -----------------------------------------------------------------------

    DRIFT_MAX_SNAPSHOTS = 52  # ~1 year of weekly retrains

    async def _snapshot_drift(self, user_id: int, pipeline: Pipeline, feature_names: list[str]) -> None:
        """Persist a timestamped snapshot of feature importances to Redis.

        Keeps the last DRIFT_MAX_SNAPSHOTS entries per user so the dashboard
        can show how taste has shifted over time.
        """
        try:
            importances = pipeline.named_steps["model"].feature_importances_
        except Exception:
            return

        if len(importances) != len(feature_names):
            return

        # Build a readable dict of importances grouped by category
        snapshot: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "genres": {},
            "actors": {},
            "directors": {},
            "studios": {},
            "numeric": {},
        }

        for name, imp in zip(feature_names, importances):
            imp_val = round(float(imp), 5)
            if imp_val <= 0:
                continue
            if name.startswith("genre_"):
                snapshot["genres"][name[len("genre_"):]] = imp_val
            elif name.startswith("actor_"):
                snapshot["actors"][name[len("actor_"):]] = imp_val
            elif name.startswith("director_"):
                snapshot["directors"][name[len("director_"):]] = imp_val
            elif name.startswith("studio_"):
                snapshot["studios"][name[len("studio_"):]] = imp_val
            else:
                snapshot["numeric"][name] = imp_val

        try:
            r = await get_redis()
            key = f"ml_drift:{user_id}"
            raw = await r.get(key)
            history = json.loads(raw) if raw else []
            history.append(snapshot)
            # Keep only the most recent snapshots
            history = history[-self.DRIFT_MAX_SNAPSHOTS:]
            await r.set(key, json.dumps(history))
            log.info("ml_predictor.drift_snapshot", user_id=user_id, snapshots=len(history))
        except Exception:
            log.warning("ml_predictor.drift_snapshot_failed", user_id=user_id)

    async def get_drift(self, user_id: int) -> dict:
        """Return drift data: historical snapshots + computed changes.

        Response: {snapshots: [...], changes: [...], summary: str}
        """
        try:
            r = await get_redis()
            raw = await r.get(f"ml_drift:{user_id}")
            snapshots = json.loads(raw) if raw else []
        except Exception:
            snapshots = []

        if len(snapshots) < 2:
            return {
                "snapshots": snapshots,
                "changes": [],
                "summary": "Need at least 2 training runs to detect drift. Retrain again next week.",
            }

        # Compare latest vs earliest
        oldest = snapshots[0]
        latest = snapshots[-1]

        changes = []

        # Genre drift
        all_genres = set(list(oldest.get("genres", {}).keys()) + list(latest.get("genres", {}).keys()))
        for genre in sorted(all_genres):
            old_val = oldest.get("genres", {}).get(genre, 0)
            new_val = latest.get("genres", {}).get(genre, 0)
            if old_val == 0 and new_val == 0:
                continue
            delta = new_val - old_val
            if abs(delta) < 0.001:
                continue
            pct = (delta / old_val * 100) if old_val > 0 else 100
            direction = "up" if delta > 0 else "down"
            changes.append({
                "category": "genre",
                "name": genre.replace("-", " ").title(),
                "old_importance": round(old_val, 4),
                "new_importance": round(new_val, 4),
                "delta": round(delta, 4),
                "pct_change": round(pct, 1),
                "direction": direction,
            })

        # Top movers from other categories
        for cat in ("actors", "directors", "studios"):
            all_keys = set(list(oldest.get(cat, {}).keys()) + list(latest.get(cat, {}).keys()))
            for key in sorted(all_keys):
                old_val = oldest.get(cat, {}).get(key, 0)
                new_val = latest.get(cat, {}).get(key, 0)
                delta = new_val - old_val
                if abs(delta) < 0.002:
                    continue
                pct = (delta / old_val * 100) if old_val > 0 else 100
                changes.append({
                    "category": cat.rstrip("s"),
                    "name": key.replace("_", " ").title(),
                    "old_importance": round(old_val, 4),
                    "new_importance": round(new_val, 4),
                    "delta": round(delta, 4),
                    "pct_change": round(pct, 1),
                    "direction": "up" if delta > 0 else "down",
                })

        # Sort by absolute delta descending
        changes.sort(key=lambda c: abs(c["delta"]), reverse=True)

        # Add plain-English description per change
        for c in changes:
            c["description"] = self._describe_change(c)

        # Build summary string in plain English
        top_up = [c for c in changes if c["direction"] == "up"][:3]
        top_down = [c for c in changes if c["direction"] == "down"][:3]

        # Use actual training count from DB (model version), not snapshot window
        total_runs = len(snapshots)
        try:
            from app.utils.database import async_session
            from app.models.schema import MLModel
            async with async_session() as db:
                latest_model = (await db.execute(
                    select(MLModel)
                    .where(MLModel.user_id == user_id, MLModel.is_active == True)
                    .order_by(MLModel.version.desc())
                    .limit(1)
                )).scalar_one_or_none()
                if latest_model and latest_model.version:
                    total_runs = latest_model.version
        except Exception:
            pass

        run_label = f"{total_runs} training run{'s' if total_runs != 1 else ''}"

        sentences = []
        if top_up:
            if len(top_up) == 1:
                sentences.append(f"You've been leaning more towards {top_up[0]['name'].lower()} content.")
            else:
                rising = ", ".join(c["name"] for c in top_up[:-1]) + f" and {top_up[-1]['name']}"
                sentences.append(f"Your taste is shifting towards {rising.lower()}.")
        if top_down:
            if len(top_down) == 1:
                sentences.append(f"{top_down[0]['name']} is less influential in your ratings than before.")
            else:
                falling = ", ".join(c["name"] for c in top_down[:-1]) + f" and {top_down[-1]['name']}"
                sentences.append(f"{falling} matter less to your ratings now.")
        if not sentences:
            summary = "Your watching taste has been steady — no significant shifts detected."
        else:
            summary = " ".join(sentences) + f" (based on {run_label})"

        return {
            "snapshots": snapshots,
            "changes": changes[:20],  # top 20 movers
            "summary": summary,
        }

    @staticmethod
    def _describe_change(c: dict) -> str:
        """Generate a varied plain-English one-liner for a single drift change."""
        name = c["name"]
        cat = c["category"]
        direction = c["direction"]
        pct = abs(c.get("pct_change", 0))

        # ── Genre ────────────────────────────────────────────────────────
        if cat == "genre":
            if direction == "up":
                if pct >= 100:
                    return f"Big shift — {name} has jumped to one of the strongest drivers of your ratings."
                if pct >= 40:
                    return f"You're clearly gravitating towards {name} content lately."
                if pct >= 15:
                    return f"{name} is starting to carry a bit more weight in what you enjoy."
                return f"A small nudge towards {name} — not a major change yet."
            else:
                if pct >= 100:
                    return f"{name} used to matter a lot more — it's dropped off significantly."
                if pct >= 40:
                    return f"You seem to be moving away from {name} content."
                if pct >= 15:
                    return f"{name} is playing a smaller role in your taste than it used to."
                return f"{name} dipped slightly, but it's still in the mix."

        # ── Actor ────────────────────────────────────────────────────────
        if cat == "actor":
            if direction == "up":
                if pct >= 100:
                    return f"{name} has become a strong signal — you consistently rate their work higher."
                if pct >= 40:
                    return f"You're clearly enjoying {name}'s films more these days."
                if pct >= 15:
                    return f"{name} is starting to positively influence your ratings."
                return f"A slight lean towards {name} — early days."
            else:
                if pct >= 100:
                    return f"{name} used to boost your ratings but that effect has faded."
                if pct >= 40:
                    return f"{name}'s presence matters less to your scores than before."
                if pct >= 15:
                    return f"{name} is a bit less of a draw for you now."
                return f"Tiny dip for {name} — nothing dramatic."

        # ── Director ─────────────────────────────────────────────────────
        if cat == "director":
            if direction == "up":
                if pct >= 100:
                    return f"You've developed a real appreciation for {name}'s work."
                if pct >= 40:
                    return f"{name}'s films are landing better with you than they used to."
                if pct >= 15:
                    return f"You're warming up to {name} as a director."
                return f"A minor uptick for {name} — keep watching to see if it holds."
            else:
                if pct >= 100:
                    return f"{name}'s influence on your ratings has dropped sharply."
                if pct >= 40:
                    return f"You're less swayed by {name}'s name on a project now."
                if pct >= 15:
                    return f"{name} is a slightly weaker predictor of what you'll enjoy."
                return f"Small decline for {name} — still a factor though."

        # ── Studio ───────────────────────────────────────────────────────
        if cat == "studio":
            if direction == "up":
                if pct >= 100:
                    return f"{name} has become one of the strongest studio signals in your ratings."
                if pct >= 40:
                    return f"You're consistently rating {name} releases higher."
                if pct >= 15:
                    return f"{name} is gaining some ground as a taste indicator for you."
                return f"A small tick up for {name} productions."
            else:
                if pct >= 100:
                    return f"{name} used to be a reliable indicator but it's lost that edge."
                if pct >= 40:
                    return f"{name} releases don't predict your ratings as well anymore."
                if pct >= 15:
                    return f"{name} is a bit less relevant to your taste now."
                return f"Minor dip for {name} — still on the radar."

        # ── Numeric / other ──────────────────────────────────────────────
        if direction == "up":
            if pct >= 40:
                return f"{name} has become a stronger factor in predicting your ratings."
            return f"{name} is slightly more influential in predictions."
        else:
            if pct >= 40:
                return f"{name} matters less to predictions than it used to."
            return f"{name} is slightly less influential in predictions."
