"""Service #2 — ML Rating Predictor.

1. Fetches user's complete Simkl rating history
2. Extracts feature vectors:
   - Genres (one-hot, 21 genres)
   - Year, decade, runtime
   - Item type (movie/show)
   - Actors (top-N frequent from user's history, one-hot)
   - Directors (top-N frequent, one-hot)
   - Studios/Networks (top-N frequent, one-hot)
   - Simkl community rating
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
from app.models.schema import MLModel, Prediction, User, UserRating, AppSetting
from app.utils.simkl_client import SimklClient
from app.utils.database import async_session
from app.utils.emby_client import EmbyClient
from app.utils.library_cache import LibraryCache
from app.utils.redis_cache import get_redis

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

# Emby/TMDB genre labels → canonical Simkl-style tokens used by the model
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
        self.emby = None
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
        self.emby = EmbyClient()
        try:
            async with async_session() as db:
                users = (await db.execute(
                    select(User).where(User.simkl_access_token.isnot(None))
                )).scalars().all()

            for user in users:
                try:
                    await self.train_for_user(user)
                except Exception:
                    log.exception("ml_predictor.train_error", user_id=user.id)

            log.info("ml_predictor.train_complete", users=len(users))
        finally:
            if self.emby:
                await self.emby.close()
                self.emby = None

    async def train_for_user(self, user: User) -> dict:
        """Full pipeline: fetch → cache → featurize → train → predict → persist."""
        owned = self.emby is None
        if owned:
            self.emby = EmbyClient()
        try:
            return await self._train_for_user_inner(user)
        finally:
            if owned and self.emby:
                await self.emby.close()
                self.emby = None

    async def _train_for_user_inner(self, user: User) -> dict:
        """Internal training pipeline."""
        simkl = SimklClient(
            access_token=user.simkl_access_token,
            token_expires=user.simkl_token_expires,
        )
        try:
            # 1. Fetch and cache ratings
            ratings = await self._fetch_and_cache_ratings(simkl, user)
            # Minimum ratings for training: configurable via ML_MIN_RATINGS
            # (default 5, hard floor 3 for cross-validation). More ratings =
            # better predictions; below ~15 expect rough results.
            min_ratings = max(3, int(os.environ.get("ML_MIN_RATINGS", "5")))
            if len(ratings) < min_ratings:
                log.warning("ml_predictor.too_few_ratings", user=user.emby_username, count=len(ratings))
                return {
                    "status": "skipped",
                    "reason": f"only {len(ratings)} rating(s) found — need at least {min_ratings}. "
                              f"Rate more items on Simkl, then retrain.",
                }

            # 2. Enrich with Emby metadata (actors, directors, studios, genres)
            enriched = await self._enrich_with_emby(ratings)

            # 2b. Persist enriched genres back to DB for bias detector
            genre_updates = 0
            async with async_session() as db:
                for item in enriched:
                    if item.get("genres") and item.get("simkl_id"):
                        result = await db.execute(
                            select(UserRating).where(
                                UserRating.user_id == user.id,
                                UserRating.simkl_id == item["simkl_id"],
                            )
                        )
                        row = result.scalar_one_or_none()
                        if row and (not row.genres or row.genres == []):
                            row.genres = item["genres"]
                            genre_updates += 1
                    # Also match by title+year for MDBList items without simkl_id
                    elif item.get("genres") and item.get("title") and not item.get("simkl_id"):
                        result = await db.execute(
                            select(UserRating).where(
                                UserRating.user_id == user.id,
                                UserRating.title == item["title"],
                            ).limit(1)
                        )
                        row = result.scalar_one_or_none()
                        if row and (not row.genres or row.genres == []):
                            row.genres = item["genres"]
                            genre_updates += 1
                if genre_updates:
                    await db.commit()
                    log.info("ml_predictor.genres_backfilled", count=genre_updates)

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
                next_version = max((m.version or 0 for m in old), default=0) + 1
                for m in old:
                    m.is_active = False
                db.add(MLModel(
                    user_id=user.id,
                    version=next_version,
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
            await simkl.close()

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
        results = []
        for r in rows:
            item = {
                "emby_item_id": r.emby_item_id,
                "title": r.title,
                "predicted_rating": r.predicted_rating,
                "confidence": r.confidence,
                "explanation": r.explanation,
                "overview": r.overview or "",
                "imdb_id": None,
                "tmdb_id": None,
                "item_type": "movie",
            }
            # Resolve provider IDs from library cache
            try:
                from app.utils.library_cache import LibraryCache
                cached = None
                if r.emby_item_id:
                    # Try by emby ID first via title lookup (cache stores emby_id)
                    if r.title:
                        cached = await LibraryCache.find_by_title(r.title)
                if cached:
                    pids = cached.get("provider_ids") or cached.get("ProviderIds") or {}
                    item["imdb_id"] = pids.get("Imdb") or pids.get("imdb")
                    item["tmdb_id"] = pids.get("Tmdb") or pids.get("tmdb")
                    item_type = cached.get("type", "movie")
                    item["item_type"] = "show" if item_type == "series" else "movie"
            except Exception:
                pass
            results.append(item)
        return results

    # -----------------------------------------------------------------------
    # Fetch & cache Simkl ratings
    # -----------------------------------------------------------------------

    async def _fetch_and_cache_ratings(self, simkl: SimklClient, user: User) -> list[dict]:
        # ── Simkl ratings ──
        raw_ratings = []
        try:
            raw_ratings = await simkl.get_user_ratings(kind="all")
        except Exception as e:
            log.warning("ml_predictor.simkl_ratings_failed", error=str(e)[:120])

        rows = []
        seen_imdb: set[str] = set()
        for entry in raw_ratings:
            item = entry.get("movie") or entry.get("show") or entry
            imdb_id = item.get("ids", {}).get("imdb", "")
            if imdb_id:
                seen_imdb.add(imdb_id)
            rows.append({
                "simkl_id": str(item.get("ids", {}).get("simkl") or item.get("ids", {}).get("simkl_id") or ""),
                "simkl_slug": item.get("ids", {}).get("slug", ""),
                "title": item.get("title", ""),
                "item_type": "movie" if entry.get("_type", "").startswith("movie") or "movie" in entry else "show",
                "rating": entry.get("rating", 0),
                "genres": item.get("genres", []),
                "year": item.get("year"),
                "runtime": item.get("runtime"),
                "simkl_rating": (
                    item.get("ratings", {}).get("simkl", {}).get("rating")
                    or item.get("ratings", {}).get("mal", {}).get("rating")
                ),  # community rating from extended=full
                "ids": item.get("ids", {}),
                "rated_at": entry.get("rated_at"),
            })

        community_count = sum(1 for r in rows if r.get("simkl_rating"))
        log.info("ml_predictor.simkl_community_ratings",
                 total=len(rows), with_community=community_count)

        # ── MDBList ratings (supplement — has a rating for every watched item) ──
        mdb_added = 0
        try:
            from app.utils.mdblist_client import MDBListClient
            from app.utils.secure_redis import secure_get
            mdb_key = await secure_get("mdblist_api_key")
            log.info("ml_predictor.mdblist_attempt", has_key=bool(mdb_key),
                     simkl_ratings=len(rows))
            if mdb_key:
                mdb = MDBListClient(api_key=mdb_key)
                try:
                    mdb_ratings = await mdb.get_all_ratings()
                    log.info("ml_predictor.mdblist_ratings_raw",
                             type=type(mdb_ratings).__name__,
                             keys=list(mdb_ratings.keys()) if isinstance(mdb_ratings, dict) else "not_dict",
                             movies=len(mdb_ratings.get("movies", [])) if isinstance(mdb_ratings, dict) else 0,
                             shows=len(mdb_ratings.get("shows", [])) if isinstance(mdb_ratings, dict) else 0)
                    if isinstance(mdb_ratings, dict):
                        for kind, item_type in (("movies", "movie"), ("shows", "show")):
                            kind_items = mdb_ratings.get(kind, [])
                            if kind_items and kind == "movies":
                                first = kind_items[0] if kind_items else {}
                                log.info("ml_predictor.mdblist_first_item",
                                         keys=list(first.keys())[:15],
                                         has_movie_key=bool(first.get("movie")),
                                         has_show_key=bool(first.get("show")),
                                         inner_keys=list((first.get("movie") or first.get("show") or {}).keys())[:10])
                            for item in kind_items:
                                # MDBList wraps items: {rating, rated_at, movie: {title, ids, ...}}
                                inner = item.get("movie") or item.get("show") or item
                                # Rating from the wrapper level
                                rating = item.get("rating")
                                if rating is None and item.get("score") is not None:
                                    try:
                                        rating = round(float(item["score"]) / 10, 1)
                                    except (ValueError, TypeError):
                                        pass
                                # IDs from the inner object
                                ids = inner.get("ids", {})
                                if not isinstance(ids, dict):
                                    ids = {}
                                imdb_id = (
                                    ids.get("imdb", "")
                                    or inner.get("imdb_id", "")
                                    or inner.get("imdb", "")
                                )
                                if not rating or not imdb_id:
                                    continue
                                if imdb_id in seen_imdb:
                                    continue  # already have from Simkl
                                seen_imdb.add(imdb_id)
                                rows.append({
                                    "simkl_id": str(ids.get("simkl") or ids.get("simkl_id") or ""),
                                    "simkl_slug": "",
                                    "title": inner.get("title", ""),
                                    "item_type": item_type,
                                    "rating": int(round(float(rating))),
                                    "genres": [g.lower() for g in inner.get("genres", [])],
                                    "year": inner.get("year"),
                                    "runtime": inner.get("runtime"),
                                    "simkl_rating": None,
                                    "ids": ids,
                                    "rated_at": item.get("rated_at"),
                                })
                                mdb_added += 1
                finally:
                    await mdb.close()
        except Exception as e:
            log.warning("ml_predictor.mdblist_ratings_failed", error=str(e)[:120])

        if mdb_added:
            log.info("ml_predictor.mdblist_ratings_merged", count=mdb_added)

        # persist to DB — preserve user-submitted ratings
        async with async_session() as db:
            # Only delete imported ratings; keep source='user' rows
            await db.execute(
                delete(UserRating).where(
                    UserRating.user_id == user.id,
                    UserRating.source != "user",
                )
            )
            # Load existing user-submitted imdb_ids to avoid duplicates
            user_rated_q = select(UserRating.imdb_id).where(
                UserRating.user_id == user.id,
                UserRating.source == "user",
                UserRating.imdb_id.isnot(None),
            )
            user_rated_imdb = set(
                r for r in (await db.execute(user_rated_q)).scalars().all() if r
            )
            for r in rows:
                ids = r.get("ids", {})
                imdb = ids.get("imdb", "") or ""
                # Skip if user already rated this item manually
                if imdb and imdb in user_rated_imdb:
                    continue
                tmdb = str(ids.get("tmdb", "")) if ids.get("tmdb") else ""
                db.add(UserRating(
                    user_id=user.id,
                    simkl_id=r["simkl_id"],
                    simkl_slug=r["simkl_slug"],
                    title=r["title"],
                    item_type=r["item_type"],
                    rating=r["rating"],
                    genres=r["genres"],
                    year=r["year"],
                    runtime=r["runtime"],
                    simkl_rating=r.get("simkl_rating"),
                    source="imported",
                    imdb_id=imdb or None,
                    tmdb_id=tmdb or None,
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
                # Simkl community rating (extended=full) covers items not in the library
                "community_rating": r.get("simkl_rating") or 0,
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

            # If in library, fetch full Emby metadata for People/Studios/Genres
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
                    # Fill genres from Emby if rating source didn't provide them
                    if not item_data.get("genres") and full_item.get("Genres"):
                        item_data["genres"] = [g.lower() for g in full_item["Genres"]]
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

        # Notify for high-confidence high-score predictions
        try:
            from app.utils.notification_client import notify
            top = [p for p in predictions if p["predicted_rating"] >= 8.5 and p["confidence"] >= 0.6]
            if top:
                top.sort(key=lambda p: p["predicted_rating"], reverse=True)
                names = [f"{p['title']} ({p['predicted_rating']:.1f})" for p in top[:3]]
                notify("prediction", "🤖 High-Score Predictions",
                       ", ".join(names) + (f" +{len(top)-3} more" if len(top) > 3 else ""))
        except Exception:
            pass

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

        redis_key = f"ml_drift:{user_id}"
        db_key = f"ml_drift:{user_id}"
        # Read existing history (Redis first, then DB)
        history = None
        try:
            r = await get_redis()
            raw = await r.get(redis_key)
            if raw:
                history = json.loads(raw)
        except Exception:
            pass
        if history is None:
            try:
                async with async_session() as db:
                    row = (await db.execute(
                        select(AppSetting).where(AppSetting.key == db_key)
                    )).scalar_one_or_none()
                    history = json.loads(row.value) if row and row.value else []
            except Exception:
                history = []
        history.append(snapshot)
        history = history[-self.DRIFT_MAX_SNAPSHOTS:]
        encoded = json.dumps(history)
        # Write to DB
        try:
            async with async_session() as db:
                row = (await db.execute(
                    select(AppSetting).where(AppSetting.key == db_key)
                )).scalar_one_or_none()
                now = datetime.now(timezone.utc).replace(tzinfo=None)
                if row:
                    row.value = encoded
                    row.updated_at = now
                else:
                    db.add(AppSetting(key=db_key, value=encoded, updated_at=now))
                await db.commit()
            log.info("ml_predictor.drift_snapshot", user_id=user_id, snapshots=len(history))
        except Exception:
            log.warning("ml_predictor.drift_snapshot_db_failed", user_id=user_id)
        # Write to Redis cache
        try:
            r = await get_redis()
            await r.set(redis_key, encoded)
        except Exception:
            pass

    async def get_drift(self, user_id: int) -> dict:
        """Return drift data: historical snapshots + computed changes.

        Response: {snapshots: [...], changes: [...], summary: str}
        """
        redis_key = f"ml_drift:{user_id}"
        db_key = f"ml_drift:{user_id}"
        snapshots = None
        try:
            r = await get_redis()
            raw = await r.get(redis_key)
            if raw:
                snapshots = json.loads(raw)
        except Exception:
            pass
        if snapshots is None:
            try:
                async with async_session() as db:
                    row = (await db.execute(
                        select(AppSetting).where(AppSetting.key == db_key)
                    )).scalar_one_or_none()
                    snapshots = json.loads(row.value) if row and row.value else []
                # Re-populate Redis
                if snapshots:
                    try:
                        r = await get_redis()
                        await r.set(redis_key, json.dumps(snapshots))
                    except Exception:
                        pass
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
                if latest_model and latest_model.version and latest_model.version > 1:
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
