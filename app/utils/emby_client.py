"""Async Emby Server REST API client.

Covers:
  - Library queries (items, search, metadata)
  - Collection CRUD
  - Playback state
  - User management
  - Webhook reception helpers
"""

from __future__ import annotations

from typing import Any

import httpx
import structlog

from app.config import settings

log = structlog.get_logger()


class EmbyClient:
    """Single shared instance — authenticates with a server-level API key."""

    def __init__(self):
        self._base = settings.emby_url.rstrip("/")
        self._key = settings.emby_api_key
        self._client = httpx.AsyncClient(timeout=30.0)

    # -- async context manager -----------------------------------------------

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False

    # -- helpers -------------------------------------------------------------

    def _url(self, path: str) -> str:
        return f"{self._base}{path}"

    def _params(self, extra: dict | None = None) -> dict:
        p = {"api_key": self._key}
        if extra:
            p.update(extra)
        return p

    async def _get(self, path: str, params: dict | None = None) -> Any:
        resp = await self._client.get(
            self._url(path), params=self._params(params),
        )
        resp.raise_for_status()
        return resp.json()

    async def _post(self, path: str, body: dict | None = None, params: dict | None = None) -> Any:
        resp = await self._client.post(
            self._url(path), json=body, params=self._params(params),
        )
        resp.raise_for_status()
        if resp.content:
            return resp.json()
        return {}

    async def _delete(self, path: str, params: dict | None = None) -> None:
        resp = await self._client.delete(
            self._url(path), params=self._params(params),
        )
        resp.raise_for_status()

    async def close(self):
        await self._client.aclose()

    # -- Server info ---------------------------------------------------------

    async def get_system_info(self) -> dict:
        return await self._get("/System/Info/Public")

    async def get_metadata_country(self) -> str:
        """Return the server's MetadataCountryCode from /System/Configuration.

        Falls back to 'us' if the field is missing or the call fails.
        Requires admin-level API key (which the suite already uses).
        """
        try:
            config = await self._get("/System/Configuration")
            code = config.get("MetadataCountryCode", "") or ""
            return code.lower() if code else "us"
        except Exception:
            log.warning("emby.metadata_country_failed")
            return "us"

    # -- Users ---------------------------------------------------------------

    async def get_users(self) -> list[dict]:
        return await self._get("/Users")

    async def get_user(self, user_id: str) -> dict:
        return await self._get(f"/Users/{user_id}")

    # -- Libraries (virtual folders) ------------------------------------------

    async def get_virtual_folders(self) -> list[dict]:
        """Return Emby library folders (name, type, paths, item count)."""
        raw = await self._get("/Library/VirtualFolders")
        results = []
        for f in raw:
            results.append({
                "name": f.get("Name", ""),
                "collection_type": f.get("CollectionType", ""),
                "item_id": f.get("ItemId", ""),
                "locations": f.get("Locations", []),
            })
        return results

    # -- Library items -------------------------------------------------------

    async def get_items(
        self,
        user_id: str | None = None,
        item_type: str | None = None,
        parent_id: str | None = None,
        search_term: str | None = None,
        fields: str = "ProviderIds,Genres,Overview,People,Studios,DateCreated,RunTimeTicks,CommunityRating,OfficialRating,ProductionYear,UserData",
        recursive: bool = True,
        limit: int = 500,
        start_index: int = 0,
        filters: str | None = None,
        sort_by: str | None = None,
        sort_order: str | None = None,
        extra_params: dict[str, str] | None = None,
    ) -> dict:
        """Flexible item query.  Returns {Items: [...], TotalRecordCount: int}."""
        params: dict[str, Any] = {
            "Recursive": str(recursive).lower(),
            "Fields": fields,
            "Limit": limit,
            "StartIndex": start_index,
        }
        if item_type:
            params["IncludeItemTypes"] = item_type
        if parent_id:
            params["ParentId"] = parent_id
        if search_term:
            params["SearchTerm"] = search_term
        if filters:
            params["Filters"] = filters
        if sort_by:
            params["SortBy"] = sort_by
        if sort_order:
            params["SortOrder"] = sort_order
        if extra_params:
            params.update(extra_params)

        path = f"/Users/{user_id}/Items" if user_id else "/Items"
        return await self._get(path, params)

    async def get_all_movies(self, user_id: str | None = None, fields: str | None = None) -> list[dict]:
        """Page through every movie in the library."""
        items: list[dict] = []
        start = 0
        batch = 500
        kwargs: dict = {"user_id": user_id, "item_type": "Movie", "limit": batch}
        if fields:
            kwargs["fields"] = fields
        while True:
            kwargs["start_index"] = start
            resp = await self.get_items(**kwargs)
            items.extend(resp.get("Items", []))
            if start + batch >= resp.get("TotalRecordCount", 0):
                break
            start += batch
        return items

    async def get_all_series(self, user_id: str | None = None) -> list[dict]:
        items: list[dict] = []
        start = 0
        batch = 500
        while True:
            resp = await self.get_items(
                user_id=user_id, item_type="Series",
                limit=batch, start_index=start,
            )
            items.extend(resp.get("Items", []))
            if start + batch >= resp.get("TotalRecordCount", 0):
                break
            start += batch
        return items

    async def get_all_episodes(self, series_id: str, user_id: str | None = None) -> list[dict]:
        return (await self.get_items(
            user_id=user_id, item_type="Episode", parent_id=series_id,
        )).get("Items", [])

    async def get_item(self, item_id: str, user_id: str | None = None) -> dict:
        path = f"/Users/{user_id}/Items/{item_id}" if user_id else f"/Items/{item_id}"
        return await self._get(path, {"Fields": "ProviderIds,Genres,Overview,People,Studios,RunTimeTicks,Tags,Taglines,CommunityRating,OfficialRating"})

    async def get_library_items(
        self,
        item_type: str,
        skip: int = 0,
        limit: int = 100,
        fields: list[str] | None = None,
        user_id: str | None = None,
    ) -> list[dict]:
        """Paginated library fetch for cache indexing."""
        field_str = ",".join(fields) if fields else "ProviderIds,ProductionYear"
        resp = await self.get_items(
            user_id=user_id,
            item_type=item_type,
            fields=field_str,
            limit=limit,
            start_index=skip,
        )
        return resp.get("Items", [])

    async def search_items(self, term: str, item_type: str | None = None) -> list[dict]:
        resp = await self.get_items(search_term=term, item_type=item_type, limit=20)
        return resp.get("Items", [])

    async def get_items_by_ids(self, item_ids: list[str], user_id: str | None = None) -> list[dict]:
        """Batch fetch items by ID (single call).

        When user_id is provided, uses /Users/{uid}/Items which returns
        full item data on all Emby builds.  Server-scope /Items may omit
        fields like Overview on some builds.
        """
        if not item_ids:
            return []
        fields = ("CommunityRating,OfficialRating,ProductionYear,ProviderIds,"
                  "RunTimeTicks,Genres,Overview,Tags,Taglines,People,Studios,"
                  "DateCreated")
        path = f"/Users/{user_id}/Items" if user_id else "/Items"
        resp = await self._get(path, {
            "Ids": ",".join(str(i) for i in item_ids),
            "Fields": fields,
        })
        return resp.get("Items", [])

    async def get_user_items_by_ids(self, user_id: str, item_ids: list[str]) -> list[dict]:
        """Batch fetch items with UserData (includes Played status, LastPlayedDate).

        Uses user-scoped endpoint so each item includes UserData with
        Played, PlayCount, UnplayedItemCount (for Series), LastPlayedDate, etc.
        """
        if not item_ids or not user_id:
            return []
        resp = await self._get(f"/Users/{user_id}/Items", {
            "Ids": ",".join(str(i) for i in item_ids),
            "Fields": "ProviderIds,ProductionYear,UserData",
        })
        return resp.get("Items", [])

    async def get_item_safe(self, item_id: str, user_id: str | None = None) -> dict | None:
        """Fetch a single item, tolerating Emby builds where /Items/{id} 404s.
        Tries the user-scoped endpoint first, then falls back to /Items?Ids=."""
        if user_id:
            try:
                return await self.get_item(item_id, user_id=user_id)
            except Exception:
                pass
        try:
            return await self.get_item(item_id)
        except Exception:
            pass
        items = await self.get_items_by_ids([item_id], user_id=user_id)
        return items[0] if items else None

    # -- Provider ID helpers -------------------------------------------------

    @staticmethod
    def get_provider_id(item: dict, provider: str) -> str | None:
        """Extract TMDB, TVDB, IMDB id from an Emby item dict."""
        ids = item.get("ProviderIds", {})
        return ids.get(provider) or ids.get(provider.lower()) or ids.get(provider.capitalize())

    # -- Collections ---------------------------------------------------------

    async def get_collections(self) -> list[dict]:
        resp = await self.get_items(item_type="BoxSet", recursive=True)
        return resp.get("Items", [])

    async def create_collection(self, name: str, item_ids: list[str] | None = None) -> dict:
        """Create a collection.  Emby requires at least one item id at
        creation time (creating an empty collection returns 500)."""
        params: dict[str, Any] = {"Name": name}
        if item_ids:
            params["Ids"] = ",".join(item_ids)
        return await self._post("/Collections", params=params)

    async def add_to_collection(self, collection_id: str, item_ids: list[str]) -> None:
        await self._post(
            f"/Collections/{collection_id}/Items",
            params={"Ids": ",".join(item_ids)},
        )

    async def remove_from_collection(self, collection_id: str, item_ids: list[str]) -> None:
        await self._delete(
            f"/Collections/{collection_id}/Items",
            params={"Ids": ",".join(item_ids)},
        )

    async def find_or_create_collection(self, name: str, initial_item_ids: list[str] | None = None) -> str:
        """Return collection ID, creating it (with initial items) if missing."""
        existing = await self.get_collections()
        for col in existing:
            if col.get("Name", "").lower() == name.lower():
                return col["Id"]
        result = await self.create_collection(name, initial_item_ids)
        return result.get("Id", "")

    async def set_collection_items(self, collection_name: str, item_ids: list[str]) -> str:
        """Overwrite a collection's contents with exactly these items."""
        if not item_ids:
            return ""
        col_id = await self.find_or_create_collection(collection_name, item_ids)

        # get current items (filter to main types — trailers/extras are auto-linked by Emby)
        current = await self.get_items(parent_id=col_id, recursive=False)
        current_ids = {
            i["Id"] for i in current.get("Items", [])
            if i.get("Type") in ("Movie", "Series", "Episode", "BoxSet", None)
        }
        new_ids = set(item_ids)

        to_remove = current_ids - new_ids
        to_add = new_ids - current_ids

        if to_remove:
            await self.remove_from_collection(col_id, list(to_remove))
        if to_add:
            await self.add_to_collection(col_id, list(to_add))

        return col_id

    # -- Playlists (ordered — preserves item order, unlike Collections) ------

    async def create_playlist(
        self, name: str, item_ids: list[str], user_id: str | None = None,
    ) -> str:
        """Create an Emby Playlist (preserves item insertion order).

        POST /Playlists?UserId=X&Name=X&Ids=comma,separated&MediaType=Video
        Returns the playlist Id.
        """
        if not item_ids:
            return ""
        uid = user_id or ""
        params = {
            "UserId": uid,
            "Name": name,
            "Ids": ",".join(item_ids),
            "MediaType": "Video",
        }
        data = await self._post("/Playlists", params=params)
        playlist_id = data.get("Id", "")
        log.info(
            "emby.playlist_created",
            name=name,
            playlist_id=playlist_id,
            items=len(item_ids),
        )
        return playlist_id

    async def delete_item(self, item_id: str) -> None:
        """Delete an Emby item (playlist, collection, etc)."""
        await self._delete(f"/Items/{item_id}")

    async def recreate_playlist(
        self, name: str, item_ids: list[str], user_id: str | None = None,
    ) -> str:
        """Delete existing playlist with this name, then create fresh.

        Emby has no reorder/move API for playlist items, so the pattern is
        delete-and-recreate with items in the desired order.
        """
        # Find existing playlist by name
        items = await self.get_items(
            item_type="Playlist",
            recursive=True,
            search_term=name,
        )
        for item in items.get("Items", []):
            if item.get("Name") == name:
                await self.delete_item(item["Id"])
                log.info("emby.playlist_deleted_for_recreate", name=name, old_id=item["Id"])
                break

        return await self.create_playlist(name, item_ids, user_id)

    # -- Playback state ------------------------------------------------------

    async def get_sessions(self) -> list[dict]:
        """Active playback sessions."""
        return await self._get("/Sessions")

    async def mark_played(self, user_id: str, item_id: str) -> None:
        await self._post(f"/Users/{user_id}/PlayedItems/{item_id}")

    async def mark_unplayed(self, user_id: str, item_id: str) -> None:
        await self._delete(f"/Users/{user_id}/PlayedItems/{item_id}")

    # -- Playback control (for watch-party sync) -----------------------------

    async def send_play_command(
        self, session_id: str, command: str,
        controlling_user_id: str | None = None,
    ) -> None:
        """command: PlayPause | Stop | Seek | etc.

        ControllingUserId tells Emby who is issuing the remote command.
        Without it, some Emby clients silently ignore the command even
        though the server returns 204.
        """
        params: dict[str, Any] = {}
        if controlling_user_id:
            params["ControllingUserId"] = controlling_user_id
        await self._post(
            f"/Sessions/{session_id}/Playing/{command}",
            params=params if params else None,
        )

    async def play_item_on_session(
        self, session_id: str, item_id: str, start_position_ticks: int = 0,
        controlling_user_id: str | None = None,
    ) -> None:
        """Start playing a specific item on a remote Emby session.

        POST /Sessions/{id}/Playing with PlayCommand=PlayNow

        ItemIds and PlayCommand go as query parameters.
        StartPositionTicks must be in the JSON body for Emby to honour it.

        The ControllingUserId tells Emby who is issuing the remote command.
        Without it, some Emby clients silently ignore the play command even
        though the server returns 204.  Setting it to the session's own
        UserId ensures the client trusts the command.
        """
        params: dict[str, Any] = {
            "ItemIds": item_id,
            "PlayCommand": "PlayNow",
        }
        if controlling_user_id:
            params["ControllingUserId"] = controlling_user_id
        body: dict[str, Any] = {}
        if start_position_ticks:
            body["StartPositionTicks"] = int(start_position_ticks)
        await self._post(
            f"/Sessions/{session_id}/Playing",
            body=body or None,
            params=params,
        )

    async def send_play_state_command(
        self, session_id: str, command: str, seek_ticks: int | None = None,
    ) -> None:
        params: dict[str, Any] = {}
        if seek_ticks is not None:
            params["SeekPositionTicks"] = seek_ticks
        await self._post(
            f"/Sessions/{session_id}/Playing/{command}",
            params=params,
        )

    # -- Metadata update (for enrichment push) --------------------------------

    async def set_playlist_overview(
        self, playlist_id: str, overview: str,
        user_id: str | None = None,
    ) -> bool:
        """Set Overview on a playlist item.

        1. Wait 1s for Emby to fully index the playlist
        2. GET /Users/{uid}/Items/{id} — full response
        3. Set Overview, LockedFields, LockData
        4. POST /Items/{id}
        """
        if not playlist_id or not overview:
            return False

        try:
            # Step 1: Wait for Emby to fully index the playlist
            import asyncio
            await asyncio.sleep(1)

            # Step 2: GET the full playlist object (user-scoped, no Fields)
            get_url = self._url(f"/Users/{user_id}/Items/{playlist_id}")
            get_resp = await self._client.get(
                get_url,
                params={"api_key": self._key},
                headers={"X-MediaBrowser-Token": self._key},
            )
            get_resp.raise_for_status()
            item = get_resp.json()

            if not item or not item.get("Id"):
                log.warning("emby.set_overview_item_not_found",
                            playlist_id=playlist_id)
                return False

            # Step 3: Set Overview, lock it, and lock metadata
            item["Overview"] = overview
            locked = list(set(item.get("LockedFields") or []) | {"Overview"})
            item["LockedFields"] = locked
            item["LockData"] = True

            # Step 4: POST the complete object with auth header
            post_url = self._url(f"/Items/{playlist_id}")
            resp = await self._client.post(
                post_url,
                json=item,
                params={"api_key": self._key},
                headers={
                    "Content-Type": "application/json",
                    "X-MediaBrowser-Token": self._key,
                },
            )
            log.info("emby.set_playlist_overview",
                     playlist_id=playlist_id,
                     status=resp.status_code,
                     overview_len=len(overview))

            return resp.status_code < 400
        except Exception as e:
            log.warning("emby.set_overview_failed",
                        playlist_id=playlist_id,
                        error=str(e)[:200])
            return False

    async def set_provider_ids(
        self, item_id: str, provider_ids: dict,
        user_id: str | None = None,
        replace: bool = False,
    ) -> bool:
        """Reassign an item's ProviderIds (Imdb / Tmdb / Tvdb).

        Used by the duplicate re-link action: rather than deleting a
        mis-matched copy, point it at the correct provider entry so the
        duplicate group resolves itself on the next metadata refresh.

        provider_ids: dict like {"Imdb": "tt1234567", "Tmdb": "603"}.
        A value of None or "" removes that key.

        replace=True discards existing ProviderIds entirely; the default
        merges the supplied keys over whatever is already there.

        Emby requires the full item object on POST /Items/{id}, so this
        GETs first and merges.  ProviderIds is added to LockedFields so
        the next library scan doesn't revert the change.
        """
        try:
            item = await self.get_item_safe(item_id, user_id=user_id)
            if not item:
                log.warning("emby.relink_item_not_found", item_id=item_id)
                return False

            existing = {} if replace else dict(item.get("ProviderIds") or {})
            for key, val in (provider_ids or {}).items():
                if val:
                    existing[key] = str(val)
                else:
                    existing.pop(key, None)

            item["ProviderIds"] = existing
            item["LockData"] = True
            locked = set(item.get("LockedFields") or [])
            locked.add("ProviderIds")
            item["LockedFields"] = list(locked)

            resp = await self._client.post(
                self._url(f"/Items/{item_id}"),
                json=item,
                params=self._params(),
                headers={
                    "Content-Type": "application/json",
                    "X-MediaBrowser-Token": self._key,
                },
            )
            log.info("emby.provider_ids_set", item_id=item_id,
                     provider_ids=existing,
                     status_code=resp.status_code)
            resp.raise_for_status()
            return True
        except Exception as e:
            log.warning("emby.set_provider_ids_failed", item_id=item_id,
                        error=str(e)[:200])
            return False

    async def refresh_item(
        self, item_id: str,
        replace_all_metadata: bool = False,
        replace_all_images: bool = False,
    ) -> bool:
        """Trigger a metadata refresh on a single item.

        Called after a re-link so Emby re-pulls metadata against the
        newly assigned provider ID.  Fire-and-forget — Emby returns 204
        immediately and processes the refresh in the background.
        """
        try:
            resp = await self._client.post(
                self._url(f"/Items/{item_id}/Refresh"),
                params={
                    **self._params(),
                    "Recursive": "false",
                    "MetadataRefreshMode": "FullRefresh",
                    "ImageRefreshMode": (
                        "FullRefresh" if replace_all_images else "Default"
                    ),
                    "ReplaceAllMetadata": str(replace_all_metadata).lower(),
                    "ReplaceAllImages": str(replace_all_images).lower(),
                },
            )
            log.info("emby.item_refresh_queued", item_id=item_id,
                     status_code=resp.status_code)
            resp.raise_for_status()
            return True
        except Exception as e:
            log.warning("emby.item_refresh_failed", item_id=item_id,
                        error=str(e)[:200])
            return False

    async def update_item(
        self, item_id: str, updates: dict,
        current_item: dict | None = None,
        user_id: str | None = None,
        lock_fields: list[str] | None = None,
    ) -> bool:
        """Update an Emby item's metadata fields.

        GET the full item first (user-scoped for complete data), merge
        updates, POST back.  Emby requires the full item object on
        POST /Items/{id} — partial objects cause field blanking.

        Pass user_id for the user-scoped GET (required on builds where
        /Items/{id} returns 404).

        lock_fields: list of field names to add to LockedFields so Emby
        won't overwrite them on the next metadata refresh.

        Safe fields to update: Tags, Taglines, CommunityRating,
        Genres, Overview, OfficialRating.  Changes propagate to
        all Emby clients automatically.
        """
        try:
            item = current_item
            if not item:
                item = await self.get_item_safe(item_id, user_id=user_id)
            if not item:
                log.warning("emby.update_item_not_found", item_id=item_id)
                return False

            log.debug("emby.update_item_got",
                       item_id=item_id,
                       item_name=item.get("Name"),
                       item_type=item.get("Type"),
                       has_overview=bool(item.get("Overview")),
                       keys=sorted(item.keys())[:20])

            for key, val in updates.items():
                item[key] = val

            # Lock updated fields to prevent Emby metadata refresh
            # from overwriting them
            if lock_fields:
                locked = set(item.get("LockedFields") or [])
                locked.update(lock_fields)
                item["LockedFields"] = list(locked)

            resp = await self._client.post(
                self._url(f"/Items/{item_id}"),
                json=item,
                params=self._params(),
            )
            log.info("emby.item_updated", item_id=item_id,
                     fields=list(updates.keys()),
                     status_code=resp.status_code)
            resp.raise_for_status()
            return True
        except Exception as e:
            log.warning("emby.item_update_failed", item_id=item_id,
                        error=str(e)[:200])
            return False
