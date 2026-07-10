"""Service #3 — Shared Universe Discovery.

1. Maintains a database of known universes + watch orders
2. Scans Emby library and matches items to universes
3. Creates ordered Emby collections per universe
4. Tracks watch progress per user
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import structlog
from sqlalchemy import select, delete, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.schema import Universe, UniverseItem, User
from app.utils.trakt_client import TraktClient
from app.utils.emby_client import EmbyClient
from app.utils.redis_cache import cache_get, cache_set
from app.utils.database import async_session

log = structlog.get_logger()


# ---------------------------------------------------------------------------
# Curated universe definitions
# ---------------------------------------------------------------------------
# These seed the DB on first run.  Trakt IDs are slugs.
# Release order is used for collection sorting.

KNOWN_UNIVERSES: list[dict] = [
    {
        "name": "Marvel Cinematic Universe",
        "slug": "mcu",
        "description": "All MCU movies and Disney+ shows in release order",
        "items": [
            {"title": "Iron Man", "year": 2008, "type": "movie", "trakt_slug": "iron-man-2008", "release_order": 1, "chronological_order": 3},
            {"title": "The Incredible Hulk", "year": 2008, "type": "movie", "trakt_slug": "the-incredible-hulk-2008", "release_order": 2, "chronological_order": 4},
            {"title": "Iron Man 2", "year": 2010, "type": "movie", "trakt_slug": "iron-man-2-2010", "release_order": 3, "chronological_order": 5},
            {"title": "Thor", "year": 2011, "type": "movie", "trakt_slug": "thor-2011", "release_order": 4, "chronological_order": 6},
            {"title": "Captain America: The First Avenger", "year": 2011, "type": "movie", "trakt_slug": "captain-america-the-first-avenger-2011", "release_order": 5, "chronological_order": 1},
            {"title": "The Avengers", "year": 2012, "type": "movie", "trakt_slug": "the-avengers-2012", "release_order": 6, "chronological_order": 7},
            {"title": "Iron Man 3", "year": 2013, "type": "movie", "trakt_slug": "iron-man-3-2013", "release_order": 7, "chronological_order": 8},
            {"title": "Thor: The Dark World", "year": 2013, "type": "movie", "trakt_slug": "thor-the-dark-world-2013", "release_order": 8, "chronological_order": 9},
            {"title": "Captain America: The Winter Soldier", "year": 2014, "type": "movie", "trakt_slug": "captain-america-the-winter-soldier-2014", "release_order": 9, "chronological_order": 10},
            {"title": "Guardians of the Galaxy", "year": 2014, "type": "movie", "trakt_slug": "guardians-of-the-galaxy-2014", "release_order": 10, "chronological_order": 11},
            {"title": "Avengers: Age of Ultron", "year": 2015, "type": "movie", "trakt_slug": "avengers-age-of-ultron-2015", "release_order": 11, "chronological_order": 12},
            {"title": "Ant-Man", "year": 2015, "type": "movie", "trakt_slug": "ant-man-2015", "release_order": 12, "chronological_order": 13},
            {"title": "Captain America: Civil War", "year": 2016, "type": "movie", "trakt_slug": "captain-america-civil-war-2016", "release_order": 13, "chronological_order": 14},
            {"title": "Doctor Strange", "year": 2016, "type": "movie", "trakt_slug": "doctor-strange-2016", "release_order": 14, "chronological_order": 15},
            {"title": "Guardians of the Galaxy Vol. 2", "year": 2017, "type": "movie", "trakt_slug": "guardians-of-the-galaxy-vol-2-2017", "release_order": 15, "chronological_order": 16},
            {"title": "Spider-Man: Homecoming", "year": 2017, "type": "movie", "trakt_slug": "spider-man-homecoming-2017", "release_order": 16, "chronological_order": 17},
            {"title": "Thor: Ragnarok", "year": 2017, "type": "movie", "trakt_slug": "thor-ragnarok-2017", "release_order": 17, "chronological_order": 18},
            {"title": "Black Panther", "year": 2018, "type": "movie", "trakt_slug": "black-panther-2018", "release_order": 18, "chronological_order": 19},
            {"title": "Avengers: Infinity War", "year": 2018, "type": "movie", "trakt_slug": "avengers-infinity-war-2018", "release_order": 19, "chronological_order": 20},
            {"title": "Ant-Man and the Wasp", "year": 2018, "type": "movie", "trakt_slug": "ant-man-and-the-wasp-2018", "release_order": 20, "chronological_order": 21},
            {"title": "Captain Marvel", "year": 2019, "type": "movie", "trakt_slug": "captain-marvel-2019", "release_order": 21, "chronological_order": 2},
            {"title": "Avengers: Endgame", "year": 2019, "type": "movie", "trakt_slug": "avengers-endgame-2019", "release_order": 22, "chronological_order": 22},
        ],
    },
    {
        "name": "Star Wars (Skywalker Saga)",
        "slug": "star-wars-skywalker",
        "description": "The nine-episode Skywalker Saga",
        "items": [
            {"title": "Star Wars: Episode I - The Phantom Menace", "year": 1999, "type": "movie", "trakt_slug": "star-wars-episode-i-the-phantom-menace-1999", "release_order": 4, "chronological_order": 1},
            {"title": "Star Wars: Episode II - Attack of the Clones", "year": 2002, "type": "movie", "trakt_slug": "star-wars-episode-ii-attack-of-the-clones-2002", "release_order": 5, "chronological_order": 2},
            {"title": "Star Wars: Episode III - Revenge of the Sith", "year": 2005, "type": "movie", "trakt_slug": "star-wars-episode-iii-revenge-of-the-sith-2005", "release_order": 6, "chronological_order": 3},
            {"title": "Star Wars", "year": 1977, "type": "movie", "trakt_slug": "star-wars-1977", "release_order": 1, "chronological_order": 4},
            {"title": "The Empire Strikes Back", "year": 1980, "type": "movie", "trakt_slug": "the-empire-strikes-back-1980", "release_order": 2, "chronological_order": 5},
            {"title": "Return of the Jedi", "year": 1983, "type": "movie", "trakt_slug": "return-of-the-jedi-1983", "release_order": 3, "chronological_order": 6},
            {"title": "Star Wars: The Force Awakens", "year": 2015, "type": "movie", "trakt_slug": "star-wars-the-force-awakens-2015", "release_order": 7, "chronological_order": 7},
            {"title": "Star Wars: The Last Jedi", "year": 2017, "type": "movie", "trakt_slug": "star-wars-the-last-jedi-2017", "release_order": 8, "chronological_order": 8},
            {"title": "Star Wars: The Rise of Skywalker", "year": 2019, "type": "movie", "trakt_slug": "star-wars-the-rise-of-skywalker-2019", "release_order": 9, "chronological_order": 9},
        ],
    },
    {
        "name": "The Lord of the Rings + The Hobbit",
        "slug": "middle-earth",
        "description": "Peter Jackson's Middle-earth films",
        "items": [
            {"title": "The Lord of the Rings: The Fellowship of the Ring", "year": 2001, "type": "movie", "trakt_slug": "the-lord-of-the-rings-the-fellowship-of-the-ring-2001", "release_order": 1, "chronological_order": 4},
            {"title": "The Lord of the Rings: The Two Towers", "year": 2002, "type": "movie", "trakt_slug": "the-lord-of-the-rings-the-two-towers-2002", "release_order": 2, "chronological_order": 5},
            {"title": "The Lord of the Rings: The Return of the King", "year": 2003, "type": "movie", "trakt_slug": "the-lord-of-the-rings-the-return-of-the-king-2003", "release_order": 3, "chronological_order": 6},
            {"title": "The Hobbit: An Unexpected Journey", "year": 2012, "type": "movie", "trakt_slug": "the-hobbit-an-unexpected-journey-2012", "release_order": 4, "chronological_order": 1},
            {"title": "The Hobbit: The Desolation of Smaug", "year": 2013, "type": "movie", "trakt_slug": "the-hobbit-the-desolation-of-smaug-2013", "release_order": 5, "chronological_order": 2},
            {"title": "The Hobbit: The Battle of the Five Armies", "year": 2014, "type": "movie", "trakt_slug": "the-hobbit-the-battle-of-the-five-armies-2014", "release_order": 6, "chronological_order": 3},
        ],
    },
    {
        "name": "DC Extended Universe",
        "slug": "dceu",
        "description": "DC films from Man of Steel onwards",
        "items": [
            {"title": "Man of Steel", "year": 2013, "type": "movie", "trakt_slug": "man-of-steel-2013", "release_order": 1, "chronological_order": 1},
            {"title": "Batman v Superman: Dawn of Justice", "year": 2016, "type": "movie", "trakt_slug": "batman-v-superman-dawn-of-justice-2016", "release_order": 2, "chronological_order": 2},
            {"title": "Suicide Squad", "year": 2016, "type": "movie", "trakt_slug": "suicide-squad-2016", "release_order": 3, "chronological_order": 3},
            {"title": "Wonder Woman", "year": 2017, "type": "movie", "trakt_slug": "wonder-woman-2017", "release_order": 4, "chronological_order": 4},
            {"title": "Justice League", "year": 2017, "type": "movie", "trakt_slug": "justice-league-2017", "release_order": 5, "chronological_order": 5},
            {"title": "Aquaman", "year": 2018, "type": "movie", "trakt_slug": "aquaman-2018", "release_order": 6, "chronological_order": 6},
        ],
    },
    {
        "name": "Harry Potter",
        "slug": "harry-potter",
        "description": "Wizarding World films",
        "items": [
            {"title": "Harry Potter and the Philosopher's Stone", "year": 2001, "type": "movie", "trakt_slug": "harry-potter-and-the-philosopher-s-stone-2001", "release_order": 1, "chronological_order": 1},
            {"title": "Harry Potter and the Chamber of Secrets", "year": 2002, "type": "movie", "trakt_slug": "harry-potter-and-the-chamber-of-secrets-2002", "release_order": 2, "chronological_order": 2},
            {"title": "Harry Potter and the Prisoner of Azkaban", "year": 2004, "type": "movie", "trakt_slug": "harry-potter-and-the-prisoner-of-azkaban-2004", "release_order": 3, "chronological_order": 3},
            {"title": "Harry Potter and the Goblet of Fire", "year": 2005, "type": "movie", "trakt_slug": "harry-potter-and-the-goblet-of-fire-2005", "release_order": 4, "chronological_order": 4},
            {"title": "Harry Potter and the Order of the Phoenix", "year": 2007, "type": "movie", "trakt_slug": "harry-potter-and-the-order-of-the-phoenix-2007", "release_order": 5, "chronological_order": 5},
            {"title": "Harry Potter and the Half-Blood Prince", "year": 2009, "type": "movie", "trakt_slug": "harry-potter-and-the-half-blood-prince-2009", "release_order": 6, "chronological_order": 6},
            {"title": "Harry Potter and the Deathly Hallows: Part 1", "year": 2010, "type": "movie", "trakt_slug": "harry-potter-and-the-deathly-hallows-part-1-2010", "release_order": 7, "chronological_order": 7},
            {"title": "Harry Potter and the Deathly Hallows: Part 2", "year": 2011, "type": "movie", "trakt_slug": "harry-potter-and-the-deathly-hallows-part-2-2011", "release_order": 8, "chronological_order": 8},
        ],
    },
    # ── Additional franchises ────────────────────────────────────────────
    {
        "name": "X-Men",
        "slug": "x-men",
        "description": "X-Men film franchise",
        "items": [
            {"title": "X-Men", "year": 2000, "type": "movie", "trakt_slug": "x-men-2000", "release_order": 1, "chronological_order": 4},
            {"title": "X2: X-Men United", "year": 2003, "type": "movie", "trakt_slug": "x2-x-men-united-2003", "release_order": 2, "chronological_order": 5},
            {"title": "X-Men: The Last Stand", "year": 2006, "type": "movie", "trakt_slug": "x-men-the-last-stand-2006", "release_order": 3, "chronological_order": 6},
            {"title": "X-Men Origins: Wolverine", "year": 2009, "type": "movie", "trakt_slug": "x-men-origins-wolverine-2009", "release_order": 4, "chronological_order": 2},
            {"title": "X-Men: First Class", "year": 2011, "type": "movie", "trakt_slug": "x-men-first-class-2011", "release_order": 5, "chronological_order": 1},
            {"title": "The Wolverine", "year": 2013, "type": "movie", "trakt_slug": "the-wolverine-2013", "release_order": 6, "chronological_order": 7},
            {"title": "X-Men: Days of Future Past", "year": 2014, "type": "movie", "trakt_slug": "x-men-days-of-future-past-2014", "release_order": 7, "chronological_order": 3},
            {"title": "Deadpool", "year": 2016, "type": "movie", "trakt_slug": "deadpool-2016", "release_order": 8, "chronological_order": 8},
            {"title": "X-Men: Apocalypse", "year": 2016, "type": "movie", "trakt_slug": "x-men-apocalypse-2016", "release_order": 9, "chronological_order": 9},
            {"title": "Logan", "year": 2017, "type": "movie", "trakt_slug": "logan-2017", "release_order": 10, "chronological_order": 13},
            {"title": "Deadpool 2", "year": 2018, "type": "movie", "trakt_slug": "deadpool-2-2018", "release_order": 11, "chronological_order": 10},
            {"title": "Dark Phoenix", "year": 2019, "type": "movie", "trakt_slug": "dark-phoenix-2019", "release_order": 12, "chronological_order": 11},
            {"title": "The New Mutants", "year": 2020, "type": "movie", "trakt_slug": "the-new-mutants-2020", "release_order": 13, "chronological_order": 12},
        ],
    },
    {
        "name": "Fast & Furious",
        "slug": "fast-furious",
        "description": "The Fast Saga",
        "items": [
            {"title": "The Fast and the Furious", "year": 2001, "type": "movie", "trakt_slug": "the-fast-and-the-furious-2001", "release_order": 1, "chronological_order": 1},
            {"title": "2 Fast 2 Furious", "year": 2003, "type": "movie", "trakt_slug": "2-fast-2-furious-2003", "release_order": 2, "chronological_order": 2},
            {"title": "The Fast and the Furious: Tokyo Drift", "year": 2006, "type": "movie", "trakt_slug": "the-fast-and-the-furious-tokyo-drift-2006", "release_order": 3, "chronological_order": 5},
            {"title": "Fast & Furious", "year": 2009, "type": "movie", "trakt_slug": "fast-furious-2009", "release_order": 4, "chronological_order": 3},
            {"title": "Fast Five", "year": 2011, "type": "movie", "trakt_slug": "fast-five-2011", "release_order": 5, "chronological_order": 4},
            {"title": "Fast & Furious 6", "year": 2013, "type": "movie", "trakt_slug": "fast-furious-6-2013", "release_order": 6, "chronological_order": 6},
            {"title": "Furious 7", "year": 2015, "type": "movie", "trakt_slug": "furious-7-2015", "release_order": 7, "chronological_order": 7},
            {"title": "The Fate of the Furious", "year": 2017, "type": "movie", "trakt_slug": "the-fate-of-the-furious-2017", "release_order": 8, "chronological_order": 8},
            {"title": "F9", "year": 2021, "type": "movie", "trakt_slug": "f9-the-fast-saga-2021", "release_order": 9, "chronological_order": 9},
            {"title": "Fast X", "year": 2023, "type": "movie", "trakt_slug": "fast-x-2023", "release_order": 10, "chronological_order": 10},
        ],
    },
    {
        "name": "Mission: Impossible",
        "slug": "mission-impossible",
        "description": "Tom Cruise's Mission: Impossible films",
        "items": [
            {"title": "Mission: Impossible", "year": 1996, "type": "movie", "trakt_slug": "mission-impossible-1996", "release_order": 1, "chronological_order": 1},
            {"title": "Mission: Impossible II", "year": 2000, "type": "movie", "trakt_slug": "mission-impossible-ii-2000", "release_order": 2, "chronological_order": 2},
            {"title": "Mission: Impossible III", "year": 2006, "type": "movie", "trakt_slug": "mission-impossible-iii-2006", "release_order": 3, "chronological_order": 3},
            {"title": "Mission: Impossible - Ghost Protocol", "year": 2011, "type": "movie", "trakt_slug": "mission-impossible-ghost-protocol-2011", "release_order": 4, "chronological_order": 4},
            {"title": "Mission: Impossible - Rogue Nation", "year": 2015, "type": "movie", "trakt_slug": "mission-impossible-rogue-nation-2015", "release_order": 5, "chronological_order": 5},
            {"title": "Mission: Impossible - Fallout", "year": 2018, "type": "movie", "trakt_slug": "mission-impossible-fallout-2018", "release_order": 6, "chronological_order": 6},
            {"title": "Mission: Impossible - Dead Reckoning Part One", "year": 2023, "type": "movie", "trakt_slug": "mission-impossible-dead-reckoning-part-one-2023", "release_order": 7, "chronological_order": 7},
        ],
    },
    {
        "name": "John Wick",
        "slug": "john-wick",
        "description": "The John Wick action franchise",
        "items": [
            {"title": "John Wick", "year": 2014, "type": "movie", "trakt_slug": "john-wick-2014", "release_order": 1, "chronological_order": 1},
            {"title": "John Wick: Chapter 2", "year": 2017, "type": "movie", "trakt_slug": "john-wick-chapter-2-2017", "release_order": 2, "chronological_order": 2},
            {"title": "John Wick: Chapter 3 - Parabellum", "year": 2019, "type": "movie", "trakt_slug": "john-wick-chapter-3-parabellum-2019", "release_order": 3, "chronological_order": 3},
            {"title": "John Wick: Chapter 4", "year": 2023, "type": "movie", "trakt_slug": "john-wick-chapter-4-2023", "release_order": 4, "chronological_order": 4},
        ],
    },
    {
        "name": "Jurassic Park / World",
        "slug": "jurassic",
        "description": "Jurassic Park and Jurassic World films",
        "items": [
            {"title": "Jurassic Park", "year": 1993, "type": "movie", "trakt_slug": "jurassic-park-1993", "release_order": 1, "chronological_order": 1},
            {"title": "The Lost World: Jurassic Park", "year": 1997, "type": "movie", "trakt_slug": "the-lost-world-jurassic-park-1997", "release_order": 2, "chronological_order": 2},
            {"title": "Jurassic Park III", "year": 2001, "type": "movie", "trakt_slug": "jurassic-park-iii-2001", "release_order": 3, "chronological_order": 3},
            {"title": "Jurassic World", "year": 2015, "type": "movie", "trakt_slug": "jurassic-world-2015", "release_order": 4, "chronological_order": 4},
            {"title": "Jurassic World: Fallen Kingdom", "year": 2018, "type": "movie", "trakt_slug": "jurassic-world-fallen-kingdom-2018", "release_order": 5, "chronological_order": 5},
            {"title": "Jurassic World Dominion", "year": 2022, "type": "movie", "trakt_slug": "jurassic-world-dominion-2022", "release_order": 6, "chronological_order": 6},
        ],
    },
    {
        "name": "Alien",
        "slug": "alien",
        "description": "Alien franchise including prequels",
        "items": [
            {"title": "Alien", "year": 1979, "type": "movie", "trakt_slug": "alien-1979", "release_order": 1, "chronological_order": 3},
            {"title": "Aliens", "year": 1986, "type": "movie", "trakt_slug": "aliens-1986", "release_order": 2, "chronological_order": 4},
            {"title": "Alien 3", "year": 1992, "type": "movie", "trakt_slug": "alien-3-1992", "release_order": 3, "chronological_order": 5},
            {"title": "Alien Resurrection", "year": 1997, "type": "movie", "trakt_slug": "alien-resurrection-1997", "release_order": 4, "chronological_order": 6},
            {"title": "Prometheus", "year": 2012, "type": "movie", "trakt_slug": "prometheus-2012", "release_order": 5, "chronological_order": 1},
            {"title": "Alien: Covenant", "year": 2017, "type": "movie", "trakt_slug": "alien-covenant-2017", "release_order": 6, "chronological_order": 2},
            {"title": "Alien: Romulus", "year": 2024, "type": "movie", "trakt_slug": "alien-romulus-2024", "release_order": 7, "chronological_order": 7},
        ],
    },
    {
        "name": "The Conjuring Universe",
        "slug": "conjuring",
        "description": "The Conjuring shared horror universe",
        "items": [
            {"title": "The Conjuring", "year": 2013, "type": "movie", "trakt_slug": "the-conjuring-2013", "release_order": 1, "chronological_order": 3},
            {"title": "Annabelle", "year": 2014, "type": "movie", "trakt_slug": "annabelle-2014", "release_order": 2, "chronological_order": 4},
            {"title": "The Conjuring 2", "year": 2016, "type": "movie", "trakt_slug": "the-conjuring-2-2016", "release_order": 3, "chronological_order": 5},
            {"title": "Annabelle: Creation", "year": 2017, "type": "movie", "trakt_slug": "annabelle-creation-2017", "release_order": 4, "chronological_order": 1},
            {"title": "The Nun", "year": 2018, "type": "movie", "trakt_slug": "the-nun-2018", "release_order": 5, "chronological_order": 2},
            {"title": "Annabelle Comes Home", "year": 2019, "type": "movie", "trakt_slug": "annabelle-comes-home-2019", "release_order": 6, "chronological_order": 6},
            {"title": "The Conjuring: The Devil Made Me Do It", "year": 2021, "type": "movie", "trakt_slug": "the-conjuring-the-devil-made-me-do-it-2021", "release_order": 7, "chronological_order": 7},
            {"title": "The Nun II", "year": 2023, "type": "movie", "trakt_slug": "the-nun-ii-2023", "release_order": 8, "chronological_order": 8},
        ],
    },
    {
        "name": "MonsterVerse",
        "slug": "monsterverse",
        "description": "Legendary's Godzilla and Kong shared universe",
        "items": [
            {"title": "Godzilla", "year": 2014, "type": "movie", "trakt_slug": "godzilla-2014", "release_order": 1, "chronological_order": 1},
            {"title": "Kong: Skull Island", "year": 2017, "type": "movie", "trakt_slug": "kong-skull-island-2017", "release_order": 2, "chronological_order": 2},
            {"title": "Godzilla: King of the Monsters", "year": 2019, "type": "movie", "trakt_slug": "godzilla-king-of-the-monsters-2019", "release_order": 3, "chronological_order": 3},
            {"title": "Godzilla vs. Kong", "year": 2021, "type": "movie", "trakt_slug": "godzilla-vs-kong-2021", "release_order": 4, "chronological_order": 4},
            {"title": "Godzilla x Kong: The New Empire", "year": 2024, "type": "movie", "trakt_slug": "godzilla-x-kong-the-new-empire-2024", "release_order": 5, "chronological_order": 5},
        ],
    },
    {
        "name": "Rocky / Creed",
        "slug": "rocky-creed",
        "description": "Rocky Balboa and Creed boxing saga",
        "items": [
            {"title": "Rocky", "year": 1976, "type": "movie", "trakt_slug": "rocky-1976", "release_order": 1, "chronological_order": 1},
            {"title": "Rocky II", "year": 1979, "type": "movie", "trakt_slug": "rocky-ii-1979", "release_order": 2, "chronological_order": 2},
            {"title": "Rocky III", "year": 1982, "type": "movie", "trakt_slug": "rocky-iii-1982", "release_order": 3, "chronological_order": 3},
            {"title": "Rocky IV", "year": 1985, "type": "movie", "trakt_slug": "rocky-iv-1985", "release_order": 4, "chronological_order": 4},
            {"title": "Rocky V", "year": 1990, "type": "movie", "trakt_slug": "rocky-v-1990", "release_order": 5, "chronological_order": 5},
            {"title": "Rocky Balboa", "year": 2006, "type": "movie", "trakt_slug": "rocky-balboa-2006", "release_order": 6, "chronological_order": 6},
            {"title": "Creed", "year": 2015, "type": "movie", "trakt_slug": "creed-2015", "release_order": 7, "chronological_order": 7},
            {"title": "Creed II", "year": 2018, "type": "movie", "trakt_slug": "creed-ii-2018", "release_order": 8, "chronological_order": 8},
            {"title": "Creed III", "year": 2023, "type": "movie", "trakt_slug": "creed-iii-2023", "release_order": 9, "chronological_order": 9},
        ],
    },
    {
        "name": "Planet of the Apes (Reboot)",
        "slug": "apes-reboot",
        "description": "Planet of the Apes reboot trilogy + sequel",
        "items": [
            {"title": "Rise of the Planet of the Apes", "year": 2011, "type": "movie", "trakt_slug": "rise-of-the-planet-of-the-apes-2011", "release_order": 1, "chronological_order": 1},
            {"title": "Dawn of the Planet of the Apes", "year": 2014, "type": "movie", "trakt_slug": "dawn-of-the-planet-of-the-apes-2014", "release_order": 2, "chronological_order": 2},
            {"title": "War for the Planet of the Apes", "year": 2017, "type": "movie", "trakt_slug": "war-for-the-planet-of-the-apes-2017", "release_order": 3, "chronological_order": 3},
            {"title": "Kingdom of the Planet of the Apes", "year": 2024, "type": "movie", "trakt_slug": "kingdom-of-the-planet-of-the-apes-2024", "release_order": 4, "chronological_order": 4},
        ],
    },
    {
        "name": "The Matrix",
        "slug": "matrix",
        "description": "The Matrix film series",
        "items": [
            {"title": "The Matrix", "year": 1999, "type": "movie", "trakt_slug": "the-matrix-1999", "release_order": 1, "chronological_order": 1},
            {"title": "The Matrix Reloaded", "year": 2003, "type": "movie", "trakt_slug": "the-matrix-reloaded-2003", "release_order": 2, "chronological_order": 2},
            {"title": "The Matrix Revolutions", "year": 2003, "type": "movie", "trakt_slug": "the-matrix-revolutions-2003", "release_order": 3, "chronological_order": 3},
            {"title": "The Matrix Resurrections", "year": 2021, "type": "movie", "trakt_slug": "the-matrix-resurrections-2021", "release_order": 4, "chronological_order": 4},
        ],
    },
    {
        "name": "Indiana Jones",
        "slug": "indiana-jones",
        "description": "Indiana Jones adventure films",
        "items": [
            {"title": "Raiders of the Lost Ark", "year": 1981, "type": "movie", "trakt_slug": "raiders-of-the-lost-ark-1981", "release_order": 1, "chronological_order": 2},
            {"title": "Indiana Jones and the Temple of Doom", "year": 1984, "type": "movie", "trakt_slug": "indiana-jones-and-the-temple-of-doom-1984", "release_order": 2, "chronological_order": 1},
            {"title": "Indiana Jones and the Last Crusade", "year": 1989, "type": "movie", "trakt_slug": "indiana-jones-and-the-last-crusade-1989", "release_order": 3, "chronological_order": 3},
            {"title": "Indiana Jones and the Kingdom of the Crystal Skull", "year": 2008, "type": "movie", "trakt_slug": "indiana-jones-and-the-kingdom-of-the-crystal-skull-2008", "release_order": 4, "chronological_order": 4},
            {"title": "Indiana Jones and the Dial of Destiny", "year": 2023, "type": "movie", "trakt_slug": "indiana-jones-and-the-dial-of-destiny-2023", "release_order": 5, "chronological_order": 5},
        ],
    },
    {
        "name": "Mad Max",
        "slug": "mad-max",
        "description": "Mad Max post-apocalyptic franchise",
        "items": [
            {"title": "Mad Max", "year": 1979, "type": "movie", "trakt_slug": "mad-max-1979", "release_order": 1, "chronological_order": 1},
            {"title": "Mad Max 2: The Road Warrior", "year": 1981, "type": "movie", "trakt_slug": "mad-max-2-the-road-warrior-1981", "release_order": 2, "chronological_order": 2},
            {"title": "Mad Max Beyond Thunderdome", "year": 1985, "type": "movie", "trakt_slug": "mad-max-beyond-thunderdome-1985", "release_order": 3, "chronological_order": 3},
            {"title": "Mad Max: Fury Road", "year": 2015, "type": "movie", "trakt_slug": "mad-max-fury-road-2015", "release_order": 4, "chronological_order": 4},
            {"title": "Furiosa: A Mad Max Saga", "year": 2024, "type": "movie", "trakt_slug": "furiosa-a-mad-max-saga-2024", "release_order": 5, "chronological_order": 5},
        ],
    },
    {
        "name": "Fantastic Beasts",
        "slug": "fantastic-beasts",
        "description": "Wizarding World prequels set before Harry Potter",
        "items": [
            {"title": "Fantastic Beasts and Where to Find Them", "year": 2016, "type": "movie", "trakt_slug": "fantastic-beasts-and-where-to-find-them-2016", "release_order": 1, "chronological_order": 1},
            {"title": "Fantastic Beasts: The Crimes of Grindelwald", "year": 2018, "type": "movie", "trakt_slug": "fantastic-beasts-the-crimes-of-grindelwald-2018", "release_order": 2, "chronological_order": 2},
            {"title": "Fantastic Beasts: The Secrets of Dumbledore", "year": 2022, "type": "movie", "trakt_slug": "fantastic-beasts-the-secrets-of-dumbledore-2022", "release_order": 3, "chronological_order": 3},
        ],
    },
    {
        "name": "Predator",
        "slug": "predator",
        "description": "Predator sci-fi action franchise",
        "items": [
            {"title": "Predator", "year": 1987, "type": "movie", "trakt_slug": "predator-1987", "release_order": 1, "chronological_order": 2},
            {"title": "Predator 2", "year": 1990, "type": "movie", "trakt_slug": "predator-2-1990", "release_order": 2, "chronological_order": 3},
            {"title": "Predators", "year": 2010, "type": "movie", "trakt_slug": "predators-2010", "release_order": 3, "chronological_order": 4},
            {"title": "The Predator", "year": 2018, "type": "movie", "trakt_slug": "the-predator-2018", "release_order": 4, "chronological_order": 5},
            {"title": "Prey", "year": 2022, "type": "movie", "trakt_slug": "prey-2022", "release_order": 5, "chronological_order": 1},
        ],
    },
]


class UniverseDiscoveryService:
    def __init__(self):
        self.emby = EmbyClient()

    # -----------------------------------------------------------------------
    # Public entry points
    # -----------------------------------------------------------------------

    async def run_scan(self):
        """Scheduler entry point — seed universes, resolve IDs, match library, create collections."""
        log.info("universe_discovery.scan_start")
        await self._seed_universes()
        await self._resolve_provider_ids()
        await self._match_library()
        await self._create_collections()
        log.info("universe_discovery.scan_complete")

    async def get_universes(self) -> list[dict]:
        """Return all universes with item counts + library match stats."""
        async with async_session() as db:
            universes = (await db.execute(select(Universe))).scalars().all()
            result = []
            for u in universes:
                items = (await db.execute(
                    select(UniverseItem).where(UniverseItem.universe_id == u.id)
                    .order_by(UniverseItem.release_order)
                )).scalars().all()

                in_library = sum(1 for i in items if i.in_library)
                watched = sum(1 for i in items if i.watched)

                # Next recommended = first unwatched item (in release order) that's
                # actually in the library. If the next item in order isn't in the
                # library yet, fall through to the first unwatched item that IS
                # available, so the recommendation is always something playable.
                next_recommended = None
                first_unwatched_missing = None
                for i in items:
                    if i.watched:
                        continue
                    if i.in_library:
                        next_recommended = {
                            "id": i.id,
                            "title": i.title,
                            "year": i.year,
                            "release_order": i.release_order,
                            "emby_item_id": i.emby_item_id,
                        }
                        break
                    if first_unwatched_missing is None:
                        first_unwatched_missing = {
                            "id": i.id,
                            "title": i.title,
                            "year": i.year,
                            "release_order": i.release_order,
                            "emby_item_id": None,
                        }
                if next_recommended is None:
                    next_recommended = first_unwatched_missing  # may still be None if fully watched

                result.append({
                    "id": u.id,
                    "name": u.name,
                    "slug": u.slug,
                    "description": u.description,
                    "total_items": len(items),
                    "in_library": in_library,
                    "watched": watched,
                    "completion_pct": round(watched / len(items) * 100, 1) if items else 0,
                    "next_recommended": next_recommended,
                    "items": [
                        {
                            "id": i.id,
                            "title": i.title,
                            "year": i.year,
                            "release_order": i.release_order,
                            "chronological_order": i.chronological_order,
                            "in_library": i.in_library,
                            "watched": i.watched,
                            "emby_item_id": i.emby_item_id,
                            "imdb_id": i.imdb_id,
                            "tmdb_id": i.tmdb_id,
                        }
                        for i in items
                    ],
                })
            return result

    # -----------------------------------------------------------------------
    # Seed known universes into DB
    # -----------------------------------------------------------------------

    async def _seed_universes(self):
        async with async_session() as db:
            for u_def in KNOWN_UNIVERSES:
                existing = (await db.execute(
                    select(Universe).where(Universe.slug == u_def["slug"])
                )).scalar_one_or_none()

                if existing:
                    universe = existing
                else:
                    universe = Universe(
                        name=u_def["name"],
                        slug=u_def["slug"],
                        description=u_def.get("description", ""),
                    )
                    db.add(universe)
                    await db.flush()

                # upsert items
                for item_def in u_def["items"]:
                    existing_item = (await db.execute(
                        select(UniverseItem).where(
                            UniverseItem.universe_id == universe.id,
                            UniverseItem.title == item_def["title"],
                        )
                    )).scalar_one_or_none()

                    if existing_item:
                        # Backfill trakt_slug if it was added after initial seed
                        if item_def.get("trakt_slug") and not existing_item.trakt_id:
                            existing_item.trakt_id = item_def["trakt_slug"]
                    else:
                        db.add(UniverseItem(
                            universe_id=universe.id,
                            trakt_id=item_def.get("trakt_slug"),
                            title=item_def["title"],
                            item_type=item_def.get("type", "movie"),
                            year=item_def.get("year"),
                            release_order=item_def.get("release_order", 0),
                            chronological_order=item_def.get("chronological_order", 0),
                        ))

                universe.total_items = len(u_def["items"])

            await db.commit()
        log.info("universe_discovery.seeded", count=len(KNOWN_UNIVERSES))

    # -----------------------------------------------------------------------
    # Resolve IMDB / TMDB IDs from Trakt for each universe item
    # -----------------------------------------------------------------------

    async def _resolve_provider_ids(self):
        """Look up IMDB/TMDB IDs from Trakt for universe items that don't have them yet.

        Uses the trakt_slug (direct lookup) or falls back to search by title+year.
        Resolved IDs are cached on the UniverseItem row so this only hits Trakt
        once per item across all future scans.
        """
        trakt = TraktClient()
        resolved = 0
        errors = 0

        try:
            async with async_session() as db:
                # Only resolve items missing both IDs
                items = (await db.execute(
                    select(UniverseItem).where(
                        (UniverseItem.imdb_id.is_(None)) & (UniverseItem.tmdb_id.is_(None))
                    )
                )).scalars().all()

                if not items:
                    log.info("universe_discovery.resolve_ids_skip", reason="all_resolved")
                    return

                log.info("universe_discovery.resolve_ids_start", count=len(items))

                for ui in items:
                    try:
                        ids = None
                        kind = "movies" if ui.item_type == "movie" else "shows"

                        # Strategy 1: direct lookup by trakt_slug
                        if ui.trakt_id:  # trakt_id column stores the slug
                            try:
                                details = await trakt.get_item_details(kind, ui.trakt_id)
                                ids = details.get("ids", {})
                            except Exception:
                                pass  # slug might be wrong, fall through to search

                        # Strategy 2: search by title + year
                        if not ids:
                            results = await trakt.search(
                                f"{ui.title} {ui.year or ''}".strip(),
                                kind="movie" if ui.item_type == "movie" else "show",
                            )
                            if results:
                                # Pick the best match — first result with matching year
                                for r in results:
                                    item_data = r.get("movie") or r.get("show") or {}
                                    if item_data.get("year") == ui.year:
                                        ids = item_data.get("ids", {})
                                        break
                                # Fall back to first result if no year match
                                if not ids and results:
                                    item_data = results[0].get("movie") or results[0].get("show") or {}
                                    ids = item_data.get("ids", {})

                        if ids:
                            imdb = ids.get("imdb")
                            tmdb = ids.get("tmdb")
                            if imdb:
                                ui.imdb_id = str(imdb)
                            if tmdb:
                                ui.tmdb_id = str(tmdb)
                            # Also backfill trakt_id/slug if we didn't have one
                            if not ui.trakt_id and ids.get("slug"):
                                ui.trakt_id = ids["slug"]
                            resolved += 1
                        else:
                            log.warning("universe_discovery.resolve_ids_miss",
                                        title=ui.title, year=ui.year)

                    except Exception as e:
                        errors += 1
                        log.warning("universe_discovery.resolve_ids_error",
                                    title=ui.title, error=str(e)[:200])

                    # Respect Trakt rate limits — small delay between lookups
                    import asyncio
                    await asyncio.sleep(0.3)

                await db.commit()
        finally:
            await trakt.close()

        log.info("universe_discovery.resolve_ids_done",
                 resolved=resolved, errors=errors)

    # -----------------------------------------------------------------------
    # Match universe items to Emby library
    # -----------------------------------------------------------------------

    async def _match_library(self):
        """Match universe items to Emby library using provider IDs first,
        falling back to title+year string matching.

        Priority: IMDB ID > TMDB ID > exact title:year > title-only
        """
        async with async_session() as db:
            first_user = (await db.execute(
                select(User).order_by(User.id)
            )).scalars().first()
        emby_user_id = first_user.emby_user_id if first_user else None

        movies = await self.emby.get_all_movies(user_id=emby_user_id)
        series = await self.emby.get_all_series(user_id=emby_user_id)
        all_items = movies + series

        # Build multiple indexes for matching
        imdb_index: dict[str, dict] = {}   # "tt0371746" → emby item
        tmdb_index: dict[str, dict] = {}   # "1726" → emby item
        title_year_index: dict[str, dict] = {}  # "iron man:2008" → emby item
        title_index: dict[str, dict] = {}       # "iron man" → emby item

        for item in all_items:
            provider_ids = item.get("ProviderIds", {})

            # Index by IMDB
            imdb = provider_ids.get("Imdb") or provider_ids.get("imdb")
            if imdb:
                imdb_index[imdb.lower()] = item

            # Index by TMDB
            tmdb = provider_ids.get("Tmdb") or provider_ids.get("tmdb")
            if tmdb:
                tmdb_index[str(tmdb)] = item

            # Index by title+year and title-only (fallback)
            name = item.get("Name", "").lower()
            year = item.get("ProductionYear", "")
            if name:
                title_year_index[f"{name}:{year}"] = item
                title_index[name] = item

        async with async_session() as db:
            universe_items = (await db.execute(select(UniverseItem))).scalars().all()

            matched = 0
            match_methods = {"imdb": 0, "tmdb": 0, "title_year": 0, "title": 0}

            for ui in universe_items:
                emby_item = None
                method = None

                # 1. Match by IMDB ID (most reliable)
                if ui.imdb_id:
                    emby_item = imdb_index.get(ui.imdb_id.lower())
                    if emby_item:
                        method = "imdb"

                # 2. Match by TMDB ID
                if not emby_item and ui.tmdb_id:
                    emby_item = tmdb_index.get(str(ui.tmdb_id))
                    if emby_item:
                        method = "tmdb"

                # 3. Fallback: title + year
                if not emby_item:
                    key_exact = f"{ui.title.lower()}:{ui.year}"
                    emby_item = title_year_index.get(key_exact)
                    if emby_item:
                        method = "title_year"

                # 4. Last resort: title only
                if not emby_item:
                    emby_item = title_index.get(ui.title.lower())
                    if emby_item:
                        method = "title"

                if emby_item:
                    ui.in_library = True
                    ui.emby_item_id = emby_item["Id"]
                    ui.watched = emby_item.get("UserData", {}).get("Played", False)
                    matched += 1
                    if method:
                        match_methods[method] += 1
                else:
                    ui.in_library = False
                    ui.emby_item_id = None

            await db.commit()
        log.info("universe_discovery.matched",
                 matched=matched, total=len(universe_items),
                 methods=match_methods)

    # -----------------------------------------------------------------------
    # Create Emby playlists (preserves watch order)
    # -----------------------------------------------------------------------

    async def _create_collections(self):
        """Create ordered Emby playlists for each universe.

        Uses playlists (not collections) because playlists preserve the
        insertion order of item IDs.  Collections sort by their own rules
        (release date / name) and ignore the order items were added.
        """
        async with async_session() as db:
            first_user = (await db.execute(
                select(User).order_by(User.id)
            )).scalars().first()
            emby_user_id = first_user.emby_user_id if first_user else None

            universes = (await db.execute(select(Universe))).scalars().all()

            for u in universes:
                items = (await db.execute(
                    select(UniverseItem)
                    .where(UniverseItem.universe_id == u.id, UniverseItem.in_library == True)
                    .order_by(UniverseItem.release_order)
                )).scalars().all()

                if not items:
                    continue

                emby_ids = [i.emby_item_id for i in items if i.emby_item_id]
                if emby_ids:
                    playlist_name = f"🌌 {u.name}"
                    await self.emby.recreate_playlist(
                        playlist_name, emby_ids, user_id=emby_user_id,
                    )
                    log.info("universe_discovery.playlist_created",
                             universe=u.name, items=len(emby_ids))
