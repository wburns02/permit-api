#!/usr/bin/env python3
"""
OpenGov / ViewPoint Cloud Permit Scraper

Authenticates via Auth0 implicit grant (captures Bearer token from URL fragment),
then fetches permit records from each configured portal via the GraphQL API.
Loads into hot_leads on T430 (100.122.216.15, db: permits).

Auth:
  - Auth0 domain: accounts.viewpointcloud.com
  - Client ID: Kne3XYPvChciFOG9DvQ01Ukm1wyBTdTQ
  - One token works for ALL portals (universal Auth0 tenant)
  - Token is cached to disk and reused until expiry (24h)

API (GraphQL, NOT REST):
  - Endpoint: POST https://records.viewpointcloud.com/graphql
  - Community passed via 'community' HTTP header (NOT URL path)
  - Query: getRecords(where: {isEnabled: true}, page: {page: N, size: 100})
  - Uses inline fragments: ... on RecordPaginated { records { ... } page { total } }

Usage:
    python3 scrape_opengov.py                    # First 10 portals
    python3 scrape_opengov.py --all              # All portals
    python3 scrape_opengov.py --community stpetersburgfl  # Single portal
    python3 scrape_opengov.py --days 7           # Last 7 days
    python3 scrape_opengov.py --dry-run          # Don't write to DB
    python3 scrape_opengov.py --all --days 30    # All portals, last 30 days
"""

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime, date, timedelta
from pathlib import Path
from urllib.parse import urlencode, urlparse

try:
    import requests
except ImportError:
    print("pip install requests")
    sys.exit(1)

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("pip install psycopg2-binary")
    sys.exit(1)

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
except ImportError:
    print("pip install playwright && playwright install chromium")
    sys.exit(1)

# ── Config ──────────────────────────────────────────────────────────────────

AUTH0_DOMAIN = "accounts.viewpointcloud.com"
AUTH0_CLIENT_ID = "Kne3XYPvChciFOG9DvQ01Ukm1wyBTdTQ"
AUTH0_AUDIENCE = "viewpointcloud.com/api/production"
AUTH0_PORTAL = "stpetersburgfl"  # Any portal works for auth

# Auth credentials
OPENGOV_EMAIL = os.getenv("OPENGOV_EMAIL", "willwalterburns@gmail.com")
OPENGOV_PASSWORD = os.getenv("OPENGOV_PASSWORD", "#Espn2025")

# GraphQL API endpoint (NOT the REST API which returns empty data)
GRAPHQL_ENDPOINT = "https://records.viewpointcloud.com/graphql"
PAGE_SIZE = 100

DB_HOST = os.getenv("DB_HOST", "100.122.216.15")
DB_PORT = "5432"
DB_NAME = "permits"
DB_USER = "will"

RATE_PORTAL = 2.0   # seconds between portals
RATE_PAGE   = 1.0   # seconds between pages

# Token cache path (reuse across runs)
# Supports multiple machines: R730 uses /home/will/scrapers/opengov/, local uses ReactCRM path
_TOKEN_CACHE_CANDIDATES = [
    Path("/home/will/ReactCRM/scrapers/opengov/auth_token.json"),
    Path("/home/will/scrapers/opengov/auth_token.json"),
]
# Use whichever parent directory already exists, prefer ReactCRM if on dev machine
TOKEN_CACHE = next(
    (p for p in _TOKEN_CACHE_CANDIDATES if p.parent.exists()),
    _TOKEN_CACHE_CANDIDATES[-1]  # fallback: scrapers/opengov
)

# ── Portal List (214 configured, extracted from opengov-config.ts) ──────────

JURISDICTIONS = [
    # California
    {"id": "countyoflakeca",      "name": "Lake County",                "state": "CA"},
    {"id": "beniciaca",           "name": "Benicia",                    "state": "CA", "county": "Solano"},
    {"id": "cityofsanrafaelca",   "name": "San Rafael",                 "state": "CA"},
    {"id": "camarilloca",         "name": "Camarillo",                  "state": "CA"},
    {"id": "eurekaca",            "name": "Eureka",                     "state": "CA"},
    {"id": "scottsvalleyca",      "name": "Scotts Valley",              "state": "CA"},
    {"id": "elsegundoca",         "name": "El Segundo",                 "state": "CA"},
    {"id": "americancanyonca",    "name": "American Canyon",            "state": "CA"},
    {"id": "millvalleyca",        "name": "Mill Valley",                "state": "CA"},
    {"id": "sonomaca",            "name": "Sonoma",                     "state": "CA"},
    {"id": "lakeportca",          "name": "Lakeport",                   "state": "CA"},
    {"id": "clearlakeca",         "name": "Clearlake",                  "state": "CA"},
    {"id": "westernriversidecogca","name": "Western Riverside COG",     "state": "CA"},
    {"id": "calimesaca",          "name": "Calimesa",                   "state": "CA"},
    {"id": "cityoflapalmaca",     "name": "La Palma",                   "state": "CA"},
    {"id": "colusacountyca",      "name": "Colusa County",              "state": "CA"},
    {"id": "arcataca",            "name": "Arcata",                     "state": "CA"},
    {"id": "fortunaca",           "name": "Fortuna",                    "state": "CA"},
    {"id": "industryca",          "name": "City of Industry",           "state": "CA"},
    {"id": "countyofinyoca",      "name": "Inyo County",                "state": "CA"},
    # Connecticut
    {"id": "newcanaanct",         "name": "New Canaan",                 "state": "CT", "county": "Fairfield"},
    {"id": "bloomfieldct",        "name": "Bloomfield",                 "state": "CT"},
    {"id": "avonct",              "name": "Avon",                       "state": "CT"},
    {"id": "stamfordct",          "name": "Stamford",                   "state": "CT"},
    {"id": "danburyct",           "name": "Danbury",                    "state": "CT"},
    {"id": "norwichct",           "name": "Norwich",                    "state": "CT"},
    {"id": "glastonburyct",       "name": "Glastonbury",                "state": "CT"},
    {"id": "ridgefieldct",        "name": "Ridgefield",                 "state": "CT"},
    {"id": "stoningtonct",        "name": "Stonington",                 "state": "CT"},
    {"id": "newfairfieldct",      "name": "New Fairfield",              "state": "CT"},
    {"id": "newmilfordct",        "name": "New Milford",                "state": "CT"},
    {"id": "woodburyct",          "name": "Woodbury",                   "state": "CT"},
    {"id": "farmingtonct",        "name": "Farmington",                 "state": "CT"},
    {"id": "easthartfordct",      "name": "East Hartford",              "state": "CT"},
    {"id": "cheshirect",          "name": "Cheshire",                   "state": "CT"},
    {"id": "torringtonct",        "name": "Torrington",                 "state": "CT"},
    {"id": "newingtonct",         "name": "Newington",                  "state": "CT"},
    {"id": "branfordct",          "name": "Branford",                   "state": "CT"},
    {"id": "madisonct",           "name": "Madison",                    "state": "CT"},
    {"id": "winchesterct",        "name": "Winchester",                 "state": "CT"},
    {"id": "darienct",            "name": "Darien",                     "state": "CT"},
    {"id": "wiltonct",            "name": "Wilton",                     "state": "CT"},
    {"id": "bristolct",           "name": "Bristol",                    "state": "CT"},
    {"id": "willingtonct",        "name": "Willington",                 "state": "CT"},
    {"id": "rockyhillct",         "name": "Rocky Hill",                 "state": "CT"},
    {"id": "hamdenct",            "name": "Hamden",                     "state": "CT"},
    # Colorado
    {"id": "brightonco",          "name": "Brighton",                   "state": "CO"},
    {"id": "pueblocountyco",      "name": "Pueblo County",              "state": "CO"},
    {"id": "durangoco",           "name": "Durango",                    "state": "CO"},
    # Florida
    {"id": "stpetersburgfl",      "name": "St. Petersburg",             "state": "FL", "county": "Pinellas"},
    {"id": "apopkafl",            "name": "Apopka",                     "state": "FL", "county": "Orange"},
    {"id": "cocoabeachfl",        "name": "Cocoa Beach",                "state": "FL", "county": "Brevard"},
    {"id": "lauderdalelakesfl",   "name": "Lauderdale Lakes",           "state": "FL"},
    {"id": "stuartfl",            "name": "Stuart",                     "state": "FL"},
    {"id": "marathonfl",          "name": "Marathon",                   "state": "FL"},
    # Georgia
    {"id": "sandyspringsga",      "name": "Sandy Springs",              "state": "GA"},
    {"id": "dekalbcountyga",      "name": "DeKalb County",              "state": "GA"},
    {"id": "chambleega",          "name": "Chamblee",                   "state": "GA"},
    {"id": "smyrnaga",            "name": "Smyrna",                     "state": "GA"},
    {"id": "glynncountyga",       "name": "Glynn County",               "state": "GA"},
    # Idaho
    {"id": "stateofidaho",        "name": "State of Idaho",             "state": "ID"},
    {"id": "postfallsid",         "name": "Post Falls",                 "state": "ID"},
    # Illinois
    {"id": "deerfieldil",         "name": "Deerfield",                  "state": "IL"},
    {"id": "champaignil",         "name": "Champaign",                  "state": "IL"},
    {"id": "plainfieldil",        "name": "Plainfield",                 "state": "IL"},
    {"id": "bolingbrookil",       "name": "Bolingbrook",                "state": "IL"},
    {"id": "schaumburgil",        "name": "Schaumburg",                 "state": "IL"},
    {"id": "decaturil",           "name": "Decatur",                    "state": "IL"},
    {"id": "lemontil",            "name": "Lemont",                     "state": "IL"},
    # Indiana
    {"id": "brownsburgin",        "name": "Brownsburg",                 "state": "IN", "county": "Hendricks"},
    {"id": "fishersin",           "name": "Fishers",                    "state": "IN"},
    {"id": "garyin",              "name": "Gary",                       "state": "IN"},
    {"id": "monroecountyin",      "name": "Monroe County",              "state": "IN"},
    # Iowa
    {"id": "waukeeia",            "name": "Waukee",                     "state": "IA"},
    {"id": "polkcountyia",        "name": "Polk County",                "state": "IA"},
    # Kansas
    {"id": "springhillks",        "name": "Spring Hill",                "state": "KS"},
    {"id": "goddardks",           "name": "Goddard",                    "state": "KS"},
    # Kentucky
    {"id": "bereaky",             "name": "Berea",                      "state": "KY"},
    # Maine
    {"id": "yorkme",              "name": "York",                       "state": "ME"},
    # Maryland
    {"id": "frederickmd",         "name": "Frederick",                  "state": "MD", "county": "Frederick"},
    {"id": "baltimoremddhcd",     "name": "Baltimore DHCD",             "state": "MD"},
    {"id": "countyofdorchestermd","name": "Dorchester County",          "state": "MD"},
    {"id": "cecilcountymd",       "name": "Cecil County",               "state": "MD"},
    # Massachusetts
    {"id": "arlingtonma",         "name": "Arlington",                  "state": "MA", "county": "Middlesex"},
    {"id": "needhamma",           "name": "Needham",                    "state": "MA", "county": "Norfolk"},
    {"id": "springfieldma",       "name": "Springfield",                "state": "MA", "county": "Hampden"},
    {"id": "fallriverma",         "name": "Fall River",                 "state": "MA", "county": "Bristol"},
    {"id": "framinghamma",        "name": "Framingham",                 "state": "MA"},
    {"id": "lexingtonma",         "name": "Lexington",                  "state": "MA"},
    {"id": "tewksburyma",         "name": "Tewksbury",                  "state": "MA"},
    {"id": "bournema",            "name": "Bourne",                     "state": "MA"},
    {"id": "peabodyma",           "name": "Peabody",                    "state": "MA"},
    {"id": "cambridgema",         "name": "Cambridge",                  "state": "MA"},
    {"id": "methuenma",           "name": "Methuen",                    "state": "MA"},
    {"id": "watertownma",         "name": "Watertown",                  "state": "MA"},
    {"id": "beverlyma",           "name": "Beverly",                    "state": "MA"},
    {"id": "littletonma",         "name": "Littleton",                  "state": "MA"},
    {"id": "brewsterma",          "name": "Brewster",                   "state": "MA"},
    {"id": "newtonma",            "name": "Newton",                     "state": "MA"},
    {"id": "williamstownma",      "name": "Williamstown",               "state": "MA"},
    {"id": "boxfordma",           "name": "Boxford",                    "state": "MA"},
    {"id": "westspringfieldma",   "name": "West Springfield",           "state": "MA"},
    {"id": "southhadleyma",       "name": "South Hadley",               "state": "MA"},
    {"id": "provincetownma",      "name": "Provincetown",               "state": "MA"},
    {"id": "salemma",             "name": "Salem",                      "state": "MA"},
    {"id": "worcesterma",         "name": "Worcester",                  "state": "MA"},
    {"id": "chathamma",           "name": "Chatham",                    "state": "MA"},
    {"id": "northattleboroughma", "name": "North Attleborough",         "state": "MA"},
    {"id": "medfieldma",          "name": "Medfield",                   "state": "MA"},
    {"id": "newburyportma",       "name": "Newburyport",                "state": "MA"},
    {"id": "tisburyma",           "name": "Tisbury",                    "state": "MA"},
    {"id": "warehamma",           "name": "Wareham",                    "state": "MA"},
    {"id": "northamptonma",       "name": "Northampton",                "state": "MA"},
    {"id": "shrewsburyma",        "name": "Shrewsbury",                 "state": "MA"},
    {"id": "gardnerma",           "name": "Gardner",                    "state": "MA"},
    {"id": "stonehamma",          "name": "Stoneham",                   "state": "MA"},
    {"id": "hudsonma",            "name": "Hudson",                     "state": "MA"},
    {"id": "northboroughma",      "name": "Northborough",               "state": "MA"},
    {"id": "grotonma",            "name": "Groton",                     "state": "MA"},
    {"id": "hanoverma",           "name": "Hanover",                    "state": "MA"},
    {"id": "newbedfordma",        "name": "New Bedford",                "state": "MA"},
    {"id": "edgartownma",         "name": "Edgartown",                  "state": "MA"},
    {"id": "dennisma",            "name": "Dennis",                     "state": "MA"},
    {"id": "dudleyma",            "name": "Dudley",                     "state": "MA"},
    {"id": "natickma",            "name": "Natick",                     "state": "MA"},
    {"id": "cantonma",            "name": "Canton",                     "state": "MA"},
    # Minnesota
    {"id": "burnsvillemn",        "name": "Burnsville",                 "state": "MN"},
    {"id": "oakdalemn",           "name": "Oakdale",                    "state": "MN"},
    {"id": "northstpaulmn",       "name": "North St. Paul",             "state": "MN"},
    {"id": "winonacountymn",      "name": "Winona County",              "state": "MN"},
    {"id": "medinamn",            "name": "Medina",                     "state": "MN"},
    # Mississippi
    {"id": "jacksonms",           "name": "Jackson",                    "state": "MS"},
    # Nevada
    {"id": "nyecountynv",         "name": "Nye County",                 "state": "NV"},
    # New Hampshire
    {"id": "rochesternh",         "name": "Rochester",                  "state": "NH"},
    {"id": "claremontnh",         "name": "Claremont",                  "state": "NH"},
    # New Jersey
    {"id": "princetonnj",         "name": "Princeton",                  "state": "NJ"},
    # New York
    {"id": "ithacacityny",        "name": "Ithaca",                     "state": "NY", "county": "Tompkins"},
    {"id": "hempsteadny",         "name": "Town of Hempstead",          "state": "NY"},
    {"id": "valleystreamny",      "name": "Valley Stream",              "state": "NY"},
    {"id": "mountvernonny",       "name": "Mount Vernon",               "state": "NY"},
    {"id": "salinany",            "name": "Town of Salina",             "state": "NY"},
    {"id": "countyofonondagany",  "name": "Onondaga County",            "state": "NY"},
    {"id": "townofhuntingtonny",  "name": "Town of Huntington",         "state": "NY"},
    {"id": "mountpleasantny",     "name": "Mount Pleasant",             "state": "NY"},
    {"id": "cortlandtny",         "name": "Cortlandt",                  "state": "NY"},
    # North Carolina
    {"id": "countyofnashnc",      "name": "Nash County",                "state": "NC"},
    {"id": "chapelhillnc",        "name": "Chapel Hill",                "state": "NC"},
    {"id": "marionnc",            "name": "Marion",                     "state": "NC"},
    {"id": "warrencountync",      "name": "Warren County",              "state": "NC"},
    {"id": "chathamcountync",     "name": "Chatham County",             "state": "NC"},
    {"id": "countyofwilsonnc",    "name": "Wilson County",              "state": "NC"},
    {"id": "kingsmountainnc",     "name": "Kings Mountain",             "state": "NC"},
    {"id": "davidsoncountync",    "name": "Davidson County",            "state": "NC"},
    {"id": "southportnc",         "name": "Southport",                  "state": "NC"},
    {"id": "townofwarrenton",     "name": "Town of Warrenton",          "state": "NC"},
    # Ohio
    {"id": "tallmadgeoh",         "name": "Tallmadge",                  "state": "OH"},
    {"id": "fairfieldoh",         "name": "Fairfield",                  "state": "OH"},
    {"id": "gahannaoh",           "name": "Gahanna",                    "state": "OH"},
    {"id": "northcantonoh",       "name": "North Canton",               "state": "OH"},
    {"id": "woosteroh",           "name": "Wooster",                    "state": "OH"},
    {"id": "unioncountyoh",       "name": "Union County",               "state": "OH"},
    {"id": "hudsonoh",            "name": "Hudson",                     "state": "OH"},
    {"id": "portagecountyoh",     "name": "Portage County",             "state": "OH"},
    {"id": "plaincityoh",         "name": "Plain City",                 "state": "OH"},
    # Pennsylvania
    {"id": "scrantonpa",          "name": "Scranton",                   "state": "PA", "county": "Lackawanna"},
    {"id": "cheltenhampa",        "name": "Cheltenham Township",        "state": "PA"},
    {"id": "yorkpa",              "name": "York",                       "state": "PA"},
    {"id": "eastonpa",            "name": "Easton",                     "state": "PA"},
    {"id": "abingtonpa",          "name": "Abington Township",          "state": "PA"},
    {"id": "cranberrytownshippa", "name": "Cranberry Township",         "state": "PA"},
    {"id": "springfielddelco",    "name": "Springfield Township (Delaware County)", "state": "PA"},
    {"id": "townshiplowermakefieldpa", "name": "Lower Makefield Township", "state": "PA"},
    # Rhode Island
    {"id": "providenceri",        "name": "Providence",                 "state": "RI", "county": "Providence"},
    {"id": "eastprovidenceri",    "name": "East Providence",            "state": "RI", "county": "Providence"},
    {"id": "smithfieldri",        "name": "Smithfield",                 "state": "RI", "county": "Providence"},
    {"id": "narragansettri",      "name": "Narragansett",               "state": "RI", "county": "Washington"},
    {"id": "scituateri",          "name": "Scituate",                   "state": "RI", "county": "Providence"},
    {"id": "middletownri",        "name": "Middletown",                 "state": "RI"},
    {"id": "westwarwickri",       "name": "West Warwick",               "state": "RI"},
    {"id": "cranstonri",          "name": "Cranston",                   "state": "RI"},
    {"id": "newportri",           "name": "Newport",                    "state": "RI"},
    {"id": "westerlyri",          "name": "Westerly",                   "state": "RI"},
    {"id": "northkingstownri",    "name": "North Kingstown",            "state": "RI"},
    {"id": "newshorehamri",       "name": "New Shoreham",               "state": "RI"},
    {"id": "cumberlandri",        "name": "Cumberland",                 "state": "RI"},
    {"id": "eastgreenwichri",     "name": "East Greenwich",             "state": "RI"},
    {"id": "northprovidenceri",   "name": "North Providence",           "state": "RI"},
    {"id": "southkingstownri",    "name": "South Kingstown",            "state": "RI"},
    {"id": "coventryri",          "name": "Coventry",                   "state": "RI"},
    {"id": "richmondri",          "name": "Richmond",                   "state": "RI"},
    {"id": "bristolri",           "name": "Bristol",                    "state": "RI"},
    {"id": "glocesterri",         "name": "Glocester",                  "state": "RI"},
    # South Carolina
    {"id": "countyofandersonsc",  "name": "Anderson County",            "state": "SC"},
    {"id": "northmyrtlebeachsc",  "name": "North Myrtle Beach",         "state": "SC"},
    {"id": "goosecreeksc",        "name": "Goose Creek",                "state": "SC"},
    {"id": "andersonsc",          "name": "City of Anderson",           "state": "SC"},
    # South Dakota
    {"id": "watertownsd",         "name": "Watertown",                  "state": "SD"},
    # Tennessee
    {"id": "chattanoogatn",       "name": "Chattanooga",                "state": "TN", "county": "Hamilton"},
    {"id": "hamiltontn",          "name": "Hamilton County",            "state": "TN"},
    {"id": "metronashvilletn",    "name": "Metro Nashville",            "state": "TN"},
    # Texas
    {"id": "ennistx",             "name": "City of Ennis",              "state": "TX"},
    {"id": "bedfordtx",           "name": "City of Bedford",            "state": "TX"},
    {"id": "countyofbexartx",     "name": "Bexar County",               "state": "TX"},
    {"id": "aransaspasstx",       "name": "Aransas Pass",               "state": "TX"},
    {"id": "galvestoncountytx",   "name": "Galveston County",           "state": "TX"},
    # Vermont
    {"id": "townofbrattleborovt", "name": "Brattleboro",                "state": "VT"},
    # Washington
    {"id": "maplevalleywa",       "name": "Maple Valley",               "state": "WA"},
    # Wisconsin
    {"id": "sunprairiewi",        "name": "Sun Prairie",                "state": "WI"},
    {"id": "oconomowocwi",        "name": "Oconomowoc",                 "state": "WI"},
    {"id": "countyofsaukwi",      "name": "Sauk County",                "state": "WI"},
    # Alaska
    {"id": "sewardak",            "name": "Seward",                     "state": "AK"},
    # Wyoming
    {"id": "cheyennewy",          "name": "Cheyenne",                   "state": "WY"},
    {"id": "natronacountywy",     "name": "Natrona County",             "state": "WY"},
]

# ── Auth ─────────────────────────────────────────────────────────────────────

def load_cached_token():
    """Load saved token from disk. Return (token, expires_at_ms) or (None, 0)."""
    try:
        if TOKEN_CACHE.exists():
            data = json.loads(TOKEN_CACHE.read_text())
            token = data.get("accessToken")
            expires_at = data.get("expiresAt", 0)
            if token and expires_at:
                return token, expires_at
    except Exception as e:
        print(f"[Auth] Could not load cached token: {e}")
    return None, 0


def save_token(access_token, expires_at_ms):
    """Save token to disk for reuse."""
    try:
        TOKEN_CACHE.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "accessToken": access_token,
            "expiresAt": expires_at_ms,
            "issuedAt": int(time.time() * 1000),
            "tenant": AUTH0_PORTAL
        }
        TOKEN_CACHE.write_text(json.dumps(data, indent=2))
        print(f"[Auth] Token saved to {TOKEN_CACHE}")
    except Exception as e:
        print(f"[Auth] Could not save token: {e}")


def _try_login_playwright(email, password):
    """
    Login via Playwright Auth0 implicit grant flow.
    Captures the Bearer token from the URL fragment (#access_token=...) on redirect.

    The key technique: use page.on("framenavigated") to capture the token from the
    redirect URL BEFORE the SPA can consume it from the hash fragment.

    Returns (token, expires_at_ms) or (None, 0).
    """
    from urllib.parse import urlparse

    redirect_uri = f"https://{AUTH0_PORTAL}.portal.opengov.com"
    nonce = "opengov_scraper_" + str(int(time.time()))

    params = {
        "client_id": AUTH0_CLIENT_ID,
        "response_type": "token id_token",
        "redirect_uri": redirect_uri,
        "scope": "openid profile email",
        "audience": AUTH0_AUDIENCE,
        "nonce": nonce,
        "prompt": "login"
    }
    auth_url = f"https://{AUTH0_DOMAIN}/authorize?" + urlencode(params)
    captured = {"token": None, "expires_at": 0}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800}
        )
        page = context.new_page()

        # Critical: capture token from framenavigated event
        # The SPA reads and clears the URL hash, so we must intercept it here
        def on_frame_navigated(frame):
            if frame == page.main_frame:
                url = frame.url
                if "access_token=" in url:
                    token_data = _parse_token_from_url(url)
                    if token_data:
                        captured["token"] = token_data["access_token"]
                        captured["expires_at"] = token_data["expires_at"]
                        print(f"[Auth] Token captured! len={len(captured['token'])}")

        page.on("framenavigated", on_frame_navigated)

        try:
            # Navigate to Auth0 authorize URL
            # Use domcontentloaded instead of networkidle to avoid waiting for SPA load
            page.goto(auth_url, timeout=30000, wait_until="domcontentloaded")
            time.sleep(2)

            # Check if already redirected with token (e.g., from session cookie)
            if captured["token"]:
                browser.close()
                return captured["token"], captured["expires_at"]

            # Also check page URL directly
            if "access_token=" in page.url:
                token_data = _parse_token_from_url(page.url)
                if token_data:
                    captured["token"] = token_data["access_token"]
                    captured["expires_at"] = token_data["expires_at"]
                    browser.close()
                    return captured["token"], captured["expires_at"]

            # Wait for Auth0 Lock login form
            try:
                page.wait_for_selector('input[name="email"], input[type="email"]', timeout=10000)
            except PlaywrightTimeout:
                print("[Auth] No login form found")
                browser.close()
                return None, 0

            # Fill email and password (Auth0 Lock shows both fields)
            page.fill('input[name="email"]', email)
            page.fill('input[name="password"]', password)

            # Click submit
            submit_btn = page.query_selector('button[type="submit"]')
            if submit_btn:
                submit_btn.click()
            else:
                print("[Auth] No submit button found")
                browser.close()
                return None, 0

            # Wait for redirect with token (up to 20 seconds)
            for i in range(20):
                time.sleep(1)
                if captured["token"]:
                    break
                # Also poll the URL directly as a fallback
                try:
                    current_url = page.url
                    if "access_token=" in current_url:
                        token_data = _parse_token_from_url(current_url)
                        if token_data:
                            captured["token"] = token_data["access_token"]
                            captured["expires_at"] = token_data["expires_at"]
                            break
                except Exception:
                    pass

            # Check for login errors
            if not captured["token"]:
                try:
                    error_el = page.query_selector('.auth0-lock-error-msg, .auth0-global-message-error')
                    if error_el:
                        err_text = error_el.inner_text().strip()
                        if err_text:
                            print(f"[Auth] Login error: {err_text}")
                except Exception:
                    pass

        except Exception as e:
            print(f"[Auth] Playwright error: {e}")
        finally:
            browser.close()

    return captured["token"], captured["expires_at"]


def authenticate(token_override=None):
    """
    Authenticate via Auth0 implicit grant using Playwright.
    Captures the Bearer token from the URL fragment after login redirect.

    Strategy (in order):
    1. --token flag or OPENGOV_TOKEN env var (direct override)
    2. Cached token from disk (if not expired)
    3. Full login with Playwright (Auth0 Lock widget)

    Returns:
        access_token string
    """
    # Use manually provided token if given
    if token_override:
        print("[Auth] Using provided token override")
        return token_override

    # Check cache first
    cached_token, expires_at = load_cached_token()
    now_ms = int(time.time() * 1000)
    buffer_ms = 5 * 60 * 1000  # 5 minute buffer

    if cached_token and expires_at > now_ms + buffer_ms:
        remaining_hours = (expires_at - now_ms) // 3600000
        print(f"[Auth] Using cached token (valid for ~{remaining_hours}h)")
        return cached_token

    print("[Auth] Token expired or missing -- re-authenticating...")

    # Full login with credentials
    if OPENGOV_EMAIL and OPENGOV_PASSWORD:
        print(f"[Auth] Logging in as {OPENGOV_EMAIL}")
        token, expires_at_ms = _try_login_playwright(OPENGOV_EMAIL, OPENGOV_PASSWORD)
        if token:
            save_token(token, expires_at_ms)
            print("[Auth] Authentication successful!")
            return token
        print(f"[Auth] Login failed for {OPENGOV_EMAIL}")

    raise RuntimeError(
        "[Auth] Failed to obtain access token.\n\n"
        "  To fix:\n"
        "  1. Provide a token directly:\n"
        "     python3 scrape_opengov.py --token <bearer_token>\n\n"
        "  2. Or set env vars:\n"
        "     OPENGOV_EMAIL=your@email OPENGOV_PASSWORD=yourpass python3 scrape_opengov.py\n\n"
        "  3. Or set OPENGOV_TOKEN=<bearer_token> in env"
    )


def _parse_token_from_url(url):
    """Parse access_token and expires_in from URL fragment (#access_token=...&expires_in=...)."""
    try:
        fragment = urlparse(url).fragment
        if not fragment:
            return None
        params = dict(pair.split("=", 1) for pair in fragment.split("&") if "=" in pair)
        access_token = params.get("access_token")
        expires_in = int(params.get("expires_in", "86400"))
        if access_token:
            return {
                "access_token": access_token,
                "expires_at": int(time.time() * 1000) + expires_in * 1000
            }
    except Exception as e:
        print(f"[Auth] Error parsing token from URL: {e}")
    return None


# ── GraphQL API ─────────────────────────────────────────────────────────────

# GraphQL query to fetch permit records with all available fields
RECORDS_QUERY = """
query GetRecords($where: RecordsWhereInput!, $page: PageInput!, $sort: RecordSort) {
    getRecords(where: $where, page: $page, sort: $sort) {
        ... on RecordPaginated {
            records {
                recordID
                recordNo
                fullAddress
                recordTypeID
                recordType {
                    categoryID
                    category { name }
                }
                status
                isEnabled
                dateCreated
                lastUpdatedDate
                description
                city
                state
                postalCode
                streetNo
                streetName
                ownerName
                latitude
                longitude
                applicantFullName
                applicantFirstName
                applicantLastName
            }
            page { total page size }
        }
    }
}
"""


def fetch_records_page(community, token, page_num):
    """
    Fetch one page of records via the GraphQL API.
    Returns (records_list, total_count).
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "community": community,
    }

    variables = {
        "where": {"isEnabled": True},
        "page": {"page": page_num, "size": PAGE_SIZE},
        "sort": {"field": "lastUpdatedDate", "direction": "DESC"},
    }

    try:
        resp = requests.post(
            GRAPHQL_ENDPOINT,
            headers=headers,
            json={"query": RECORDS_QUERY, "variables": variables},
            timeout=30,
        )

        if resp.status_code == 401:
            print(f"  [API] 401 Unauthorized for {community} -- token may be invalid")
            return [], 0
        if resp.status_code == 429:
            print(f"  [API] Rate limited on {community} -- waiting 10s")
            time.sleep(10)
            return [], 0
        if not resp.ok:
            print(f"  [API] HTTP {resp.status_code} for {community}: {resp.text[:200]}")
            return [], 0

        data = resp.json()

        # Check for GraphQL errors
        if data.get("errors"):
            msg = data["errors"][0].get("message", "")
            print(f"  [API] GraphQL error for {community}: {msg[:150]}")
            return [], -1  # -1 signals skip

        # Extract records from the response
        get_records = (data.get("data") or {}).get("getRecords") or {}
        records = get_records.get("records") or []
        total = (get_records.get("page") or {}).get("total", 0)
        return records, total

    except requests.exceptions.Timeout:
        print(f"  [API] Timeout fetching page {page_num} for {community}")
        return [], 0
    except Exception as e:
        print(f"  [API] Error fetching {community} page {page_num}: {e}")
        return [], 0


def scrape_portal(jurisdiction, token, since_date=None):
    """
    Scrape records from one portal, sorted by lastUpdatedDate DESC.
    Stops paginating once records are older than since_date.
    Returns list of transformed records.
    """
    community = jurisdiction["id"]
    name = jurisdiction["name"]
    state = jurisdiction["state"]

    print(f"\n  Portal: {name}, {state} ({community})")

    all_records = []
    page_num = 1
    total = None
    stop_early = False

    while True:
        raw_records, count = fetch_records_page(community, token, page_num)

        if count == -1:
            # Portal error -- skip
            break

        if total is None and count:
            total = count
            print(f"  Total records in portal: {total:,}")

        if not raw_records:
            if page_num == 1 and total and total > 0:
                print(f"  Total={total:,} but 0 records returned (may need higher access)")
            elif page_num == 1:
                print(f"  No records returned")
            break

        # Transform and filter by since_date
        page_kept = 0
        oldest_on_page = None
        for raw in raw_records:
            # Track the oldest record on this page (for early stop)
            updated_at = raw.get("lastUpdatedDate") or raw.get("dateCreated") or ""
            if updated_at:
                try:
                    record_date = datetime.fromisoformat(updated_at.replace("Z", "+00:00")).date()
                    if oldest_on_page is None or record_date < oldest_on_page:
                        oldest_on_page = record_date
                except Exception:
                    pass

            record = transform_record(raw, jurisdiction)
            if record:
                # Apply date filter: skip records older than since_date
                if since_date:
                    record_updated = None
                    updated_str = raw.get("lastUpdatedDate") or raw.get("dateCreated") or ""
                    if updated_str:
                        try:
                            record_updated = datetime.fromisoformat(updated_str.replace("Z", "+00:00")).date()
                        except Exception:
                            pass
                    if record_updated and record_updated < since_date:
                        continue
                all_records.append(record)
                page_kept += 1

        print(f"  Page {page_num}: {len(raw_records)} fetched, {page_kept} kept ({len(all_records)} total)")

        # Early stop: if results are sorted by date DESC and the oldest record
        # on this page is before our cutoff, no point paginating further
        if since_date and oldest_on_page and oldest_on_page < since_date:
            print(f"  Reached records from {oldest_on_page} (before {since_date}) -- stopping")
            break

        # Check if there are more pages
        # Note: API may return fewer records than page size (nulls/filtered),
        # so use total count, not len(raw_records)
        if total and page_num * PAGE_SIZE >= total:
            break
        if not raw_records:
            break

        page_num += 1
        time.sleep(RATE_PAGE)

    print(f"  Done: {len(all_records):,} records from {name}")
    return all_records


# Status code mapping for OpenGov records
STATUS_MAP = {
    0: "Draft",
    1: "Submitted",
    2: "In Review",
    3: "Approved",
    4: "Denied",
    5: "Expired",
    -1: "Cancelled",
}


def transform_record(raw, jurisdiction):
    """Transform a GraphQL record to hot_leads schema."""
    # Parse issue_date
    created_at = raw.get("dateCreated") or ""
    issue_date = None
    if created_at:
        try:
            issue_date = datetime.fromisoformat(created_at.replace("Z", "+00:00")).date()
        except Exception:
            pass

    # Parse address
    address = (raw.get("fullAddress") or "").strip()
    city = (raw.get("city") or jurisdiction["name"]).strip()
    state = raw.get("state") or jurisdiction["state"]
    if state and len(state) > 2:
        state = jurisdiction["state"]

    permit_number = raw.get("recordNo") or str(raw.get("recordID", ""))

    # Get permit type from nested recordType
    record_type = raw.get("recordType") or {}
    category = record_type.get("category") or {}
    permit_type = category.get("name") or ""

    # Map numeric status to string
    raw_status = raw.get("status")
    if isinstance(raw_status, int):
        status = STATUS_MAP.get(raw_status, f"Status_{raw_status}")
    else:
        status = str(raw_status or "")

    description = (raw.get("description") or "").strip()
    applicant_name = (
        raw.get("applicantFullName") or
        " ".join(filter(None, [raw.get("applicantFirstName"), raw.get("applicantLastName")])) or
        ""
    ).strip()
    owner_name = (raw.get("ownerName") or "").strip()
    zip_code = (raw.get("postalCode") or "").strip()
    lat = raw.get("latitude")
    lng = raw.get("longitude")

    # Only include records with at least a permit number or address
    if not permit_number and not address:
        return None

    return {
        "permit_number": permit_number[:200] if permit_number else None,
        "permit_type": permit_type[:200] if permit_type else None,
        "description": description[:500] if description else None,
        "address": address[:300] if address else None,
        "city": city[:100] if city else None,
        "state": (state or jurisdiction["state"])[:2],
        "zip": zip_code[:10] if zip_code else None,
        "county": jurisdiction.get("county", "")[:100] or None,
        "lat": float(lat) if lat else None,
        "lng": float(lng) if lng else None,
        "issue_date": issue_date,
        "status": status[:100] if status else None,
        "valuation": None,  # Not available in GraphQL response
        "applicant_name": applicant_name[:200] if applicant_name else None,
        "owner_name": owner_name[:200] if owner_name else None,
        "contractor_name": None,  # Not directly available
        "contractor_company": None,  # Not directly available
        "jurisdiction": f"{jurisdiction['name']}, {jurisdiction['state']}",
        "source": f"opengov_{jurisdiction['id']}",
    }


# ── Database ─────────────────────────────────────────────────────────────────

def get_db_conn():
    """Connect to T430 PostgreSQL."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        connect_timeout=15
    )


def insert_records(records, dry_run=False):
    """Bulk-insert records into hot_leads, ignoring duplicates."""
    if not records:
        return 0

    if dry_run:
        print(f"  [DB] DRY RUN — would insert {len(records)} records")
        return len(records)

    rows = [
        (
            str(uuid.uuid4()),
            r.get("permit_number"),
            r.get("permit_type"),
            None,   # work_class
            r.get("description"),
            r.get("address"),
            r.get("city"),
            r.get("state", "??"),
            r.get("zip"),
            r.get("county"),
            r.get("lat"),
            r.get("lng"),
            r.get("issue_date"),
            None,   # applied_date
            r.get("status"),
            r.get("valuation"),
            None,   # sqft
            None,   # housing_units
            r.get("contractor_company"),
            r.get("contractor_name"),
            None, None, None, None,  # phone/addr/city/zip
            None,   # contractor_trade
            r.get("applicant_name"),
            None,   # applicant_org
            None,   # applicant_phone
            r.get("owner_name"),
            r.get("jurisdiction"),
            r.get("source"),
        )
        for r in records
    ]

    insert_sql = """
        INSERT INTO hot_leads (
            id, permit_number, permit_type, work_class, description,
            address, city, state, zip, county, lat, lng,
            issue_date, applied_date, status, valuation, sqft, housing_units,
            contractor_company, contractor_name, contractor_phone, contractor_address,
            contractor_city, contractor_zip, contractor_trade,
            applicant_name, applicant_org, applicant_phone,
            owner_name, jurisdiction, source
        )
        VALUES %s
        ON CONFLICT (permit_number, source) DO NOTHING
    """

    try:
        conn = get_db_conn()
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, rows, page_size=500)
                inserted = cur.rowcount
        conn.close()
        return inserted
    except Exception as e:
        print(f"  [DB] Insert error: {e}")
        return 0


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="OpenGov permit scraper")
    parser.add_argument("--all", action="store_true", help="Scrape all 214 portals (default: first 10)")
    parser.add_argument("--community", help="Scrape single portal by community slug")
    parser.add_argument("--days", type=int, default=90, help="Number of days to look back (default: 90)")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    parser.add_argument("--token", help="Bearer token to use directly (skip Playwright auth). Get from browser DevTools.")
    parser.add_argument("--list", action="store_true", help="List all configured portals and exit")
    args = parser.parse_args()

    if args.list:
        print(f"{'ID':<35} {'Name':<35} State")
        print("-" * 80)
        for j in JURISDICTIONS:
            print(f"{j['id']:<35} {j['name']:<35} {j['state']}")
        print(f"\nTotal: {len(JURISDICTIONS)} portals")
        return

    since_date = date.today() - timedelta(days=args.days)
    print("=" * 60)
    print("OPENGOV / VIEWPOINT CLOUD PERMIT SCRAPER")
    print("=" * 60)
    print(f"Started:    {datetime.now().isoformat()}")
    print(f"Since date: {since_date}")
    print(f"DB target:  {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"Dry run:    {args.dry_run}")

    # Select portals to scrape
    if args.community:
        portals = [j for j in JURISDICTIONS if j["id"] == args.community]
        if not portals:
            print(f"ERROR: Community '{args.community}' not found in config")
            print("Available communities:")
            for j in JURISDICTIONS:
                print(f"  {j['id']}  ({j['name']}, {j['state']})")
            sys.exit(1)
        print(f"Mode:       single portal ({args.community})")
    elif args.all:
        portals = JURISDICTIONS
        print(f"Mode:       all {len(portals)} portals")
    else:
        portals = JURISDICTIONS[:10]
        print(f"Mode:       first 10 portals (use --all for all {len(JURISDICTIONS)})")

    print()

    # Also check OPENGOV_TOKEN env var
    token_override = args.token or os.getenv("OPENGOV_TOKEN")

    # Authenticate
    try:
        token = authenticate(token_override=token_override)
    except RuntimeError as e:
        print(f"FATAL: {e}")
        sys.exit(1)

    # Test the token on one portal before the full run
    print("\n[Pre-flight] Testing token on sandyspringsga (GraphQL)...")
    test_records, test_count = fetch_records_page("sandyspringsga", token, 1)
    if test_count == 0 and not test_records:
        print("[Pre-flight] WARNING: Got 0 records from test portal -- auth may have failed")
        print("[Pre-flight] Continuing anyway...")
    else:
        print(f"[Pre-flight] OK -- got {len(test_records)} records (total: {test_count:,})")

    # Scrape each portal
    grand_total_scraped = 0
    grand_total_inserted = 0
    errors = []

    for i, portal in enumerate(portals, 1):
        print(f"\n[{i}/{len(portals)}] {portal['name']}, {portal['state']} ({portal['id']})")

        try:
            records = scrape_portal(portal, token, since_date)
            grand_total_scraped += len(records)

            if records:
                inserted = insert_records(records, dry_run=args.dry_run)
                grand_total_inserted += inserted
                print(f"  Inserted: {inserted}/{len(records)}")
            else:
                print(f"  No records to insert")

        except Exception as e:
            error_msg = f"{portal['id']}: {e}"
            print(f"  ERROR: {error_msg}")
            errors.append(error_msg)

        # Rate limit between portals
        if i < len(portals):
            time.sleep(RATE_PORTAL)

    # Summary
    print("\n" + "=" * 60)
    print("COMPLETE")
    print("=" * 60)
    print(f"Finished:    {datetime.now().isoformat()}")
    print(f"Portals:     {len(portals)}")
    print(f"Scraped:     {grand_total_scraped} records")
    print(f"Inserted:    {grand_total_inserted} records")
    if errors:
        print(f"Errors ({len(errors)}):")
        for e in errors:
            print(f"  - {e}")
    print("=" * 60)


if __name__ == "__main__":
    main()
