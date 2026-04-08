#!/usr/bin/env python3
"""
OpenGov / ViewPoint Cloud Permit Scraper

Authenticates via Auth0 implicit grant (captures Bearer token from URL fragment),
then fetches permit records from each configured portal via the REST API.
Loads into hot_leads on T430 (100.122.216.15, db: permits).

Auth:
  - Auth0 domain: accounts.viewpointcloud.com
  - Client ID: Kne3XYPvChciFOG9DvQ01Ukm1wyBTdTQ
  - One token works for ALL 214 portals (universal Auth0 tenant)
  - Token is cached to disk and reused until expiry

API:
  - Records: GET https://api-east.viewpointcloud.com/v2/{community}/records
  - Filter by date via ?filter[updatedAt][gte]={iso_date}
  - Paginated via ?page[number]={n}&page[size]=100

Usage:
    python3 scrape_opengov.py                    # First 10 portals, last 90 days
    python3 scrape_opengov.py --all              # All 214 portals
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
from urllib.parse import urlencode

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

# Auth credentials — try willwalterburns@gmail.com first, then will@macseptic.com
# The JWT in auth_token.json was issued to will@macseptic.com
OPENGOV_EMAIL = os.getenv("OPENGOV_EMAIL", "willwalterburns@gmail.com")
OPENGOV_PASSWORD = os.getenv("OPENGOV_PASSWORD", "z4pFUuvq7xE7YXi")

# Alternative credentials (account that had a working token)
OPENGOV_EMAIL_ALT = os.getenv("OPENGOV_EMAIL_ALT", "will@macseptic.com")
OPENGOV_PASSWORD_ALT = os.getenv("OPENGOV_PASSWORD_ALT", "")

API_BASE = "https://api-east.viewpointcloud.com/v2"
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
    Try a single login attempt via Playwright.
    Returns (token, expires_at_ms) or (None, 0).
    """
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
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800}
        )
        page = context.new_page()

        def on_frame_navigated(frame):
            if frame == page.main_frame:
                url = frame.url
                if "access_token=" in url:
                    token_data = _parse_token_from_url(url)
                    if token_data:
                        captured["token"] = token_data["access_token"]
                        captured["expires_at"] = token_data["expires_at"]
                        print("[Auth] Token captured from redirect URL!")

        page.on("framenavigated", on_frame_navigated)

        try:
            page.goto(auth_url, timeout=30000)

            if captured["token"]:
                browser.close()
                return captured["token"], captured["expires_at"]

            # Auth0 Lock widget has both email+password on same page
            try:
                page.wait_for_selector('input[type="email"]', timeout=10000)
            except PlaywrightTimeout:
                browser.close()
                return None, 0

            # Fill email and password together (Auth0 Lock shows both)
            time.sleep(1)  # let the widget finish rendering
            email_input = page.query_selector('input[type="email"]')
            pwd_input = page.query_selector('input[type="password"]')

            if email_input:
                email_input.click()
                email_input.fill(email)
            if pwd_input:
                pwd_input.click()
                pwd_input.fill(password)

            # Click submit
            submit_btn = page.query_selector('button[type="submit"]')
            if submit_btn:
                submit_btn.click()

            # Wait for redirect or error
            time.sleep(8)

            # Check for error
            error_el = page.query_selector('.auth0-lock-error-msg, .auth0-global-message-error')
            if error_el:
                err_text = error_el.inner_text().strip()
                if err_text:
                    print(f"[Auth] Login error: {err_text}")
                    browser.close()
                    return None, 0

            # Check redirect URL
            current_url = page.url
            if "access_token=" in current_url:
                token_data = _parse_token_from_url(current_url)
                if token_data:
                    captured["token"] = token_data["access_token"]
                    captured["expires_at"] = token_data["expires_at"]

            # Wait more if needed
            if not captured["token"]:
                try:
                    page.wait_for_function(
                        "() => window.location.href.includes('access_token=')",
                        timeout=15000
                    )
                    current_url = page.url
                    token_data = _parse_token_from_url(current_url)
                    if token_data:
                        captured["token"] = token_data["access_token"]
                        captured["expires_at"] = token_data["expires_at"]
                except PlaywrightTimeout:
                    pass

        except Exception as e:
            print(f"[Auth] Playwright error: {e}")
        finally:
            browser.close()

    return captured["token"], captured["expires_at"]


def _try_silent_auth():
    """
    Try to get a token using saved storage_state (session cookies).
    Returns (token, expires_at_ms) or (None, 0).
    """
    storage_state_candidates = [
        Path("/home/will/ReactCRM/scrapers/opengov/storage_state.json"),
        Path("/home/will/scrapers/opengov/storage_state.json"),
    ]
    storage_state = next((p for p in storage_state_candidates if p.exists()), None)
    if not storage_state:
        return None, 0

    print("[Auth] Trying silent auth with saved session state...")

    redirect_uri = f"https://{AUTH0_PORTAL}.portal.opengov.com"
    params = {
        "client_id": AUTH0_CLIENT_ID,
        "response_type": "token id_token",
        "redirect_uri": redirect_uri,
        "scope": "openid profile email",
        "audience": AUTH0_AUDIENCE,
        "nonce": "silent_" + str(int(time.time())),
        "prompt": "none"  # silent — only works if session is valid
    }
    auth_url = f"https://{AUTH0_DOMAIN}/authorize?" + urlencode(params)
    captured = {"token": None, "expires_at": 0}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                storage_state=str(storage_state)
            )
            page = context.new_page()

            def on_nav(frame):
                if frame == page.main_frame:
                    url = frame.url
                    if "access_token=" in url:
                        token_data = _parse_token_from_url(url)
                        if token_data:
                            captured["token"] = token_data["access_token"]
                            captured["expires_at"] = token_data["expires_at"]

            page.on("framenavigated", on_nav)
            page.goto(auth_url, timeout=20000)
            time.sleep(5)
        except Exception as e:
            print(f"[Auth] Silent auth error: {e}")
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
    3. Silent auth using saved browser session (if session cookies still valid)
    4. Full login with OPENGOV_EMAIL / OPENGOV_PASSWORD credentials

    Args:
        token_override: If provided, use this token directly (skip auth)

    Returns:
        access_token string

    To get a fresh token manually:
    1. Open https://stpetersburgfl.portal.opengov.com in browser
    2. Log in with your OpenGov account
    3. Open DevTools > Network, find any request to api-east.viewpointcloud.com
    4. Copy the Authorization: Bearer <token> header value
    5. Run: python3 scrape_opengov.py --token <that_token> ...
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

    print("[Auth] Token expired or missing — re-authenticating...")

    # Try silent auth (uses saved browser session cookies)
    token, expires_at_ms = _try_silent_auth()
    if token:
        save_token(token, expires_at_ms)
        print("[Auth] Silent auth successful!")
        return token

    # Try full login with credentials
    credential_sets = []
    if OPENGOV_EMAIL and OPENGOV_PASSWORD:
        credential_sets.append((OPENGOV_EMAIL, OPENGOV_PASSWORD))
    if OPENGOV_EMAIL_ALT and OPENGOV_PASSWORD_ALT:
        credential_sets.append((OPENGOV_EMAIL_ALT, OPENGOV_PASSWORD_ALT))

    for email, password in credential_sets:
        print(f"[Auth] Trying login: {email}")
        token, expires_at_ms = _try_login_playwright(email, password)
        if token:
            save_token(token, expires_at_ms)
            print("[Auth] Authentication successful!")
            return token
        print(f"[Auth] Failed for {email}")

    raise RuntimeError(
        "[Auth] Failed to obtain access token.\n\n"
        "  To fix:\n"
        "  1. Get a fresh token from the browser:\n"
        "     - Open https://stpetersburgfl.portal.opengov.com\n"
        "     - Log in with your OpenGov citizen account\n"
        "     - Open DevTools (F12) > Network tab\n"
        "     - Find any request to api-east.viewpointcloud.com\n"
        "     - Copy the 'Authorization' header value (without 'Bearer ')\n"
        "     - Run: python3 scrape_opengov.py --token <paste_token_here>\n\n"
        "  2. Or reset password for willwalterburns@gmail.com at:\n"
        "     https://accounts.viewpointcloud.com (check Gmail for reset email)\n\n"
        "  3. Or set env vars:\n"
        "     OPENGOV_EMAIL=your@email OPENGOV_PASSWORD=yourpass python3 scrape_opengov.py\n"
        "  4. Or set OPENGOV_TOKEN=<bearer_token> in env"
    )


def _parse_token_from_url(url):
    """Parse access_token and expires_in from URL fragment (#access_token=...&expires_in=...)."""
    try:
        from urllib.parse import urlparse, parse_qs
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


# ── API ──────────────────────────────────────────────────────────────────────

def fetch_records_page(community, token, since_date, page_num):
    """
    Fetch one page of records from the ViewPointCloud REST API.
    Returns (records_list, total_count).
    """
    url = f"{API_BASE}/{community}/records"
    params = {
        "filter[updatedAt][gte]": since_date.isoformat() + "T00:00:00.000Z",
        "page[number]": page_num,
        "page[size]": PAGE_SIZE,
        "sort": "-updatedAt"
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": f"https://{community}.portal.opengov.com",
        "Referer": f"https://{community}.portal.opengov.com/"
    }

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=30)

        if resp.status_code == 401:
            print(f"  [API] 401 Unauthorized for {community} — token may be invalid")
            return [], 0
        if resp.status_code == 404:
            print(f"  [API] 404 for {community} — portal may not exist or be inactive")
            return [], -1  # -1 signals skip
        if resp.status_code == 429:
            print(f"  [API] Rate limited on {community} — waiting 10s")
            time.sleep(10)
            return [], 0
        if not resp.ok:
            print(f"  [API] HTTP {resp.status_code} for {community}: {resp.text[:200]}")
            return [], 0

        data = resp.json()
        records = data.get("data", [])
        total = data.get("meta", {}).get("total", 0)
        return records, total

    except requests.exceptions.Timeout:
        print(f"  [API] Timeout fetching page {page_num} for {community}")
        return [], 0
    except Exception as e:
        print(f"  [API] Error fetching {community} page {page_num}: {e}")
        return [], 0


def scrape_portal(jurisdiction, token, since_date):
    """
    Scrape all records from one portal since the given date.
    Returns list of transformed records.
    """
    community = jurisdiction["id"]
    name = jurisdiction["name"]
    state = jurisdiction["state"]
    county = jurisdiction.get("county", "")

    print(f"\n  Portal: {name}, {state} ({community})")
    print(f"  Fetching records since {since_date}...")

    all_records = []
    page_num = 1
    total = None

    while True:
        raw_records, count = fetch_records_page(community, token, since_date, page_num)

        if count == -1:
            # Portal not found — skip
            break

        if total is None and count:
            total = count
            pages_needed = (total + PAGE_SIZE - 1) // PAGE_SIZE
            print(f"  Total records: {total} (~{pages_needed} pages)")

        if not raw_records:
            if page_num == 1:
                print(f"  No records returned (may require different auth scope)")
            break

        print(f"  Page {page_num}: {len(raw_records)} records")

        for raw in raw_records:
            record = transform_record(raw, jurisdiction)
            if record:
                all_records.append(record)

        # Check if there are more pages
        if len(raw_records) < PAGE_SIZE:
            break

        page_num += 1
        time.sleep(RATE_PAGE)

    print(f"  Done: {len(all_records)} records from {name}")
    return all_records


def transform_record(raw, jurisdiction):
    """Transform a ViewPointCloud API record to hot_leads schema."""
    attrs = raw.get("attributes", {}) if isinstance(raw.get("attributes"), dict) else {}

    # Parse issue_date
    created_at = attrs.get("createdAt") or attrs.get("dateCreated") or ""
    issue_date = None
    if created_at:
        try:
            issue_date = datetime.fromisoformat(created_at.replace("Z", "+00:00")).date()
        except Exception:
            pass

    # Parse address
    address = (
        attrs.get("address") or
        attrs.get("projectAddress") or
        attrs.get("workAddress") or
        ""
    ).strip()

    city = (
        attrs.get("city") or
        attrs.get("projectCity") or
        jurisdiction["name"]
    ).strip()

    state = attrs.get("state") or jurisdiction["state"]
    if len(state) > 2:
        state = jurisdiction["state"]

    permit_number = (
        attrs.get("recordNumber") or
        attrs.get("projectNumber") or
        raw.get("id") or
        ""
    )

    permit_type = (
        attrs.get("recordType") or
        attrs.get("workType") or
        attrs.get("type") or
        ""
    )

    status = (
        attrs.get("status") or
        attrs.get("projectStatus") or
        ""
    )

    description = (
        attrs.get("description") or
        attrs.get("projectDescription") or
        attrs.get("workDescription") or
        ""
    )

    applicant_name = attrs.get("applicantName") or ""
    owner_name = attrs.get("ownerName") or ""
    contractor_name = attrs.get("contractorName") or ""
    contractor_company = attrs.get("contractorCompany") or attrs.get("contractorBusinessName") or ""
    zip_code = attrs.get("zipCode") or attrs.get("zip") or ""
    lat = attrs.get("latitude") or attrs.get("projectLat")
    lng = attrs.get("longitude") or attrs.get("projectLng")
    valuation = attrs.get("valuation") or attrs.get("projectValue")

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
        "valuation": float(valuation) if valuation else None,
        "applicant_name": applicant_name[:200] if applicant_name else None,
        "owner_name": owner_name[:200] if owner_name else None,
        "contractor_name": contractor_name[:200] if contractor_name else None,
        "contractor_company": contractor_company[:200] if contractor_company else None,
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
    print("\n[Pre-flight] Testing token on stpetersburgfl...")
    test_records, test_count = fetch_records_page("stpetersburgfl", token, since_date, 1)
    if test_count == 0 and not test_records:
        print("[Pre-flight] WARNING: Got 0 records from test portal — auth may have failed")
        print("[Pre-flight] Continuing anyway (portal may just have no recent records)...")
    else:
        print(f"[Pre-flight] OK — got {len(test_records)} records (total: {test_count})")

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
