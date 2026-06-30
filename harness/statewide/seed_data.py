"""~25 representative TX jurisdictions for the pilot.

Deliberately mixed into three buckets so the pilot exercises every path the
statewide loop will hit:

  (A) KNOWN-VENDOR / EXISTING-ADAPTER  -> should auto-config + verify fast.
      These map onto adapters already in scripts/: OpenGov (scrape_opengov.py,
      214 portals), MGO Connect (scrape_mgo_ctx.py), ArcGIS (scrape_arcgis_permits.py),
      Socrata (scrape_all_metros_daily.py). For these the agent's job is a
      config row + a run, NOT new code.

  (B) UNKNOWN / WALLED CITIES  -> exercise the deep-recon + browser-capture path.
      Accela, eTRAKiT, Tyler EnerGov, Infor Rhythm/Hansen, CitizenServe, Click2Gov.
      Some are obtainable via a backdoor (open-data, CAD eSearch, captured XHR);
      some are genuinely walled (hard captcha, paid-only). The verifier decides.

  (C) COUNTIES  -> most TX counties issue NO residential building permits
      (Texas Local Gov't Code Ch. 233 limits county building regulation). The
      correct outcome for these is `walled` with the RIGHT reason ("TX counties
      do not issue residential building permits"), NOT a false "built".

vendor is our best a-priori guess; the agent re-classifies by visiting the page.
"""

SEED = [
    # ───────────────────────── (A) known-vendor, existing adapter ──────────────
    {
        "name": "Austin", "jtype": "city", "fips": "4805000",
        "portal_url": "https://data.austintexas.gov/resource/3syk-w9eu.json",
        "vendor": "socrata",
    },
    {
        "name": "Dallas", "jtype": "city", "fips": "4819000",
        "portal_url": "https://www.dallasopendata.com/resource/e7gq-4sah.json",
        "vendor": "socrata",
    },
    {
        "name": "Pearland", "jtype": "city", "fips": "4856348",
        "portal_url": "https://gis.pearlandtx.gov/hosting/rest/services",
        "vendor": "arcgis",
    },
    {
        "name": "Conroe", "jtype": "city", "fips": "4816432",
        "portal_url": "https://conroetx.viewpointcloud.com/",
        "vendor": "opengov",
    },
    {
        "name": "Mansfield", "jtype": "city", "fips": "4846452",
        "portal_url": "https://mansfieldtx.viewpointcloud.com/",
        "vendor": "opengov",
    },
    {
        "name": "Bedford", "jtype": "city", "fips": "4807132",
        "portal_url": "https://bedfordtx.viewpointcloud.com/",
        "vendor": "opengov",
    },
    {
        "name": "Cedar Park", "jtype": "city", "fips": "4813552",
        "portal_url": "https://www.mygovonline.com/",
        "vendor": "mgo",
    },
    {
        "name": "Georgetown", "jtype": "city", "fips": "4829336",
        "portal_url": "https://www.mygovonline.com/",
        "vendor": "mgo",
    },
    {
        "name": "Dripping Springs", "jtype": "city", "fips": "4821310",
        "portal_url": "https://www.mygovonline.com/",
        "vendor": "mgo",
    },
    {
        "name": "Bastrop", "jtype": "city", "fips": "4806128",
        "portal_url": "https://www.mygovonline.com/",
        "vendor": "mgo",
    },
    {
        "name": "Corpus Christi", "jtype": "city", "fips": "4817000",
        "portal_url": "https://corpuschristitx.viewpointcloud.com/",
        "vendor": "opengov",
    },

    # ───────────────────────── (B) unknown / walled cities ─────────────────────
    {
        "name": "Houston", "jtype": "city", "fips": "4835000",
        "portal_url": "https://www.houstonpermittingcenter.org/",
        "vendor": "unknown",  # city-built portal; open-data backdoor exists
    },
    {
        "name": "San Antonio", "jtype": "city", "fips": "4865000",
        "portal_url": "https://aca-prod.accela.com/COSA/",
        "vendor": "accela",
    },
    {
        "name": "Fort Worth", "jtype": "city", "fips": "4827000",
        "portal_url": "https://aca-prod.accela.com/cfw/",
        "vendor": "accela",
    },
    {
        "name": "Plano", "jtype": "city", "fips": "4858016",
        "portal_url": "https://etrakit.plano.gov/etrakit/",
        "vendor": "etrakit",
    },
    {
        "name": "Frisco", "jtype": "city", "fips": "4827684",
        "portal_url": "https://etrakit.friscotexas.gov/",
        "vendor": "etrakit",
    },
    {
        "name": "McKinney", "jtype": "city", "fips": "4845744",
        "portal_url": "https://energov.mckinneytexas.org/EnerGovProd/SelfService",
        "vendor": "energov",
    },
    {
        "name": "Denton", "jtype": "city", "fips": "4819972",
        "portal_url": "https://tylereagle.cityofdenton.com/energov_prod/selfservice",
        "vendor": "energov",
    },
    {
        "name": "Sugar Land", "jtype": "city", "fips": "4870808",
        "portal_url": "https://www.sugarlandtx.gov/389/Permits",
        "vendor": "citizenserve",
    },
    {
        "name": "League City", "jtype": "city", "fips": "4842048",
        "portal_url": "https://www.leaguecitytx.gov/179/Building-Permits",
        "vendor": "click2gov",
    },
    {
        "name": "New Braunfels", "jtype": "city", "fips": "4851180",
        "portal_url": "https://newbraunfels.gov/permits",
        "vendor": "unknown",  # Infor/Rhythm suspected; exercise deep recon
    },
    {
        "name": "Round Rock", "jtype": "city", "fips": "4863500",
        "portal_url": "https://permits.roundrocktexas.gov/",
        "vendor": "unknown",
    },

    # ───────────────────────── (C) counties (Ch. 233 / no permits) ─────────────
    {
        "name": "Harris County", "jtype": "county", "fips": "48201",
        "portal_url": "https://www.eng.hctx.net/permits",
        "vendor": "county",  # unincorporated: floodplain/septic only, no bldg permits
    },
    {
        "name": "Travis County", "jtype": "county", "fips": "48453",
        "portal_url": "https://www.traviscountytx.gov/tnr/development-permits",
        "vendor": "county",
    },
    {
        "name": "Comal County", "jtype": "county", "fips": "48091",
        "portal_url": "https://www.co.comal.tx.us/Engineers_Office.htm",
        "vendor": "county",
    },
]
