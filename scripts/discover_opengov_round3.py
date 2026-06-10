#!/usr/bin/env python3
"""
OpenGov Round 3 - Exhaustive MA/CT/RI/NH/VT/ME town brute force
plus systematic state-suffix probing for all US cities/towns.
"""

import asyncio
import aiohttp
import json
import re

with open("/home/will/permit-api/scripts/opengov_portals.json") as f:
    existing = json.load(f)
KNOWN = set(p["slug"] for p in existing)
print(f"Already found: {len(KNOWN)} portals")


# Full list of Massachusetts cities and towns (all 351), formatted as lowercase slug + "ma"
MA_TOWNS = [
    "abington", "acton", "acushnet", "agawam", "alford", "amesbury", "amherst",
    "andover", "aquinnah", "arlington", "ashburnham", "ashby", "ashfield", "ashland",
    "athol", "attleboro", "auburn", "avon", "ayer", "barnstable", "barre", "bedford",
    "belchertown", "belmont", "berkley", "berlin", "bernardston", "beverly", "billerica",
    "blackstone", "blandford", "bolton", "bourne", "boxborough", "boxford", "boylston",
    "braintree", "brewster", "bridgewater", "brimfield", "brockton", "brookfield",
    "brookline", "buckland", "burlington", "cambridge", "canton", "carlisle", "carver",
    "charlemont", "charlton", "chatham", "chelmsford", "chelsea", "cheshire", "chester",
    "chesterfield", "chicopee", "chilmark", "clarksburg", "clinton", "cohasset", "colrain",
    "concord", "conway", "cummington", "dalton", "danvers", "dartmouth", "dedham",
    "deerfield", "dennis", "dighton", "douglas", "dover", "dracut", "dudley", "dunstable",
    "duxbury", "eastbridgewater", "eastham", "easthampton", "easton", "edgartown", "erving",
    "essex", "everett", "fairhaven", "falmouth", "fitchburg", "florida", "foxborough",
    "framingham", "franklin", "freetown", "gardner", "gay head", "gayhead", "georgetown",
    "gill", "gloucester", "goshen", "gosnold", "grafton", "granby", "granville",
    "greatbarrington", "greenfield", "groton", "groveland", "hadley", "halifax",
    "hamilton", "hampden", "hancock", "hanover", "hanson", "hardwick", "harvard",
    "harwich", "hatfield", "haverhill", "hawley", "heath", "hingham", "hinsdale",
    "holbrook", "holden", "holland", "holliston", "holyoke", "hopedale", "hopkinton",
    "hubbardston", "hudson", "hull", "huntington", "ipswich", "kingston", "lakeville",
    "lancaster", "lanesborough", "lawrence", "lee", "leicester", "lenox", "leominster",
    "leverett", "lexington", "leyden", "lincoln", "littleton", "longmeadow", "lowell",
    "ludlow", "lunenburg", "lynn", "lynnfield", "malden", "manchester", "mansfield",
    "marblehead", "marion", "marlborough", "marshfield", "mashpee", "mattapoisett",
    "maynard", "medfield", "medford", "medway", "melrose", "mendon", "merrimac",
    "methuen", "middleborough", "middlefield", "milford", "millbury", "millville",
    "millis", "milton", "monroe", "monson", "montague", "monterey", "montgomery",
    "mount washington", "mountwashington",
    "nahant", "nantucket", "natick", "needham", "newbury", "newburyport", "newmarlborough",
    "newton", "norfolk", "northadams", "northandover", "northattleborough",
    "northborough", "northbridge", "northbroookfield", "northfield", "norton",
    "norwell", "norwood", "oakbluffs", "oakham", "orange", "orleans", "otis", "oxford",
    "palmer", "paxton", "peabody", "pelham", "pembroke", "pepperell", "peru",
    "petersham", "phillipston", "pittsfield", "plainfield", "plainville", "plymouth",
    "plympton", "princeton", "provincetown", "quincy", "randolph", "raynham", "reading",
    "rehoboth", "revere", "richmond", "rochester", "rockland", "rockport", "rowe",
    "rowley", "royalston", "russell", "rutland", "salem", "salisbury", "sandisfield",
    "sandwich", "saugus", "savoy", "scituate", "seekonk", "sharon", "sheffield",
    "shelburne", "sherborn", "shirley", "shrewsbury", "shutesbury", "somerset",
    "somerville", "southampton", "southborough", "southbridge", "southhadley",
    "southwick", "spencer", "springfield", "sterling", "stockbridge", "stoneham",
    "stoughton", "stow", "sturbridge", "sudbury", "sunderland", "sutton", "swampscott",
    "swansea", "taunton", "templeton", "tewksbury", "tisbury", "tolland", "topsfield",
    "townsend", "truro", "tyngsborough", "tyringham", "upton", "uxbridge", "wakefield",
    "wales", "walpole", "waltham", "ware", "wareham", "warren", "warwick", "washington",
    "watertown", "wayland", "webster", "wellesley", "wellfleet", "wendell", "wenham",
    "westborough", "westboylston", "westbridgewater", "westfield", "westford",
    "westhampton", "weston", "westport", "westspringfield", "westtisbury",
    "weymouth", "whately", "whitman", "wilbraham", "williamsburg", "williamstown",
    "wilmington", "winchendon", "windsor", "winthrop", "woburn", "worcester",
    "worthington", "wrentham", "yarmouth",
]

# Rhode Island - all 39 cities/towns
RI_TOWNS = [
    "barrington", "bristolri", "burrillville", "central falls", "centralfalls",
    "charlestown", "coventry", "cranston", "cumberland", "east greenwich", "eastgreenwich",
    "east providence", "eastprovidence", "exeter", "foster", "glocester", "hopkinton",
    "jamestown", "johnston", "lincoln", "little compton", "littlecompton",
    "middletown", "narragansett", "new shoreham", "newshoreham", "newport",
    "north kingstown", "northkingstown", "north providence", "northprovidence",
    "north smithfield", "northsmithfield", "pawtucket", "portsmouth", "providence",
    "richmond", "scituate", "smithfield", "south kingstown", "southkingstown",
    "tiverton", "warren", "warwick", "west greenwich", "westgreenwich",
    "west warwick", "westwarwick", "westerly", "woonsocket",
]

# Connecticut - more towns
CT_TOWNS = [
    "andover", "ansonia", "ashford", "avon", "barkhamsted", "beacon falls", "beaconfalls",
    "berlin", "bethany", "bethel", "bethlehem", "bloomfield", "bolton",
    "bozrah", "branford", "bridgeport", "bridgewater", "bristol", "brookfield",
    "brooklyn", "burlington", "canaan", "canton", "chaplin", "cheshire",
    "chester", "clinton", "colchester", "colebrook", "columbia", "cornwall",
    "coventry", "cromwell", "darien", "deep river", "deepriver",
    "derby", "durham", "east granby", "eastgranby",
    "east haddam", "easthaddam", "east hampton", "easthampton",
    "east hartford", "easthartford", "east haven", "easthaven",
    "east lyme", "eastlyme", "east windsor", "eastwindsor",
    "eastford", "ellington", "enfield", "essex", "fairfield",
    "farmington", "franklin", "glastonbury", "goshen", "granby",
    "greenwich", "griswold", "groton", "guilford", "haddam",
    "hamden", "hampton", "hartford", "harwinton", "hebron",
    "kent", "killingly", "killingworth", "lebanon", "ledyard",
    "lisbon", "litchfield", "lyme", "madison", "manchester",
    "mansfield", "marlborough", "meriden", "middlebury", "middlefield",
    "middletown", "milford", "monroe", "montville", "morris",
    "naugatuck", "new britain", "newbritain", "new canaan", "newcanaan",
    "new fairfield", "newfairfield", "new hartford", "newhartford",
    "new haven", "newhaven", "new london", "newlondon",
    "new milford", "newmilford", "newington", "newtown",
    "norfolk", "north branford", "northbranford", "north canaan", "northcanaan",
    "north haven", "northhaven", "north stonington", "northstonington",
    "norwalk", "norwich", "old lyme", "oldlyme",
    "old saybrook", "oldsaybrook", "orange", "oxford",
    "plainfield", "plainville", "plymouth", "pomfret",
    "portland", "preston", "prospect", "putnam",
    "redding", "ridgefield", "rocky hill", "rockyhillct",
    "roxbury", "salem", "salisbury", "scotland",
    "seymour", "sharon", "shelton", "sherman",
    "simsbury", "somers", "south windsor", "southwindsor",
    "southbury", "southington", "sprague", "stafford",
    "stamford", "sterling", "stonington", "stratford",
    "suffield", "thomaston", "thompson", "tolland",
    "torrington", "trumbull", "union", "vernon",
    "voluntown", "wallingford", "waterbury", "waterford",
    "watertown", "west hartford", "westhartford",
    "west haven", "westhaven", "westbrook", "weston",
    "westport", "wethersfield", "wilton", "windham",
    "windsor", "windsor locks", "windsorlocks",
    "wolcott", "woodbridge", "woodbury", "woodstock",
]

# New Hampshire
NH_TOWNS = [
    "alton", "amherst", "atkinson", "auburn", "barrington", "bedford", "belmont",
    "boscawen", "bow", "brentwood", "brookline", "cambridge", "candia", "canterbury",
    "chichester", "claremont", "concord", "conway", "danville", "derry", "dover",
    "derry", "durham", "east kingston", "eastkingston", "epping", "epsom",
    "exeter", "farmington", "franconia", "fremont", "gilford",
    "goffstown", "gorham", "greenland", "hampton", "hampstead",
    "hanover", "henniker", "hillsborough", "hooksett", "hollis",
    "hopkinton", "hudson", "jaffrey", "keene", "kensington",
    "kingston", "laconia", "lancaster", "litchfield", "littleton",
    "londonderry", "loudon", "manchester", "marlborough", "merrimack",
    "milford", "moultonborough", "nashua", "newbury", "newfields",
    "newington", "newmarket", "newport", "newton", "northhampton",
    "northfield", "northwood", "nottingham", "ossipee", "pelham",
    "pembroke", "peterborough", "pittsfield", "plaistow", "plymouth",
    "portsmouth", "raymond", "rindge", "rochester", "rollinsford",
    "rye", "salem", "sandown", "sandwich", "seabrook",
    "somersworth", "southhampton", "stratham", "tilton", "weare",
    "windham", "wolfeboro",
]

# Vermont
VT_TOWNS = [
    "barre", "bellows falls", "bennington", "berlin", "brattleboro", "bristol",
    "burlington", "castleton", "colchester", "essex", "essex junction", "essexjunction",
    "fair haven", "fairhaven", "georgia", "hardwick", "hartford", "hinesburg",
    "hyde park", "hydepark", "island pond", "islandpond", "johnson",
    "lyndon", "middlebury", "milton", "montpelier", "morristown",
    "morrisville", "newport", "northfield", "norwich",
    "pittsford", "poultney", "proctor", "richford", "richmond",
    "rockingham", "royalton", "rutland", "saint albans", "stalbans",
    "saint johnsbury", "stjohnsbury", "shelburne", "south burlington", "southburlington",
    "springfield", "stowe", "swanton", "vergennes",
    "waitsfield", "waterbury", "williston", "winooski", "woodstock",
]

# Maine towns
ME_TOWNS = [
    "auburn", "augusta", "bangor", "bar harbor", "barharbor", "bath",
    "belfast", "berwick", "biddeford", "brewer", "bridgton", "brunswick",
    "bucksport", "buxton", "calais", "camden", "capeelizabeth", "caribou",
    "cumberland", "ellsworth", "eliot", "fairfield", "falmouth", "farmington",
    "freeport", "gardiner", "gorham", "gray", "houlton", "kennebunk",
    "kennebunkport", "kittery", "lewiston", "lincoln", "lisbon", "lyman",
    "millinocket", "naples", "northberwick", "norway", "oldorchardbeach",
    "oldtown", "orono", "paris", "pittsfield", "portland",
    "presqueisle", "raymond", "rockland", "rockport", "rumford",
    "saco", "sanford", "scarborough", "skowhegan", "southberwick",
    "southportland", "standish", "topsham", "waterboro", "waterville",
    "wells", "westbrook", "windham", "winslow", "winthrop", "yarmouth", "york",
]

# Additional states that showed promise
MORE_STATES = [
    # CO more
    ("arvada", "co"), ("aurora", "co"), ("boulder", "co"), ("broomfield", "co"),
    ("castlerock", "co"), ("centennial", "co"), ("coloradosprings", "co"),
    ("commercecity", "co"), ("denver", "co"), ("englewood", "co"),
    ("fortcollins", "co"), ("greeley", "co"), ("lakewood", "co"),
    ("longmont", "co"), ("loveland", "co"), ("parker", "co"),
    ("thornton", "co"), ("westminster", "co"), ("wheatridge", "co"),
    ("steamboatsprings", "co"), ("aspen", "co"), ("telluride", "co"),
    ("vail", "co"), ("glenwood springs", "co"), ("glenwood", "co"),
    ("glenwoodsprings", "co"), ("grandjunction", "co"), ("montrose", "co"),
    ("durango", "co"), ("alamosa", "co"), ("lamar", "co"),
    ("canon city", "co"), ("canoncity", "co"), ("pueblo", "co"),
    ("trinidad", "co"), ("sterling", "co"), ("fort morgan", "co"),
    ("fortmorgan", "co"), ("craig", "co"), ("gunnison", "co"),
    ("salida", "co"), ("buena vista", "co"), ("buenavista", "co"),
    ("breckenridge", "co"), ("frisco", "co"), ("dillon", "co"),
    ("silverthorne", "co"), ("keystone", "co"), ("copper mountain", "co"),
    ("leadville", "co"),

    # OH more towns
    ("barberton", "oh"), ("bay village", "oh"), ("bayvillage", "oh"),
    ("beachwood", "oh"), ("bedford", "oh"), ("berea", "oh"),
    ("brecksville", "oh"), ("broadview heights", "oh"), ("broadviewheights", "oh"),
    ("brook park", "oh"), ("brookpark", "oh"), ("brunswick", "oh"),
    ("canal winchester", "oh"), ("canalwinchester", "oh"),
    ("centerville", "oh"), ("chardon", "oh"), ("chagrin falls", "oh"),
    ("chagrinfalls", "oh"), ("chesterland", "oh"), ("chillicothe", "oh"),
    ("circleville", "oh"), ("coshocton", "oh"), ("defiance", "oh"),
    ("delaware", "oh"), ("east cleveland", "oh"), ("eastcleveland", "oh"),
    ("fairborn", "oh"), ("findlay", "oh"), ("fremont", "oh"),
    ("gallipolis", "oh"), ("garfield heights", "oh"), ("garfieldheights", "oh"),
    ("girard", "oh"), ("heath", "oh"), ("hillliard", "oh"),
    ("hilliard", "oh"), ("hubbard", "oh"), ("huron", "oh"),
    ("ironton", "oh"), ("kent", "oh"), ("kirtland", "oh"),
    ("leroy", "oh"), ("lebanon", "oh"), ("licking county", "oh"),
    ("lima", "oh"), ("lorain", "oh"), ("loudonville", "oh"),
    ("lyndhurst", "oh"), ("macedonia", "oh"), ("marietta", "oh"),
    ("marysvilleoh", "oh"), ("marysville", "oh"), ("massillon", "oh"),
    ("maumee", "oh"), ("mayfield heights", "oh"), ("mayfieldheights", "oh"),
    ("medina", "oh"), ("mentor on the lake", "oh"), ("miamisburg", "oh"),
    ("middleburg heights", "oh"), ("middleburgheights", "oh"),
    ("milford", "oh"), ("moraine", "oh"), ("mount healthy", "oh"),
    ("mount sterling", "oh"), ("munroe falls", "oh"),
    ("new philadelphia", "oh"), ("newphiladelphia", "oh"),
    ("niles", "oh"), ("north royalton", "oh"), ("northroyalton", "oh"),
    ("northfield", "oh"), ("norton", "oh"), ("oberlin", "oh"),
    ("olmsted falls", "oh"), ("olmstedfalls", "oh"), ("oregon", "oh"),
    ("oxford", "oh"), ("painesville", "oh"), ("piqua", "oh"),
    ("poland", "oh"), ("portage", "oh"), ("powell", "oh"),
    ("reading", "oh"), ("reynoldsburg", "oh"), ("richmond heights", "oh"),
    ("richmonheights", "oh"), ("rochester", "oh"), ("rocky river", "oh"),
    ("rockyriver", "oh"), ("rootstown", "oh"), ("rossford", "oh"),
    ("saint clairsville", "oh"), ("saintclairsville", "oh"),
    ("sharonville", "oh"), ("sheffield lake", "oh"),
    ("sheffieldlake", "oh"), ("shelby", "oh"), ("sidney", "oh"),
    ("solon", "oh"), ("south euclid", "oh"), ("southeuclid", "oh"),
    ("southwest licking", "oh"), ("sublimity", "oh"),
    ("sugar grove", "oh"), ("sylvania", "oh"),
    ("tiffin", "oh"), ("tipp city", "oh"), ("tippcity", "oh"),
    ("toledo", "oh"), ("troy", "oh"), ("twinsburg", "oh"),
    ("university heights", "oh"), ("universityheights", "oh"),
    ("upper arlington", "oh"), ("upperarlington", "oh"),
    ("valley view", "oh"), ("valleyview", "oh"),
    ("vandalia", "oh"), ("vermilion", "oh"), ("wadsworth", "oh"),
    ("wapakoneta", "oh"), ("westerville", "oh"), ("wickliffe", "oh"),
    ("willoughby", "oh"), ("willoughby hills", "oh"),
    ("willoughbyhills", "oh"), ("willowick", "oh"),
    ("wooster", "oh"), ("xenia", "oh"),

    # WI more
    ("de pere", "wi"), ("depere", "wi"), ("green bay", "wi"),
    ("eau claire", "wi"), ("eauclaire", "wi"), ("fond du lac", "wi"),
    ("fonddulac", "wi"), ("germantown", "wi"), ("hartland", "wi"),
    ("marshfield", "wi"), ("menomonee falls", "wi"), ("menomoneefalls", "wi"),
    ("middleton", "wi"), ("muskego", "wi"), ("new berlin", "wi"),
    ("newberlin", "wi"), ("new richmond", "wi"), ("newrichmond", "wi"),
    ("oconomowoc", "wi"), ("pewaukee", "wi"), ("plover", "wi"),
    ("port washington", "wi"), ("portwashington", "wi"), ("portage", "wi"),
    ("river falls", "wi"), ("riverfalls", "wi"), ("rothschild", "wi"),
    ("schofield", "wi"), ("shawano", "wi"), ("shorewood", "wi"),
    ("south milwaukee", "wi"), ("southmilwaukee", "wi"),
    ("sparta", "wi"), ("stoughton", "wi"), ("sun prairie", "wi"),
    ("sunprairie", "wi"), ("sussex", "wi"), ("verona", "wi"),
    ("waterford", "wi"), ("waterloo", "wi"), ("waukesha", "wi"),
    ("waupaca", "wi"), ("wausau", "wi"), ("west allis", "wi"),
    ("westallis", "wi"), ("west bend", "wi"), ("westbend", "wi"),
    ("whitefish bay", "wi"), ("whitefishbay", "wi"), ("whitewater", "wi"),
    ("wisconsin rapids", "wi"), ("wiscRapids", "wi"),

    # MN more
    ("albert lea", "mn"), ("albertlea", "mn"), ("baxter", "mn"),
    ("brooklyn center", "mn"), ("brooklyncenter", "mn"),
    ("chanhassen", "mn"), ("chaska", "mn"), ("columbia heights", "mn"),
    ("columbiaheights", "mn"), ("crystal", "mn"), ("elk river", "mn"),
    ("elkriver", "mn"), ("faribault", "mn"), ("fridley", "mn"),
    ("golden valley", "mn"), ("goldenvalley", "mn"), ("hastings", "mn"),
    ("hibbing", "mn"), ("inver grove heights", "mn"), ("invergGroveheights", "mn"),
    ("invergrovheights", "mn"), ("inversegroveheights", "mn"),
    ("isanti", "mn"), ("little falls", "mn"), ("littlefalls", "mn"),
    ("maplewood", "mn"), ("marshall", "mn"), ("minnetonka", "mn"),
    ("monticello", "mn"), ("mound", "mn"), ("new brighton", "mn"),
    ("newbrighton", "mn"), ("new ulm", "mn"), ("newulm", "mn"),
    ("north branch", "mn"), ("northbranch", "mn"), ("north manakto", "mn"),
    ("northmankato", "mn"), ("orono", "mn"), ("owatonna", "mn"),
    ("prior lake", "mn"), ("priorlake", "mn"), ("ramsey", "mn"),
    ("red wing", "mn"), ("redwing", "mn"), ("robbinsdale", "mn"),
    ("saint michael", "mn"), ("saintmichael", "mn"), ("saint paul", "mn"),
    ("saintpaul", "mn"), ("savage", "mn"), ("shakopee", "mn"),
    ("shoreview", "mn"), ("south st paul", "mn"), ("southstpaul", "mn"),
    ("stillwater", "mn"), ("vadnais heights", "mn"), ("vadnaisheights", "mn"),
    ("west st paul", "mn"), ("weststpaul", "mn"),
    ("white bear lake", "mn"), ("whitebearlake", "mn"),
    ("winona", "mn"), ("worthington", "mn"),

    # IL more
    ("addison", "il"), ("arlington heights", "il"), ("arlingtonheights", "il"),
    ("barrington", "il"), ("batavia", "il"), ("carol stream", "il"),
    ("carolstream", "il"), ("champaign", "il"), ("chicago heights", "il"),
    ("chicagoheights", "il"), ("danville", "il"), ("downers grove", "il"),
    ("downersgrove", "il"), ("effingham", "il"), ("elgin", "il"),
    ("elk grove village", "il"), ("elkgrovevillage", "il"),
    ("elmhurst", "il"), ("evanston", "il"), ("florissant", "il"),
    ("frankfort", "il"), ("freeport", "il"), ("galesburg", "il"),
    ("geneva", "il"), ("glenview", "il"), ("granite city", "il"),
    ("granitecity", "il"), ("gurnee", "il"), ("hanover park", "il"),
    ("hanoverpark", "il"), ("harvey", "il"), ("hoffman estates", "il"),
    ("hoffmanestates", "il"), ("joliet", "il"), ("kankakee", "il"),
    ("lake in the hills", "il"), ("lakeinthehills", "il"),
    ("lansing", "il"), ("lockport", "il"), ("lombard", "il"),
    ("maywood", "il"), ("moline", "il"), ("mount prospect", "il"),
    ("mountprospect", "il"), ("mundelein", "il"), ("naperville", "il"),
    ("normal", "il"), ("north chicago", "il"), ("northchicago", "il"),
    ("oak creek", "il"), ("oak forest", "il"), ("oak lawn", "il"),
    ("oaklawn", "il"), ("oak park", "il"), ("oakpark", "il"),
    ("orland park", "il"), ("orlandpark", "il"), ("oswego", "il"),
    ("palatine", "il"), ("park ridge", "il"), ("parkridge", "il"),
    ("pekin", "il"), ("peoria", "il"), ("rockford", "il"),
    ("rolling meadows", "il"), ("rollingmeadows", "il"),
    ("round lake", "il"), ("roundlake", "il"),
    ("round lake beach", "il"), ("roundlakebeach", "il"),
    ("schaumburg", "il"), ("skokie", "il"), ("springfield", "il"),
    ("st charles", "il"), ("stcharles", "il"), ("streamwood", "il"),
    ("tinley park", "il"), ("tinleypark", "il"), ("urbana", "il"),
    ("villa park", "il"), ("villapark", "il"), ("waukegan", "il"),
    ("wheaton", "il"), ("wheeling", "il"), ("waukegan", "il"),
    ("woodridge", "il"), ("worth", "il"), ("york", "il"),
    ("zion", "il"),

    # IN more
    ("anderson", "in"), ("avon", "in"), ("bloomington", "in"),
    ("brownsburg", "in"), ("carmel", "in"), ("columbus", "in"),
    ("crawfordsville", "in"), ("crown point", "in"), ("crownpoint", "in"),
    ("elkhart", "in"), ("evansville", "in"), ("fishers", "in"),
    ("fort wayne", "in"), ("fortwayne", "in"), ("franklin", "in"),
    ("gary", "in"), ("greenfield", "in"), ("greenwood", "in"),
    ("hammond", "in"), ("highland", "in"), ("hobart", "in"),
    ("huntingburg", "in"), ("indianapolis", "in"), ("jasper", "in"),
    ("jeffersonville", "in"), ("kokomo", "in"), ("lafayette", "in"),
    ("la porte", "in"), ("laporte", "in"), ("logansport", "in"),
    ("merrillville", "in"), ("michigan city", "in"), ("michigancity", "in"),
    ("mishawaka", "in"), ("muncie", "in"), ("munster", "in"),
    ("new albany", "in"), ("newalbany", "in"), ("new castle", "in"),
    ("newcastle", "in"), ("noblesville", "in"), ("north vernon", "in"),
    ("peru", "in"), ("plainfield", "in"), ("portage", "in"),
    ("richmond", "in"), ("schererville", "in"), ("seymour", "in"),
    ("shelbyville", "in"), ("south bend", "in"), ("southbend", "in"),
    ("terre haute", "in"), ("terrehaute", "in"), ("valparaiso", "in"),
    ("vincennes", "in"), ("wabash", "in"), ("warsaw", "in"),
    ("washington", "in"), ("westfield", "in"), ("zionsville", "in"),

    # TN more
    ("brentwood", "tn"), ("bristol", "tn"), ("chattanooga", "tn"),
    ("clarksville", "tn"), ("collierville", "tn"), ("columbia", "tn"),
    ("cookeville", "tn"), ("dickson", "tn"), ("dyersburg", "tn"),
    ("elizabethton", "tn"), ("franklin", "tn"), ("gallatin", "tn"),
    ("germantown", "tn"), ("goodlettsville", "tn"), ("greeneville", "tn"),
    ("hendersonville", "tn"), ("jackson", "tn"),
    ("johnson city", "tn"), ("johnsoncity", "tn"),
    ("kingsport", "tn"), ("knoxville", "tn"), ("lebanon", "tn"),
    ("memphis", "tn"), ("millington", "tn"), ("morristown", "tn"),
    ("mt juliet", "tn"), ("mtjuliet", "tn"),
    ("murfreesboro", "tn"), ("nashville", "tn"), ("nolensville", "tn"),
    ("oak ridge", "tn"), ("oakridge", "tn"), ("paris", "tn"),
    ("sevierville", "tn"), ("smyrna", "tn"), ("spring hill", "tn"),
    ("springhill", "tn"), ("spring", "tn"),

    # NC more cities/towns
    ("apex", "nc"), ("asheboro", "nc"), ("belmont", "nc"),
    ("black mountain", "nc"), ("blackmountain", "nc"), ("brevard", "nc"),
    ("burlington", "nc"), ("bunn", "nc"), ("cary", "nc"),
    ("chapel hill", "nc"), ("chapelhill", "nc"), ("charlotte", "nc"),
    ("cherryville", "nc"), ("china grove", "nc"), ("clayton", "nc"),
    ("clemmons", "nc"), ("concord", "nc"), ("conover", "nc"),
    ("cornelius", "nc"), ("davidson", "nc"), ("dunn", "nc"),
    ("durham", "nc"), ("eden", "nc"), ("elizabeth city", "nc"),
    ("elizabethcity", "nc"), ("elkin", "nc"), ("fayetteville", "nc"),
    ("garner", "nc"), ("gastonia", "nc"), ("goldsboro", "nc"),
    ("greensboro", "nc"), ("greenville", "nc"), ("harrisburg", "nc"),
    ("haw river", "nc"), ("henderson", "nc"), ("hendersonville", "nc"),
    ("hickory", "nc"), ("high point", "nc"), ("highpoint", "nc"),
    ("holly springs", "nc"), ("hollysprings", "nc"), ("huntersville", "nc"),
    ("jacksonville", "nc"), ("kannapolis", "nc"), ("kernersville", "nc"),
    ("king", "nc"), ("kings mountain", "nc"), ("kinston", "nc"),
    ("lenoir", "nc"), ("lexington", "nc"), ("lincolnton", "nc"),
    ("lumberton", "nc"), ("matthews", "nc"), ("mint hill", "nc"),
    ("minthill", "nc"), ("mooresville", "nc"), ("morehead city", "nc"),
    ("moreheadcity", "nc"), ("morganton", "nc"), ("morrisville", "nc"),
    ("mount airy", "nc"), ("mountairy", "nc"), ("mount holly", "nc"),
    ("mountholly", "nc"), ("murfreesboro", "nc"),
    ("new bern", "nc"), ("newbern", "nc"), ("newton", "nc"),
    ("oxford", "nc"), ("pinehurst", "nc"), ("raleigh", "nc"),
    ("red springs", "nc"), ("reidsville", "nc"), ("rocky mount", "nc"),
    ("rockymount", "nc"), ("roxboro", "nc"), ("salisbury", "nc"),
    ("sanford", "nc"), ("shelby", "nc"), ("smithfield", "nc"),
    ("southern pines", "nc"), ("southernpines", "nc"),
    ("southern shores", "nc"), ("southernshores", "nc"),
    ("st pauls", "nc"), ("statesville", "nc"), ("swansboro", "nc"),
    ("tarboro", "nc"), ("thomasville", "nc"), ("wake forest", "nc"),
    ("wakeforest", "nc"), ("washington", "nc"), ("whiteville", "nc"),
    ("wilkesboro", "nc"), ("williamston", "nc"), ("wilmington", "nc"),
    ("wilson", "nc"), ("winston-salem", "nc"), ("winstonsalem", "nc"),
    ("yadkinville", "nc"), ("zebulon", "nc"),

    # VA more
    ("blacksburg", "va"), ("bristol", "va"), ("buena vista", "va"),
    ("charlottesville", "va"), ("chesapeake", "va"), ("colonial heights", "va"),
    ("colonialheights", "va"), ("covington", "va"), ("culpeper", "va"),
    ("danville", "va"), ("emporia", "va"), ("fairfax", "va"),
    ("falls church", "va"), ("fallschurch", "va"), ("fredericksburg", "va"),
    ("front royal", "va"), ("frontroyal", "va"), ("galax", "va"),
    ("gloucester", "va"), ("harrisonburg", "va"), ("herndon", "va"),
    ("hopewell", "va"), ("leesburg", "va"), ("lexington", "va"),
    ("lynchburg", "va"), ("manassas", "va"), ("manassas park", "va"),
    ("manassaspark", "va"), ("martinsville", "va"), ("mclean", "va"),
    ("mechanicsville", "va"), ("newport news", "va"), ("newportnews", "va"),
    ("norfolk", "va"), ("norton", "va"), ("petersburg", "va"),
    ("portsmouth", "va"), ("poquoson", "va"), ("radford", "va"),
    ("richmond", "va"), ("roanoke", "va"), ("salem", "va"),
    ("staunton", "va"), ("suffolk", "va"), ("vienna", "va"),
    ("virginia beach", "va"), ("virginiabeach", "va"),
    ("waynesboro", "va"), ("williamsburg", "va"), ("winchester", "va"),

    # SC more
    ("aiken", "sc"), ("anderson", "sc"), ("beaufort", "sc"),
    ("bluffton", "sc"), ("camden", "sc"), ("cayce", "sc"),
    ("charleston", "sc"), ("clemson", "sc"), ("columbia", "sc"),
    ("conway", "sc"), ("easley", "sc"), ("florence", "sc"),
    ("forest acres", "sc"), ("forestacres", "sc"), ("fort mill", "sc"),
    ("fortmill", "sc"), ("gaffney", "sc"), ("goose creek", "sc"),
    ("goosecreek", "sc"), ("greenville", "sc"), ("greenwood", "sc"),
    ("greer", "sc"), ("hanahan", "sc"), ("hartsville", "sc"),
    ("hilton head", "sc"), ("hiltonhead", "sc"), ("irmo", "sc"),
    ("isle of palms", "sc"), ("isleofpalms", "sc"), ("ladson", "sc"),
    ("lake city", "sc"), ("lancastersc", "sc"), ("lexington", "sc"),
    ("lyman", "sc"), ("manning", "sc"), ("mount pleasant", "sc"),
    ("mountpleasant", "sc"), ("myrtle beach", "sc"), ("myrtlebeach", "sc"),
    ("newberry", "sc"), ("north charleston", "sc"), ("northcharleston", "sc"),
    ("north myrtle beach", "sc"), ("northmyrtlebeach", "sc"),
    ("orangeburg", "sc"), ("rock hill", "sc"), ("rockhill", "sc"),
    ("simpsonville", "sc"), ("spartanburg", "sc"), ("summerville", "sc"),
    ("sumter", "sc"), ("union", "sc"), ("west columbia", "sc"),
    ("westcolumbia", "sc"), ("york", "sc"),

    # GA more
    ("acworth", "ga"), ("albany", "ga"), ("alpharetta", "ga"),
    ("americus", "ga"), ("athens", "ga"), ("atlanta", "ga"),
    ("auburn", "ga"), ("augusta", "ga"), ("austell", "ga"),
    ("bainbridge", "ga"), ("ball ground", "ga"), ("ballground", "ga"),
    ("braselton", "ga"), ("brookhaven", "ga"), ("brunswick", "ga"),
    ("buford", "ga"), ("cairo", "ga"), ("canton", "ga"),
    ("carrollton", "ga"), ("cartersville", "ga"), ("chamblee", "ga"),
    ("cleveland", "ga"), ("college park", "ga"), ("collegepark", "ga"),
    ("columbus", "ga"), ("conyers", "ga"), ("cordele", "ga"),
    ("covington", "ga"), ("cumming", "ga"), ("dalton", "ga"),
    ("dawsonville", "ga"), ("decatur", "ga"), ("douglasville", "ga"),
    ("dublin", "ga"), ("duluth", "ga"), ("dunwoody", "ga"),
    ("east point", "ga"), ("eastpoint", "ga"), ("elberton", "ga"),
    ("ellijay", "ga"), ("fairburn", "ga"), ("fayetteville", "ga"),
    ("fitzgerald", "ga"), ("forsyth", "ga"), ("gainesville", "ga"),
    ("griffin", "ga"), ("grovetown", "ga"), ("hampton", "ga"),
    ("hinesville", "ga"), ("holly springs", "ga"), ("hollysprings", "ga"),
    ("johns creek", "ga"), ("johnscreek", "ga"), ("kennesaw", "ga"),
    ("lagrange", "ga"), ("lawrenceville", "ga"), ("lilburn", "ga"),
    ("lithia springs", "ga"), ("lithiasprings", "ga"), ("loganville", "ga"),
    ("macon", "ga"), ("marietta", "ga"), ("mcdonough", "ga"),
    ("mcrae", "ga"), ("milledgeville", "ga"), ("milton", "ga"),
    ("monroe", "ga"), ("moultrie", "ga"), ("newnan", "ga"),
    ("norcross", "ga"), ("peachtree city", "ga"), ("peachtreecity", "ga"),
    ("peachtree corners", "ga"), ("peachtreecorners", "ga"),
    ("perry", "ga"), ("pooler", "ga"), ("richmond hill", "ga"),
    ("richmondhill", "ga"), ("ringgold", "ga"), ("rome", "ga"),
    ("roswell", "ga"), ("sandy springs", "ga"), ("sandysprings", "ga"),
    ("savannah", "ga"), ("smyrna", "ga"), ("snellville", "ga"),
    ("statesboro", "ga"), ("stockbridge", "ga"), ("stone mountain", "ga"),
    ("stonemountain", "ga"), ("suwanee", "ga"), ("thomaston", "ga"),
    ("thomasville", "ga"), ("tifton", "ga"), ("tucker", "ga"),
    ("union city", "ga"), ("unioncity", "ga"), ("valdosta", "ga"),
    ("villa rica", "ga"), ("villarica", "ga"), ("winder", "ga"),
    ("woodstock", "ga"), ("warner robins", "ga"), ("warnerrobins", "ga"),

    # FL more
    ("altamonte springs", "fl"), ("altamontesprings", "fl"),
    ("apalachicola", "fl"), ("apopka", "fl"), ("atlantic beach", "fl"),
    ("atlanticbeach", "fl"), ("aventura", "fl"), ("bartow", "fl"),
    ("belle glade", "fl"), ("belleglade", "fl"), ("boca raton", "fl"),
    ("bocaraton", "fl"), ("bonita springs", "fl"), ("bonitasprings", "fl"),
    ("bradenton", "fl"), ("brooksville", "fl"), ("cape canaveral", "fl"),
    ("capecanaveral", "fl"), ("cape coral", "fl"), ("capecoral", "fl"),
    ("casselberry", "fl"), ("clearwater", "fl"), ("clermont", "fl"),
    ("clewiston", "fl"), ("cocoa", "fl"), ("cocoa beach", "fl"),
    ("coconut creek", "fl"), ("coconutcreek", "fl"),
    ("coral gables", "fl"), ("coralgables", "fl"),
    ("coral springs", "fl"), ("coralsprings", "fl"),
    ("crestview", "fl"), ("dade city", "fl"), ("dadecity", "fl"),
    ("daytona beach", "fl"), ("daytonabeach", "fl"),
    ("deerfield beach", "fl"), ("deerfieldbeach", "fl"),
    ("deltona", "fl"), ("destin", "fl"),
    ("doral", "fl"), ("dunedin", "fl"),
    ("edgewater", "fl"), ("englewood", "fl"),
    ("estero", "fl"), ("eustis", "fl"),
    ("fernandina beach", "fl"), ("fernandinabeach", "fl"),
    ("fort lauderdale", "fl"), ("fortlauderdale", "fl"),
    ("fort myers", "fl"), ("fortmyers", "fl"),
    ("fort myers beach", "fl"), ("fortmyersbeach", "fl"),
    ("fort pierce", "fl"), ("fortpierce", "fl"),
    ("fort walton beach", "fl"), ("fortwaltonbeach", "fl"),
    ("gainesville", "fl"), ("greenacres", "fl"),
    ("hallandale beach", "fl"), ("hallandalebeach", "fl"),
    ("hialeah", "fl"), ("hialeah gardens", "fl"),
    ("hollywood", "fl"), ("homestead", "fl"),
    ("immokalee", "fl"), ("inverness", "fl"),
    ("jacksonville", "fl"), ("jacksonville beach", "fl"),
    ("jacksonvillebeach", "fl"), ("jupiter", "fl"),
    ("kendall", "fl"), ("key west", "fl"), ("keywest", "fl"),
    ("kissimmee", "fl"), ("lake city", "fl"),
    ("lake mary", "fl"), ("lakemary", "fl"),
    ("lake worth", "fl"), ("lakeworth", "fl"),
    ("lakeland", "fl"), ("largo", "fl"),
    ("lauderhill", "fl"), ("leesburg", "fl"),
    ("margate", "fl"), ("melbourne", "fl"),
    ("miami", "fl"), ("miami beach", "fl"), ("miamibeach", "fl"),
    ("miami gardens", "fl"), ("miamigardens", "fl"),
    ("midway", "fl"), ("miramar", "fl"),
    ("mount dora", "fl"), ("mountdora", "fl"),
    ("naples", "fl"), ("new port richey", "fl"), ("newportrichey", "fl"),
    ("new smyrna beach", "fl"), ("newsmyrnabeach", "fl"),
    ("niceville", "fl"), ("north lauderdale", "fl"),
    ("northlauderdale", "fl"), ("north miami", "fl"),
    ("northmiami", "fl"), ("north miami beach", "fl"),
    ("northmiamibeach", "fl"), ("north palm beach", "fl"),
    ("northpalmbeach", "fl"), ("north port", "fl"),
    ("northport", "fl"), ("ocala", "fl"),
    ("ocoee", "fl"), ("okeechobee", "fl"),
    ("orange city", "fl"), ("orangecity", "fl"),
    ("orange park", "fl"), ("orangepark", "fl"),
    ("orlando", "fl"), ("ormond beach", "fl"), ("ormondbeach", "fl"),
    ("oviedo", "fl"), ("palm bay", "fl"), ("palmbay", "fl"),
    ("palm beach gardens", "fl"), ("palmbeachgardens", "fl"),
    ("palm coast", "fl"), ("palmcoast", "fl"),
    ("panama city", "fl"), ("panamacity", "fl"),
    ("panama city beach", "fl"), ("panamacitybeach", "fl"),
    ("pensacola", "fl"), ("pinellas park", "fl"), ("pinellaspark", "fl"),
    ("plantation", "fl"), ("pompano beach", "fl"), ("pompanobeach", "fl"),
    ("ponte vedra", "fl"), ("pontevedra", "fl"),
    ("port charlotte", "fl"), ("portcharlotte", "fl"),
    ("port orange", "fl"), ("portorange", "fl"),
    ("port richey", "fl"), ("portrichey", "fl"),
    ("port saint lucie", "fl"), ("portstlucie", "fl"),
    ("rockledge", "fl"), ("royal palm beach", "fl"),
    ("royalpalmbeach", "fl"), ("sanford", "fl"),
    ("sarasota", "fl"), ("sebastian", "fl"),
    ("seminole", "fl"), ("spring hill", "fl"), ("springhill", "fl"),
    ("st augustine", "fl"), ("staugustine", "fl"),
    ("st cloud", "fl"), ("stcloud", "fl"),
    ("st pete beach", "fl"), ("stpetebeach", "fl"),
    ("st petersburg", "fl"), ("stpetersburg", "fl"),
    ("sun city center", "fl"), ("suncitycenter", "fl"),
    ("sunrise", "fl"), ("tallahassee", "fl"),
    ("tamarac", "fl"), ("tampa", "fl"),
    ("tarpon springs", "fl"), ("tarponsprings", "fl"),
    ("tavares", "fl"), ("temple terrace", "fl"), ("templeterrace", "fl"),
    ("titusville", "fl"), ("valparaiso", "fl"),
    ("venice", "fl"), ("vero beach", "fl"), ("verobeach", "fl"),
    ("wauchula", "fl"), ("west miami", "fl"), ("westmiami", "fl"),
    ("west palm beach", "fl"), ("westpalmbeach", "fl"),
    ("weston", "fl"), ("wildwood", "fl"),
    ("winter garden", "fl"), ("wintergarden", "fl"),
    ("winter haven", "fl"), ("winterhaven", "fl"),
    ("winter park", "fl"), ("winterpark", "fl"),
    ("winter springs", "fl"), ("wintersprings", "fl"),
    ("zephyrhills", "fl"),
]


async def probe_slug(session, slug, semaphore):
    url = f"https://api-east.viewpointcloud.com/v2/{slug}/categories"
    async with semaphore:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    try:
                        data = await resp.json()
                        count = len(data.get("data", []))
                        return slug, count
                    except Exception:
                        return slug, 0
                return None
        except Exception:
            return None


def infer_state(slug):
    states = ["al","ak","az","ar","ca","co","ct","de","fl","ga","hi","id","il","in","ia","ks",
              "ky","la","me","md","ma","mi","mn","ms","mo","mt","ne","nv","nh","nj","nm","ny",
              "nc","nd","oh","ok","or","pa","ri","sc","sd","tn","tx","ut","vt","va","wa","wv","wi","wy"]
    for abbr in sorted(states, key=len, reverse=True):
        if slug.endswith(abbr):
            return abbr.upper()
    return "??"


async def main():
    # Build candidates
    candidates = set()

    # MA towns
    for town in MA_TOWNS:
        slug = re.sub(r'[^a-z0-9]', '', town.lower()) + "ma"
        if slug not in KNOWN:
            candidates.add(slug)

    # RI towns
    for town in RI_TOWNS:
        slug = re.sub(r'[^a-z0-9]', '', town.lower())
        if not slug.endswith("ri"):
            slug = slug + "ri"
        if slug not in KNOWN:
            candidates.add(slug)

    # CT towns
    for town in CT_TOWNS:
        slug = re.sub(r'[^a-z0-9]', '', town.lower())
        if not slug.endswith("ct"):
            slug = slug + "ct"
        if slug not in KNOWN:
            candidates.add(slug)

    # NH towns
    for town in NH_TOWNS:
        slug = re.sub(r'[^a-z0-9]', '', town.lower()) + "nh"
        if slug not in KNOWN:
            candidates.add(slug)

    # VT towns
    for town in VT_TOWNS:
        slug = re.sub(r'[^a-z0-9]', '', town.lower()) + "vt"
        if slug not in KNOWN:
            candidates.add(slug)

    # ME towns
    for town in ME_TOWNS:
        slug = re.sub(r'[^a-z0-9]', '', town.lower()) + "me"
        if slug not in KNOWN:
            candidates.add(slug)

    # More states
    for city, state in MORE_STATES:
        slug = re.sub(r'[^a-z0-9]', '', city.lower()) + state
        if slug not in KNOWN:
            candidates.add(slug)

    print(f"Round 3 candidates: {len(candidates)}")

    semaphore = asyncio.Semaphore(5)
    found = []
    batch_size = 25
    connector = aiohttp.TCPConnector(limit=10, ssl=False)
    headers = {"User-Agent": "Mozilla/5.0 (compatible; permit-research/1.0)"}

    candidates_list = list(candidates)

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        tasks = [probe_slug(session, slug, semaphore) for slug in candidates_list]

        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            results = await asyncio.gather(*batch)
            for r in results:
                if r:
                    slug, count = r
                    state = infer_state(slug)
                    found.append({"slug": slug, "state": state, "name": slug, "categories": count})
            await asyncio.sleep(0.5)
            done = min(i + batch_size, len(tasks))
            if (i // batch_size) % 10 == 0 or done == len(tasks):
                print(f"  Progress: {done}/{len(tasks)} | {len(found)} new found")

    print(f"\nRound 3 found {len(found)} additional portals")

    # Merge and save
    all_portals = list(existing)
    existing_slugs = set(p["slug"] for p in existing)
    for r in found:
        if r["slug"] not in existing_slugs:
            all_portals.append(r)
            existing_slugs.add(r["slug"])

    all_portals.sort(key=lambda x: (x.get("state", ""), x.get("name", "")))
    output_path = "/home/will/permit-api/scripts/opengov_portals.json"
    with open(output_path, "w") as f:
        json.dump(all_portals, f, indent=2)

    print(f"Total portals: {len(all_portals)}")

    if found:
        print("\nNEWLY FOUND IN ROUND 3:")
        for p in sorted(found, key=lambda x: x["state"]):
            print(f"  {p['slug']:45s} {p['state']:5s} ({p['categories']} cats)")

    return all_portals


if __name__ == "__main__":
    asyncio.run(main())
