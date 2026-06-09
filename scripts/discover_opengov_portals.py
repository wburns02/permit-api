#!/usr/bin/env python3
"""
OpenGov/ViewPoint Cloud portal discovery script.
Brute-forces community slugs against the categories API.
Pattern: {cityname}{stateabbrev} or countyof{name}{state} etc.
"""

import asyncio
import aiohttp
import json
import re
import time
from pathlib import Path

# ── Existing known slugs (216 from opengov-config.ts) ─────────────────────────
KNOWN_SLUGS = """abingtonpa americancanyonca andersonsc apopkafl aransaspasstx arcataca
arlingtonma avonct baltimoremddhcd bedfordtx beniciaca bereaky beverlyma bloomfieldct
bolingbrookil bournema boxfordma branfordct brewsterma brightonco bristolct bristolri
brownsburgin burnsvillemn calimesaca camarilloca cambridgema cantonma cecilcountymd
chambleega champaignil chapelhillnc chathamcountync chathamma chattanoogatn cheltenhampa
cheshirect cheyennewy cityoflapalmaca cityofsanrafaelca claremontnh clearlakeca
cocoabeachfl colusacountyca cortlandtny countyofandersonsc countyofbexartx
countyofdorchestermd countyofinyoca countyoflakeca countyofnashnc countyofonondagany
countyofsaukwi countyofwilsonnc coventryri cranberrytownshippa cranstonri cumberlandri
danburyct darienct davidsoncountync decaturil deerfieldil dekalbcountyga dennisma
dudleyma durangoco eastgreenwichri easthartfordct eastonpa eastprovidenceri edgartownma
elsegundoca ennistx eurekaca fairfieldoh fallriverma farmingtonct fishersin fortunaca
framinghamma frederickmd gahannaoh galvestoncountytx gardnerma garyin glastonburyct
glocesterri glynncountyga goddardks goosecreeksc grotonma hamdenct hamiltontn hanoverma
hempsteadny hudsonma hudsonoh industryca ithacacityny jacksonms kingsmountainnc
lakeportca lauderdalelakesfl lemontil lexingtonma littletonma madisonct maplevalleywa
marathonfl marionnc medfieldma medinamn methuenma metronashvilletn middletownri
millvalleyca monroecountyin mountpleasantny mountvernonny narragansettri natickma
natronacountywy needhamma newbedfordma newburyportma newcanaanct newfairfieldct
newingtonct newmilfordct newportri newshorehamri newtonma northamptonma
northattleboroughma northboroughma northcantonoh northkingstownri northmyrtlebeachsc
northprovidenceri northstpaulmn norwichct nyecountynv oakdalemn oconomowocwi
orangebeachal peabodyma plaincityoh plainfieldil polkcountyia portagecountyoh
postfallsid princetonnj providenceri provincetownma pueblocountyco richmondri
ridgefieldct rochesternh rockyhillct salemma salinany sandyspringsga schaumburgil
scituateri scottsvalleyca scrantonpa seagovilletx sewardak shrewsburyma smithfieldri
smyrnaga sonomaca southhadleyma southkingstownri southportnc springfielddelco
springfieldma springhillks stamfordct stateofidaho stonehamma stoningtonct
stpetersburgfl stuartfl sunprairiewi tallmadgeoh tewksburyma tisburyma torringtonct
townofbrattleborovt townofhuntingtonny townofwarrenton townshiplowermakefieldpa
unioncountyoh valleystreamny vestaviahillsal warehamma warrencountync watertownma
watertownsd waukeeia westerlyri westernriversidecogca westspringfieldma westwarwickri
williamstownma willingtonct wiltonct winchesterct winonacountymn woodburyct woosteroh
worcesterma yorkme yorkpa""".split()

# ── US States abbreviation map ─────────────────────────────────────────────────
STATES = {
    "al": "Alabama", "ak": "Alaska", "az": "Arizona", "ar": "Arkansas",
    "ca": "California", "co": "Colorado", "ct": "Connecticut", "de": "Delaware",
    "fl": "Florida", "ga": "Georgia", "hi": "Hawaii", "id": "Idaho",
    "il": "Illinois", "in": "Indiana", "ia": "Iowa", "ks": "Kansas",
    "ky": "Kentucky", "la": "Louisiana", "me": "Maine", "md": "Maryland",
    "ma": "Massachusetts", "mi": "Michigan", "mn": "Minnesota", "ms": "Mississippi",
    "mo": "Missouri", "mt": "Montana", "ne": "Nebraska", "nv": "Nevada",
    "nh": "New Hampshire", "nj": "New Jersey", "nm": "New Mexico", "ny": "New York",
    "nc": "North Carolina", "nd": "North Dakota", "oh": "Ohio", "ok": "Oklahoma",
    "or": "Oregon", "pa": "Pennsylvania", "ri": "Rhode Island", "sc": "South Carolina",
    "sd": "South Dakota", "tn": "Tennessee", "tx": "Texas", "ut": "Utah",
    "vt": "Vermont", "va": "Virginia", "wa": "Washington", "wv": "West Virginia",
    "wi": "Wisconsin", "wy": "Wyoming", "dc": "District of Columbia",
}

STATE_ABBREV_TO_NAME = {v.lower().replace(" ", ""): k for k, v in STATES.items()}

# ── Large candidate city list organized by state ───────────────────────────────
# Format: (city_slug_part, state_abbrev, city_display_name)
CITY_CANDIDATES = [
    # Alabama
    ("auburn", "al", "Auburn"), ("bessemer", "al", "Bessemer"),
    ("birmingham", "al", "Birmingham"), ("decatur", "al", "Decatur"),
    ("dothan", "al", "Dothan"), ("enterprise", "al", "Enterprise"),
    ("florence", "al", "Florence"), ("gadsden", "al", "Gadsden"),
    ("hoover", "al", "Hoover"), ("huntsville", "al", "Huntsville"),
    ("madison", "al", "Madison"), ("mobile", "al", "Mobile"),
    ("montgomery", "al", "Montgomery"), ("prattville", "al", "Prattville"),
    ("tuscaloosa", "al", "Tuscaloosa"), ("opelika", "al", "Opelika"),
    ("phenixcity", "al", "Phenix City"), ("vestavia", "al", "Vestavia Hills"),
    ("albertville", "al", "Albertville"), ("anniston", "al", "Anniston"),

    # Alaska
    ("anchorage", "ak", "Anchorage"), ("fairbanks", "ak", "Fairbanks"),
    ("juneau", "ak", "Juneau"), ("sitka", "ak", "Sitka"),
    ("kenai", "ak", "Kenai"), ("ketchikan", "ak", "Ketchikan"),
    ("wasilla", "ak", "Wasilla"), ("kodiak", "ak", "Kodiak"),
    ("homer", "ak", "Homer"), ("palmer", "ak", "Palmer"),

    # Arizona
    ("avondale", "az", "Avondale"), ("buckeye", "az", "Buckeye"),
    ("bullheadcity", "az", "Bullhead City"), ("casagrande", "az", "Casa Grande"),
    ("chandler", "az", "Chandler"), ("flagstaff", "az", "Flagstaff"),
    ("gilbert", "az", "Gilbert"), ("glendale", "az", "Glendale"),
    ("goodyear", "az", "Goodyear"), ("lakehavasucity", "az", "Lake Havasu City"),
    ("maricopa", "az", "Maricopa"), ("mesa", "az", "Mesa"),
    ("peoria", "az", "Peoria"), ("phoenix", "az", "Phoenix"),
    ("prescott", "az", "Prescott"), ("queencreek", "az", "Queen Creek"),
    ("scottsdale", "az", "Scottsdale"), ("surprise", "az", "Surprise"),
    ("tempe", "az", "Tempe"), ("tucson", "az", "Tucson"),
    ("yuma", "az", "Yuma"), ("gilbertaz", "az", "Gilbert"),
    ("maricopacounty", "az", "Maricopa County"), ("pimaaz", "az", "Pima"),
    ("pimacounty", "az", "Pima County"),

    # Arkansas
    ("conway", "ar", "Conway"), ("fayetteville", "ar", "Fayetteville"),
    ("fortsmith", "ar", "Fort Smith"), ("hot springs", "ar", "Hot Springs"),
    ("hotsprings", "ar", "Hot Springs"), ("jonesboro", "ar", "Jonesboro"),
    ("littlerock", "ar", "Little Rock"), ("northlittlerock", "ar", "North Little Rock"),
    ("rogers", "ar", "Rogers"), ("springdale", "ar", "Springdale"),
    ("texarkana", "ar", "Texarkana"), ("bentonville", "ar", "Bentonville"),

    # California
    ("alameda", "ca", "Alameda"), ("alhambra", "ca", "Alhambra"),
    ("anaheim", "ca", "Anaheim"), ("antioch", "ca", "Antioch"),
    ("bakersfield", "ca", "Bakersfield"), ("berkeley", "ca", "Berkeley"),
    ("buenapark", "ca", "Buena Park"), ("burbank", "ca", "Burbank"),
    ("carlsbad", "ca", "Carlsbad"), ("chino", "ca", "Chino"),
    ("chinohills", "ca", "Chino Hills"), ("chula vista", "ca", "Chula Vista"),
    ("chulavista", "ca", "Chula Vista"), ("citrus heights", "ca", "Citrus Heights"),
    ("citrushieghts", "ca", "Citrus Heights"), ("clovis", "ca", "Clovis"),
    ("compton", "ca", "Compton"), ("concord", "ca", "Concord"),
    ("corona", "ca", "Corona"), ("costamesa", "ca", "Costa Mesa"),
    ("daly city", "ca", "Daly City"), ("dalycity", "ca", "Daly City"),
    ("downey", "ca", "Downey"), ("elk grove", "ca", "Elk Grove"),
    ("elkgrove", "ca", "Elk Grove"), ("escondido", "ca", "Escondido"),
    ("fairfield", "ca", "Fairfield"), ("fontana", "ca", "Fontana"),
    ("fremont", "ca", "Fremont"), ("fresno", "ca", "Fresno"),
    ("fullerton", "ca", "Fullerton"), ("garden grove", "ca", "Garden Grove"),
    ("gardengrove", "ca", "Garden Grove"), ("glendale", "ca", "Glendale"),
    ("hawthorne", "ca", "Hawthorne"), ("hayward", "ca", "Hayward"),
    ("hemet", "ca", "Hemet"), ("huntington beach", "ca", "Huntington Beach"),
    ("huntingtonbeach", "ca", "Huntington Beach"), ("inglewood", "ca", "Inglewood"),
    ("irvine", "ca", "Irvine"), ("lacounty", "ca", "LA County"),
    ("lancaster", "ca", "Lancaster"), ("livermore", "ca", "Livermore"),
    ("lodi", "ca", "Lodi"), ("longbeach", "ca", "Long Beach"),
    ("losangeles", "ca", "Los Angeles"), ("lynwood", "ca", "Lynwood"),
    ("modesto", "ca", "Modesto"), ("montebello", "ca", "Montebello"),
    ("moreno valley", "ca", "Moreno Valley"), ("morenovalley", "ca", "Moreno Valley"),
    ("murrieta", "ca", "Murrieta"), ("norwalk", "ca", "Norwalk"),
    ("oakland", "ca", "Oakland"), ("oceanside", "ca", "Oceanside"),
    ("ontario", "ca", "Ontario"), ("orangeca", "ca", "Orange"),
    ("oroville", "ca", "Oroville"), ("oxnard", "ca", "Oxnard"),
    ("palmdale", "ca", "Palmdale"), ("paloalto", "ca", "Palo Alto"),
    ("pasadena", "ca", "Pasadena"), ("pomona", "ca", "Pomona"),
    ("rancho cucamonga", "ca", "Rancho Cucamonga"),
    ("ranchocucamonga", "ca", "Rancho Cucamonga"),
    ("redding", "ca", "Redding"), ("rialto", "ca", "Rialto"),
    ("richmond", "ca", "Richmond"), ("riverside", "ca", "Riverside"),
    ("roseville", "ca", "Roseville"), ("sacramento", "ca", "Sacramento"),
    ("salinas", "ca", "Salinas"), ("san bernardino", "ca", "San Bernardino"),
    ("sanbernardino", "ca", "San Bernardino"), ("sandiego", "ca", "San Diego"),
    ("sanfrancisco", "ca", "San Francisco"), ("sanjose", "ca", "San Jose"),
    ("sanmateo", "ca", "San Mateo"), ("santaana", "ca", "Santa Ana"),
    ("santabarbara", "ca", "Santa Barbara"), ("santaclara", "ca", "Santa Clara"),
    ("santaclarita", "ca", "Santa Clarita"), ("santamaria", "ca", "Santa Maria"),
    ("santarosa", "ca", "Santa Rosa"), ("simi valley", "ca", "Simi Valley"),
    ("simivalley", "ca", "Simi Valley"), ("stockton", "ca", "Stockton"),
    ("sunnyvale", "ca", "Sunnyvale"), ("thousand oaks", "ca", "Thousand Oaks"),
    ("thousandoaks", "ca", "Thousand Oaks"), ("torrance", "ca", "Torrance"),
    ("tulare", "ca", "Tulare"), ("turlock", "ca", "Turlock"),
    ("vacaville", "ca", "Vacaville"), ("vallejo", "ca", "Vallejo"),
    ("ventura", "ca", "Ventura"), ("victorville", "ca", "Victorville"),
    ("visalia", "ca", "Visalia"), ("westcovina", "ca", "West Covina"),
    ("westminster", "ca", "Westminster"),
    ("countyofmarin", "ca", "Marin County"), ("marincounty", "ca", "Marin County"),
    ("countysandiego", "ca", "San Diego County"),
    ("placer", "ca", "Placer"), ("placercounty", "ca", "Placer County"),
    ("sacramentocounty", "ca", "Sacramento County"),
    ("slocounty", "ca", "San Luis Obispo County"),
    ("santacruzcounty", "ca", "Santa Cruz County"),
    ("sonoma", "ca", "Sonoma County"),
    ("sonomacounty", "ca", "Sonoma County"),
    ("sutter", "ca", "Sutter"), ("suttercounty", "ca", "Sutter County"),
    ("tehama", "ca", "Tehama"), ("tehamacounty", "ca", "Tehama County"),
    ("trinity", "ca", "Trinity"), ("trinitycounty", "ca", "Trinity County"),
    ("tuolumne", "ca", "Tuolumne"), ("tuolumnecounty", "ca", "Tuolumne County"),
    ("yolo", "ca", "Yolo"), ("yolocounty", "ca", "Yolo County"),

    # Colorado
    ("arvada", "co", "Arvada"), ("aspen", "co", "Aspen"),
    ("aurora", "co", "Aurora"), ("boulder", "co", "Boulder"),
    ("broomfield", "co", "Broomfield"), ("castle rock", "co", "Castle Rock"),
    ("castlerock", "co", "Castle Rock"), ("centennial", "co", "Centennial"),
    ("colorado springs", "co", "Colorado Springs"),
    ("coloradosprings", "co", "Colorado Springs"),
    ("commerce city", "co", "Commerce City"),
    ("commercecity", "co", "Commerce City"),
    ("denver", "co", "Denver"), ("englewood", "co", "Englewood"),
    ("fort collins", "co", "Fort Collins"), ("fortcollins", "co", "Fort Collins"),
    ("greeley", "co", "Greeley"), ("highlands ranch", "co", "Highlands Ranch"),
    ("highlandsranch", "co", "Highlands Ranch"),
    ("lakewood", "co", "Lakewood"), ("longmont", "co", "Longmont"),
    ("loveland", "co", "Loveland"), ("parker", "co", "Parker"),
    ("pueblo", "co", "Pueblo"), ("sterling", "co", "Sterling"),
    ("thornton", "co", "Thornton"), ("westminster", "co", "Westminster"),
    ("wheat ridge", "co", "Wheat Ridge"), ("wheatridge", "co", "Wheat Ridge"),
    ("arapahoe", "co", "Arapahoe"), ("arapahoecounty", "co", "Arapahoe County"),
    ("boulderco", "co", "Boulder County"), ("bouldercounty", "co", "Boulder County"),
    ("elpaso", "co", "El Paso County"), ("elpasocounty", "co", "El Paso County"),
    ("jeffersoncounty", "co", "Jefferson County"),
    ("larimer", "co", "Larimer"), ("larimercounty", "co", "Larimer County"),
    ("weld", "co", "Weld"), ("weldcounty", "co", "Weld County"),

    # Connecticut (many already known, but add more)
    ("ansonia", "ct", "Ansonia"), ("bethel", "ct", "Bethel"),
    ("east haven", "ct", "East Haven"), ("easthaven", "ct", "East Haven"),
    ("east lyme", "ct", "East Lyme"), ("eastlyme", "ct", "East Lyme"),
    ("east windsor", "ct", "East Windsor"), ("enfield", "ct", "Enfield"),
    ("greenwich", "ct", "Greenwich"), ("groton", "ct", "Groton"),
    ("guilford", "ct", "Guilford"), ("hartford", "ct", "Hartford"),
    ("killingly", "ct", "Killingly"), ("manchester", "ct", "Manchester"),
    ("meriden", "ct", "Meriden"), ("middletown", "ct", "Middletown"),
    ("milford", "ct", "Milford"), ("monroe", "ct", "Monroe"),
    ("mystic", "ct", "Mystic"), ("naugatuck", "ct", "Naugatuck"),
    ("new britain", "ct", "New Britain"), ("newbritain", "ct", "New Britain"),
    ("new haven", "ct", "New Haven"), ("newhaven", "ct", "New Haven"),
    ("newington", "ct", "Newington"), ("north haven", "ct", "North Haven"),
    ("northhaven", "ct", "North Haven"), ("orange", "ct", "Orange"),
    ("plainville", "ct", "Plainville"), ("shelton", "ct", "Shelton"),
    ("simsbury", "ct", "Simsbury"), ("south windsor", "ct", "South Windsor"),
    ("southwindsor", "ct", "South Windsor"), ("southbury", "ct", "Southbury"),
    ("stratford", "ct", "Stratford"), ("suffield", "ct", "Suffield"),
    ("thomaston", "ct", "Thomaston"), ("trumbull", "ct", "Trumbull"),
    ("vernon", "ct", "Vernon"), ("wallingford", "ct", "Wallingford"),
    ("waterbury", "ct", "Waterbury"), ("waterford", "ct", "Waterford"),
    ("west hartford", "ct", "West Hartford"), ("westhartford", "ct", "West Hartford"),
    ("west haven", "ct", "West Haven"), ("westhaven", "ct", "West Haven"),
    ("wethersfield", "ct", "Wethersfield"), ("windham", "ct", "Windham"),
    ("windsor", "ct", "Windsor"), ("wolcott", "ct", "Wolcott"),

    # Delaware
    ("dover", "de", "Dover"), ("middletown", "de", "Middletown"),
    ("milford", "de", "Milford"), ("newark", "de", "Newark"),
    ("smyrna", "de", "Smyrna"), ("wilmington", "de", "Wilmington"),

    # Florida
    ("boca raton", "fl", "Boca Raton"), ("bocaraton", "fl", "Boca Raton"),
    ("bonita springs", "fl", "Bonita Springs"), ("bonitasprings", "fl", "Bonita Springs"),
    ("bradenton", "fl", "Bradenton"), ("cape coral", "fl", "Cape Coral"),
    ("capecoral", "fl", "Cape Coral"), ("clearwater", "fl", "Clearwater"),
    ("coral gables", "fl", "Coral Gables"), ("coralgables", "fl", "Coral Gables"),
    ("coral springs", "fl", "Coral Springs"), ("coralsprings", "fl", "Coral Springs"),
    ("deerfield beach", "fl", "Deerfield Beach"), ("deltona", "fl", "Deltona"),
    ("florida", "fl", "Florida"), ("fort lauderdale", "fl", "Fort Lauderdale"),
    ("fortlauderdale", "fl", "Fort Lauderdale"), ("fort myers", "fl", "Fort Myers"),
    ("fortmyers", "fl", "Fort Myers"), ("gainesville", "fl", "Gainesville"),
    ("hollywood", "fl", "Hollywood"), ("homestead", "fl", "Homestead"),
    ("jacksonville", "fl", "Jacksonville"), ("jupiter", "fl", "Jupiter"),
    ("kissimmee", "fl", "Kissimmee"), ("lakeland", "fl", "Lakeland"),
    ("largo", "fl", "Largo"), ("melbourne", "fl", "Melbourne"),
    ("miami", "fl", "Miami"), ("miami gardens", "fl", "Miami Gardens"),
    ("miramar", "fl", "Miramar"), ("naples", "fl", "Naples"),
    ("new port richey", "fl", "New Port Richey"), ("newportrichey", "fl", "New Port Richey"),
    ("ocala", "fl", "Ocala"), ("orlando", "fl", "Orlando"),
    ("palm bay", "fl", "Palm Bay"), ("palmbay", "fl", "Palm Bay"),
    ("palm beach", "fl", "Palm Beach"), ("palmbeach", "fl", "Palm Beach"),
    ("palm beach gardens", "fl", "Palm Beach Gardens"),
    ("pensacola", "fl", "Pensacola"), ("plantation", "fl", "Plantation"),
    ("pompano beach", "fl", "Pompano Beach"), ("pompanobeach", "fl", "Pompano Beach"),
    ("port st lucie", "fl", "Port St. Lucie"), ("portstlucie", "fl", "Port St. Lucie"),
    ("sarasota", "fl", "Sarasota"), ("spring hill", "fl", "Spring Hill"),
    ("springhillfl", "fl", "Spring Hill"), ("tallahassee", "fl", "Tallahassee"),
    ("tamarac", "fl", "Tamarac"), ("tampa", "fl", "Tampa"),
    ("west palm beach", "fl", "West Palm Beach"), ("westpalmbeach", "fl", "West Palm Beach"),
    ("brevardcounty", "fl", "Brevard County"), ("browardcounty", "fl", "Broward County"),
    ("charlottecounty", "fl", "Charlotte County"), ("citrus county", "fl", "Citrus County"),
    ("colliercounty", "fl", "Collier County"), ("columbia county", "fl", "Columbia County"),
    ("flaglercounty", "fl", "Flagler County"), ("hernandocounty", "fl", "Hernando County"),
    ("hillsboroughcounty", "fl", "Hillsborough County"),
    ("lakecounty", "fl", "Lake County"), ("leecounty", "fl", "Lee County"),
    ("manateecounty", "fl", "Manatee County"), ("marioncounty", "fl", "Marion County"),
    ("miamidade", "fl", "Miami-Dade"), ("miamidadecounty", "fl", "Miami-Dade County"),
    ("okaloosacounty", "fl", "Okaloosa County"), ("orangecounty", "fl", "Orange County"),
    ("osceolaounty", "fl", "Osceola County"), ("palmbeachcounty", "fl", "Palm Beach County"),
    ("pascounty", "fl", "Pasco County"), ("pinellascounty", "fl", "Pinellas County"),
    ("polkcountyfl", "fl", "Polk County"), ("putnam county", "fl", "Putnam County"),
    ("sarasotacounty", "fl", "Sarasota County"), ("seminolecounty", "fl", "Seminole County"),
    ("stluciescounty", "fl", "St. Lucie County"), ("stjohnscounty", "fl", "St. Johns County"),
    ("suwanneecounty", "fl", "Suwannee County"), ("volusia", "fl", "Volusia"),
    ("volusiacounty", "fl", "Volusia County"),

    # Georgia
    ("albany", "ga", "Albany"), ("alpharetta", "ga", "Alpharetta"),
    ("athens", "ga", "Athens"), ("atlanta", "ga", "Atlanta"),
    ("augusta", "ga", "Augusta"), ("brookhaven", "ga", "Brookhaven"),
    ("canton", "ga", "Canton"), ("cartersville", "ga", "Cartersville"),
    ("columbus", "ga", "Columbus"), ("covington", "ga", "Covington"),
    ("duluth", "ga", "Duluth"), ("dunwoody", "ga", "Dunwoody"),
    ("east point", "ga", "East Point"), ("gainesville", "ga", "Gainesville"),
    ("johns creek", "ga", "Johns Creek"), ("johnscreek", "ga", "Johns Creek"),
    ("kennesaw", "ga", "Kennesaw"), ("lawrenceville", "ga", "Lawrenceville"),
    ("macon", "ga", "Macon"), ("marietta", "ga", "Marietta"),
    ("mcdonough", "ga", "McDonough"), ("milton", "ga", "Milton"),
    ("peachtree city", "ga", "Peachtree City"), ("peachtreecity", "ga", "Peachtree City"),
    ("rome", "ga", "Rome"), ("roswell", "ga", "Roswell"),
    ("savannah", "ga", "Savannah"), ("stockbridge", "ga", "Stockbridge"),
    ("tucker", "ga", "Tucker"), ("valdosta", "ga", "Valdosta"),
    ("warner robins", "ga", "Warner Robins"), ("warnerrobins", "ga", "Warner Robins"),
    ("woodstock", "ga", "Woodstock"), ("cobb county", "ga", "Cobb County"),
    ("cobbcounty", "ga", "Cobb County"), ("dekalb", "ga", "DeKalb"),
    ("forsythcounty", "ga", "Forsyth County"), ("fulton county", "ga", "Fulton County"),
    ("fultoncounty", "ga", "Fulton County"), ("gwinnettcounty", "ga", "Gwinnett County"),
    ("hall county", "ga", "Hall County"), ("hallcounty", "ga", "Hall County"),
    ("henrycounty", "ga", "Henry County"),

    # Idaho
    ("boise", "id", "Boise"), ("caldwell", "id", "Caldwell"),
    ("coeur d alene", "id", "Coeur d'Alene"), ("coeurdAlene", "id", "Coeur d'Alene"),
    ("coeurdalene", "id", "Coeur d'Alene"), ("idahofalls", "id", "Idaho Falls"),
    ("lewiston", "id", "Lewiston"), ("meridian", "id", "Meridian"),
    ("moscow", "id", "Moscow"), ("nampa", "id", "Nampa"),
    ("pocatello", "id", "Pocatello"), ("rexburg", "id", "Rexburg"),
    ("twin falls", "id", "Twin Falls"), ("twinfalls", "id", "Twin Falls"),
    ("ada county", "id", "Ada County"), ("adacounty", "id", "Ada County"),
    ("blaine county", "id", "Blaine County"), ("blaineounty", "id", "Blaine County"),
    ("bonneville", "id", "Bonneville"), ("canyon county", "id", "Canyon County"),
    ("canyoncounty", "id", "Canyon County"), ("kootenai", "id", "Kootenai"),
    ("kootenaicounty", "id", "Kootenai County"),

    # Illinois
    ("aurora", "il", "Aurora"), ("bloomington", "il", "Bloomington"),
    ("bolingbrook", "il", "Bolingbrook"), ("chicago", "il", "Chicago"),
    ("cicero", "il", "Cicero"), ("crystal lake", "il", "Crystal Lake"),
    ("crystallake", "il", "Crystal Lake"), ("elgin", "il", "Elgin"),
    ("elk grove village", "il", "Elk Grove Village"),
    ("evanston", "il", "Evanston"), ("joliet", "il", "Joliet"),
    ("naperville", "il", "Naperville"), ("normal", "il", "Normal"),
    ("orland park", "il", "Orland Park"), ("orlandpark", "il", "Orland Park"),
    ("palatine", "il", "Palatine"), ("peoria", "il", "Peoria"),
    ("rockford", "il", "Rockford"), ("round lake beach", "il", "Round Lake Beach"),
    ("schaumburg", "il", "Schaumburg"), ("springfield", "il", "Springfield"),
    ("tinley park", "il", "Tinley Park"), ("tinleypark", "il", "Tinley Park"),
    ("waukegan", "il", "Waukegan"), ("wheaton", "il", "Wheaton"),
    ("cook county", "il", "Cook County"), ("cookcounty", "il", "Cook County"),
    ("dupage", "il", "DuPage"), ("dupagecounty", "il", "DuPage County"),
    ("kaneounty", "il", "Kane County"), ("kanecounty", "il", "Kane County"),
    ("lake county", "il", "Lake County"), ("lakecountyil", "il", "Lake County"),
    ("mchenry", "il", "McHenry"), ("mchenrycounty", "il", "McHenry County"),
    ("will county", "il", "Will County"), ("willcounty", "il", "Will County"),

    # Indiana
    ("anderson", "in", "Anderson"), ("bloomington", "in", "Bloomington"),
    ("carmel", "in", "Carmel"), ("columbus", "in", "Columbus"),
    ("evansville", "in", "Evansville"), ("fishers", "in", "Fishers"),
    ("fort wayne", "in", "Fort Wayne"), ("fortwayne", "in", "Fort Wayne"),
    ("gary", "in", "Gary"), ("greenwood", "in", "Greenwood"),
    ("hammond", "in", "Hammond"), ("indianapolis", "in", "Indianapolis"),
    ("kokomo", "in", "Kokomo"), ("lafayette", "in", "Lafayette"),
    ("merrillville", "in", "Merrillville"), ("mishawaka", "in", "Mishawaka"),
    ("muncie", "in", "Muncie"), ("noblesville", "in", "Noblesville"),
    ("south bend", "in", "South Bend"), ("southbend", "in", "South Bend"),
    ("terre haute", "in", "Terre Haute"), ("terrehaute", "in", "Terre Haute"),
    ("westfield", "in", "Westfield"), ("zionsville", "in", "Zionsville"),
    ("hamilton county", "in", "Hamilton County"), ("hamiltoncounty", "in", "Hamilton County"),
    ("hendricks county", "in", "Hendricks County"), ("hendrickscounty", "in", "Hendricks County"),
    ("johnson county", "in", "Johnson County"), ("johnsoncounty", "in", "Johnson County"),
    ("lake county", "in", "Lake County"), ("lakecountyin", "in", "Lake County"),
    ("marion county", "in", "Marion County"), ("marioncountyin", "in", "Marion County"),
    ("tippecanoe", "in", "Tippecanoe"),

    # Iowa
    ("ames", "ia", "Ames"), ("cedar falls", "ia", "Cedar Falls"),
    ("cedarfalls", "ia", "Cedar Falls"), ("cedar rapids", "ia", "Cedar Rapids"),
    ("cedarrapids", "ia", "Cedar Rapids"), ("davenport", "ia", "Davenport"),
    ("des moines", "ia", "Des Moines"), ("desmoines", "ia", "Des Moines"),
    ("dubuque", "ia", "Dubuque"), ("iowa city", "ia", "Iowa City"),
    ("iowacity", "ia", "Iowa City"), ("sioux city", "ia", "Sioux City"),
    ("siouxcity", "ia", "Sioux City"), ("waterloo", "ia", "Waterloo"),
    ("west des moines", "ia", "West Des Moines"),
    ("westdesmoines", "ia", "West Des Moines"),
    ("linn county", "ia", "Linn County"), ("linncounty", "ia", "Linn County"),
    ("polk county", "ia", "Polk County"),

    # Kansas
    ("hutchinson", "ks", "Hutchinson"), ("kansas city", "ks", "Kansas City"),
    ("kansascity", "ks", "Kansas City"), ("lawrence", "ks", "Lawrence"),
    ("lenexa", "ks", "Lenexa"), ("manhattan", "ks", "Manhattan"),
    ("olathe", "ks", "Olathe"), ("overland park", "ks", "Overland Park"),
    ("overlandpark", "ks", "Overland Park"), ("salina", "ks", "Salina"),
    ("shawnee", "ks", "Shawnee"), ("topeka", "ks", "Topeka"),
    ("wichita", "ks", "Wichita"), ("johnson county", "ks", "Johnson County"),
    ("johnsoncountyks", "ks", "Johnson County"), ("shawnee county", "ks", "Shawnee County"),
    ("wyandotte", "ks", "Wyandotte"),

    # Kentucky
    ("bowling green", "ky", "Bowling Green"), ("bowlinggreen", "ky", "Bowling Green"),
    ("covington", "ky", "Covington"), ("elizabethtown", "ky", "Elizabethtown"),
    ("florence", "ky", "Florence"), ("frankfort", "ky", "Frankfort"),
    ("hopkinsville", "ky", "Hopkinsville"), ("lexington", "ky", "Lexington"),
    ("louisville", "ky", "Louisville"), ("owensboro", "ky", "Owensboro"),
    ("paducah", "ky", "Paducah"), ("richmond", "ky", "Richmond"),
    ("fayette county", "ky", "Fayette County"),

    # Louisiana
    ("alexandria", "la", "Alexandria"), ("baton rouge", "la", "Baton Rouge"),
    ("batonrouge", "la", "Baton Rouge"), ("bossier city", "la", "Bossier City"),
    ("bossiercity", "la", "Bossier City"), ("kenner", "la", "Kenner"),
    ("lafayette", "la", "Lafayette"), ("lake charles", "la", "Lake Charles"),
    ("lakecharles", "la", "Lake Charles"), ("metairie", "la", "Metairie"),
    ("monroe", "la", "Monroe"), ("new orleans", "la", "New Orleans"),
    ("neworleans", "la", "New Orleans"), ("shreveport", "la", "Shreveport"),
    ("slidell", "la", "Slidell"),

    # Maine
    ("auburn", "me", "Auburn"), ("augusta", "me", "Augusta"),
    ("bangor", "me", "Bangor"), ("biddeford", "me", "Biddeford"),
    ("lewiston", "me", "Lewiston"), ("portland", "me", "Portland"),
    ("south portland", "me", "South Portland"), ("southportland", "me", "South Portland"),
    ("westbrook", "me", "Westbrook"),

    # Maryland
    ("annapolis", "md", "Annapolis"), ("baltimore", "md", "Baltimore"),
    ("bel air", "md", "Bel Air"), ("bethesda", "md", "Bethesda"),
    ("bowie", "md", "Bowie"), ("columbia", "md", "Columbia"),
    ("elkton", "md", "Elkton"), ("ellicott city", "md", "Ellicott City"),
    ("ellicottcity", "md", "Ellicott City"), ("gaithersburg", "md", "Gaithersburg"),
    ("germantown", "md", "Germantown"), ("hagerstown", "md", "Hagerstown"),
    ("rockville", "md", "Rockville"), ("salisbury", "md", "Salisbury"),
    ("silver spring", "md", "Silver Spring"), ("silverspring", "md", "Silver Spring"),
    ("waldorf", "md", "Waldorf"), ("anne arundel", "md", "Anne Arundel"),
    ("annearundel", "md", "Anne Arundel"), ("calvert county", "md", "Calvert County"),
    ("charles county", "md", "Charles County"), ("charlescounty", "md", "Charles County"),
    ("harford county", "md", "Harford County"), ("harfordcounty", "md", "Harford County"),
    ("howard county", "md", "Howard County"), ("howardcounty", "md", "Howard County"),
    ("montgomery county", "md", "Montgomery County"),
    ("montgomerycounty", "md", "Montgomery County"),
    ("prince georges", "md", "Prince George's County"),
    ("princegeorges", "md", "Prince George's County"),
    ("st marys", "md", "St. Mary's County"),

    # Massachusetts (many already known)
    ("acton", "ma", "Acton"), ("andover", "ma", "Andover"),
    ("attleboro", "ma", "Attleboro"), ("barnstable", "ma", "Barnstable"),
    ("belmont", "ma", "Belmont"), ("billerica", "ma", "Billerica"),
    ("braintree", "ma", "Braintree"), ("brockton", "ma", "Brockton"),
    ("burlington", "ma", "Burlington"), ("chelmsford", "ma", "Chelmsford"),
    ("dartmouth", "ma", "Dartmouth"), ("duxbury", "ma", "Duxbury"),
    ("easton", "ma", "Easton"), ("fitchburg", "ma", "Fitchburg"),
    ("gloucester", "ma", "Gloucester"), ("haverhill", "ma", "Haverhill"),
    ("hopkinton", "ma", "Hopkinton"), ("lowell", "ma", "Lowell"),
    ("lynn", "ma", "Lynn"), ("malden", "ma", "Malden"),
    ("mansfield", "ma", "Mansfield"), ("marblehead", "ma", "Marblehead"),
    ("marlborough", "ma", "Marlborough"), ("medford", "ma", "Medford"),
    ("melrose", "ma", "Melrose"), ("milford", "ma", "Milford"),
    ("millis", "ma", "Millis"), ("millbury", "ma", "Millbury"),
    ("nahant", "ma", "Nahant"), ("nantucket", "ma", "Nantucket"),
    ("norfolk", "ma", "Norfolk"), ("north reading", "ma", "North Reading"),
    ("northreading", "ma", "North Reading"), ("norwood", "ma", "Norwood"),
    ("pembroke", "ma", "Pembroke"), ("pittsfield", "ma", "Pittsfield"),
    ("plymouth", "ma", "Plymouth"), ("quincy", "ma", "Quincy"),
    ("randolph", "ma", "Randolph"), ("reading", "ma", "Reading"),
    ("revere", "ma", "Revere"), ("rockland", "ma", "Rockland"),
    ("scituate", "ma", "Scituate"), ("seekonk", "ma", "Seekonk"),
    ("sharon", "ma", "Sharon"), ("somerville", "ma", "Somerville"),
    ("south boston", "ma", "South Boston"), ("stoughton", "ma", "Stoughton"),
    ("sudbury", "ma", "Sudbury"), ("swampscott", "ma", "Swampscott"),
    ("walpole", "ma", "Walpole"), ("waltham", "ma", "Waltham"),
    ("wellesley", "ma", "Wellesley"), ("westborough", "ma", "Westborough"),
    ("westfield", "ma", "Westfield"), ("westford", "ma", "Westford"),
    ("weston", "ma", "Weston"), ("weymouth", "ma", "Weymouth"),
    ("woburn", "ma", "Woburn"), ("wrentham", "ma", "Wrentham"),
    ("yarmouth", "ma", "Yarmouth"),

    # Michigan
    ("ann arbor", "mi", "Ann Arbor"), ("annarbor", "mi", "Ann Arbor"),
    ("battle creek", "mi", "Battle Creek"), ("battlecreek", "mi", "Battle Creek"),
    ("bay city", "mi", "Bay City"), ("bayci", "mi", "Bay City"),
    ("canton", "mi", "Canton"), ("dearborn", "mi", "Dearborn"),
    ("detroit", "mi", "Detroit"), ("eastlansing", "mi", "East Lansing"),
    ("flint", "mi", "Flint"), ("grand rapids", "mi", "Grand Rapids"),
    ("grandrapids", "mi", "Grand Rapids"), ("kalamazoo", "mi", "Kalamazoo"),
    ("lansing", "mi", "Lansing"), ("lincoln park", "mi", "Lincoln Park"),
    ("livonia", "mi", "Livonia"), ("macomb", "mi", "Macomb"),
    ("midland", "mi", "Midland"), ("muskegon", "mi", "Muskegon"),
    ("novi", "mi", "Novi"), ("pontiac", "mi", "Pontiac"),
    ("port huron", "mi", "Port Huron"), ("porthuron", "mi", "Port Huron"),
    ("rochester hills", "mi", "Rochester Hills"),
    ("royal oak", "mi", "Royal Oak"), ("royaloak", "mi", "Royal Oak"),
    ("saginaw", "mi", "Saginaw"), ("saint clair shores", "mi", "St. Clair Shores"),
    ("southfield", "mi", "Southfield"), ("sterling heights", "mi", "Sterling Heights"),
    ("sterlingheights", "mi", "Sterling Heights"),
    ("taylor", "mi", "Taylor"), ("troy", "mi", "Troy"),
    ("warren", "mi", "Warren"), ("westland", "mi", "Westland"),
    ("wyoming", "mi", "Wyoming"), ("ypsilanti", "mi", "Ypsilanti"),
    ("kent county", "mi", "Kent County"), ("kentcounty", "mi", "Kent County"),
    ("macomb county", "mi", "Macomb County"), ("macombcounty", "mi", "Macomb County"),
    ("oakland county", "mi", "Oakland County"), ("oaklandcounty", "mi", "Oakland County"),
    ("wayne county", "mi", "Wayne County"), ("waynecounty", "mi", "Wayne County"),
    ("washtenaw", "mi", "Washtenaw"),

    # Minnesota
    ("apple valley", "mn", "Apple Valley"), ("applevalley", "mn", "Apple Valley"),
    ("bloomington", "mn", "Bloomington"), ("brooklyn center", "mn", "Brooklyn Center"),
    ("brooklyn park", "mn", "Brooklyn Park"), ("brooklynpark", "mn", "Brooklyn Park"),
    ("burnsville", "mn", "Burnsville"), ("coon rapids", "mn", "Coon Rapids"),
    ("coonrapids", "mn", "Coon Rapids"), ("duluth", "mn", "Duluth"),
    ("eagan", "mn", "Eagan"), ("eden prairie", "mn", "Eden Prairie"),
    ("edenprairie", "mn", "Eden Prairie"), ("edina", "mn", "Edina"),
    ("lakeville", "mn", "Lakeville"), ("mankato", "mn", "Mankato"),
    ("maple grove", "mn", "Maple Grove"), ("maplegrove", "mn", "Maple Grove"),
    ("minneapolis", "mn", "Minneapolis"), ("minnetonka", "mn", "Minnetonka"),
    ("moorhead", "mn", "Moorhead"), ("plymouth", "mn", "Plymouth"),
    ("richfield", "mn", "Richfield"), ("rochester", "mn", "Rochester"),
    ("roseville", "mn", "Roseville"), ("saint cloud", "mn", "Saint Cloud"),
    ("saintcloud", "mn", "Saint Cloud"), ("saint paul", "mn", "Saint Paul"),
    ("saintpaul", "mn", "Saint Paul"), ("woodbury", "mn", "Woodbury"),
    ("anoka county", "mn", "Anoka County"), ("anokacounty", "mn", "Anoka County"),
    ("dakota county", "mn", "Dakota County"), ("dakotacounty", "mn", "Dakota County"),
    ("hennepin", "mn", "Hennepin"), ("hennepincounty", "mn", "Hennepin County"),
    ("ramsey county", "mn", "Ramsey County"), ("ramseycounty", "mn", "Ramsey County"),
    ("scott county", "mn", "Scott County"), ("scottcounty", "mn", "Scott County"),
    ("washington county", "mn", "Washington County"),
    ("washingtoncountymn", "mn", "Washington County"),

    # Mississippi
    ("biloxi", "ms", "Biloxi"), ("brandon", "ms", "Brandon"),
    ("clinton", "ms", "Clinton"), ("gulfport", "ms", "Gulfport"),
    ("hattiesburg", "ms", "Hattiesburg"), ("horn lake", "ms", "Horn Lake"),
    ("meridian", "ms", "Meridian"), ("northbay", "ms", "Northbay"),
    ("olive branch", "ms", "Olive Branch"), ("olivebranch", "ms", "Olive Branch"),
    ("oxford", "ms", "Oxford"), ("pearl", "ms", "Pearl"),
    ("ridgeland", "ms", "Ridgeland"), ("southaven", "ms", "Southaven"),
    ("starkville", "ms", "Starkville"), ("tupelo", "ms", "Tupelo"),
    ("vicksburg", "ms", "Vicksburg"),

    # Missouri
    ("blue springs", "mo", "Blue Springs"), ("bluesprings", "mo", "Blue Springs"),
    ("cape girardeau", "mo", "Cape Girardeau"), ("capegirardeau", "mo", "Cape Girardeau"),
    ("chesterfield", "mo", "Chesterfield"), ("columbia", "mo", "Columbia"),
    ("florissant", "mo", "Florissant"), ("independence", "mo", "Independence"),
    ("jefferson city", "mo", "Jefferson City"), ("jeffersoncity", "mo", "Jefferson City"),
    ("joplin", "mo", "Joplin"), ("kansas city", "mo", "Kansas City"),
    ("kansascitymo", "mo", "Kansas City"), ("lees summit", "mo", "Lee's Summit"),
    ("leessummit", "mo", "Lee's Summit"), ("ofallon", "mo", "O'Fallon"),
    ("saint charles", "mo", "Saint Charles"), ("saintcharles", "mo", "Saint Charles"),
    ("saint joseph", "mo", "Saint Joseph"), ("saintjoseph", "mo", "Saint Joseph"),
    ("saint louis", "mo", "Saint Louis"), ("saintlouis", "mo", "Saint Louis"),
    ("springfield", "mo", "Springfield"), ("st charles", "mo", "St. Charles"),
    ("st louis", "mo", "St. Louis"), ("stlouis", "mo", "St. Louis"),
    ("stlouiscounty", "mo", "St. Louis County"),

    # Montana
    ("billings", "mt", "Billings"), ("bozeman", "mt", "Bozeman"),
    ("butte", "mt", "Butte"), ("great falls", "mt", "Great Falls"),
    ("greatfalls", "mt", "Great Falls"), ("helena", "mt", "Helena"),
    ("kalispell", "mt", "Kalispell"), ("missoula", "mt", "Missoula"),

    # Nebraska
    ("bellevue", "ne", "Bellevue"), ("fremont", "ne", "Fremont"),
    ("grand island", "ne", "Grand Island"), ("grandisland", "ne", "Grand Island"),
    ("kearney", "ne", "Kearney"), ("lincoln", "ne", "Lincoln"),
    ("norfolk", "ne", "Norfolk"), ("north platte", "ne", "North Platte"),
    ("northplatte", "ne", "North Platte"), ("omaha", "ne", "Omaha"),
    ("papillion", "ne", "Papillion"),

    # Nevada
    ("carson city", "nv", "Carson City"), ("carsoncity", "nv", "Carson City"),
    ("henderson", "nv", "Henderson"), ("las vegas", "nv", "Las Vegas"),
    ("lasvegas", "nv", "Las Vegas"), ("north las vegas", "nv", "North Las Vegas"),
    ("northlasvegas", "nv", "North Las Vegas"), ("reno", "nv", "Reno"),
    ("sparks", "nv", "Sparks"), ("spring valley", "nv", "Spring Valley"),
    ("summerlin", "nv", "Summerlin"), ("sunrise manor", "nv", "Sunrise Manor"),
    ("clark county", "nv", "Clark County"), ("clarkcounty", "nv", "Clark County"),
    ("washoecounty", "nv", "Washoe County"),

    # New Hampshire
    ("concord", "nh", "Concord"), ("derry", "nh", "Derry"),
    ("dover", "nh", "Dover"), ("keene", "nh", "Keene"),
    ("laconia", "nh", "Laconia"), ("manchester", "nh", "Manchester"),
    ("nashua", "nh", "Nashua"), ("portsmouth", "nh", "Portsmouth"),
    ("rochester", "nh", "Rochester"), ("salem", "nh", "Salem"),

    # New Jersey
    ("bayonne", "nj", "Bayonne"), ("camden", "nj", "Camden"),
    ("cherry hill", "nj", "Cherry Hill"), ("cherryhill", "nj", "Cherry Hill"),
    ("clifton", "nj", "Clifton"), ("east orange", "nj", "East Orange"),
    ("eastorange", "nj", "East Orange"), ("edison", "nj", "Edison"),
    ("elizabeth", "nj", "Elizabeth"), ("hackensack", "nj", "Hackensack"),
    ("irvington", "nj", "Irvington"), ("jersey city", "nj", "Jersey City"),
    ("jerseycity", "nj", "Jersey City"), ("lakewood", "nj", "Lakewood"),
    ("livingston", "nj", "Livingston"), ("linden", "nj", "Linden"),
    ("long branch", "nj", "Long Branch"), ("longbranch", "nj", "Long Branch"),
    ("millburn", "nj", "Millburn"), ("montclair", "nj", "Montclair"),
    ("newark", "nj", "Newark"), ("new brunswick", "nj", "New Brunswick"),
    ("newbrunswick", "nj", "New Brunswick"), ("northbay", "nj", "North Bay"),
    ("parsippany", "nj", "Parsippany"), ("passaic", "nj", "Passaic"),
    ("paterson", "nj", "Paterson"), ("perth amboy", "nj", "Perth Amboy"),
    ("perthamboy", "nj", "Perth Amboy"), ("piscataway", "nj", "Piscataway"),
    ("plainfield", "nj", "Plainfield"), ("princeton", "nj", "Princeton"),
    ("rahway", "nj", "Rahway"), ("south orange", "nj", "South Orange"),
    ("toms river", "nj", "Toms River"), ("tomsriver", "nj", "Toms River"),
    ("trenton", "nj", "Trenton"), ("union city", "nj", "Union City"),
    ("unioncity", "nj", "Union City"), ("vineland", "nj", "Vineland"),
    ("wayne", "nj", "Wayne"), ("west new york", "nj", "West New York"),
    ("burlington county", "nj", "Burlington County"),
    ("burlingtoncountynj", "nj", "Burlington County"),
    ("essex county", "nj", "Essex County"), ("essexcounty", "nj", "Essex County"),
    ("gloucester county", "nj", "Gloucester County"),
    ("glousestercounty", "nj", "Gloucester County"),
    ("hudson county", "nj", "Hudson County"), ("hudsoncounty", "nj", "Hudson County"),
    ("mercer county", "nj", "Mercer County"), ("mercercounty", "nj", "Mercer County"),
    ("middlesex county", "nj", "Middlesex County"),
    ("middlesexcounty", "nj", "Middlesex County"),
    ("monmouth county", "nj", "Monmouth County"),
    ("monmouthcounty", "nj", "Monmouth County"),
    ("morris county", "nj", "Morris County"), ("morriscounty", "nj", "Morris County"),
    ("ocean county", "nj", "Ocean County"), ("oceancounty", "nj", "Ocean County"),
    ("passaic county", "nj", "Passaic County"), ("passaiccounty", "nj", "Passaic County"),
    ("somerset county", "nj", "Somerset County"),
    ("somersetcounty", "nj", "Somerset County"),
    ("sussex county", "nj", "Sussex County"), ("sussexcounty", "nj", "Sussex County"),
    ("union county", "nj", "Union County"), ("unioncountynj", "nj", "Union County"),
    ("warren county", "nj", "Warren County"), ("warrencountynj", "nj", "Warren County"),

    # New Mexico
    ("albuquerque", "nm", "Albuquerque"), ("clovis", "nm", "Clovis"),
    ("farmington", "nm", "Farmington"), ("hobbs", "nm", "Hobbs"),
    ("las cruces", "nm", "Las Cruces"), ("lascruces", "nm", "Las Cruces"),
    ("rio rancho", "nm", "Rio Rancho"), ("riorancho", "nm", "Rio Rancho"),
    ("roswell", "nm", "Roswell"), ("santa fe", "nm", "Santa Fe"),
    ("santafenm", "nm", "Santa Fe"),

    # New York
    ("albany", "ny", "Albany"), ("amherst", "ny", "Amherst"),
    ("brentwood", "ny", "Brentwood"), ("brooklyn", "ny", "Brooklyn"),
    ("buffalo", "ny", "Buffalo"), ("freeport", "ny", "Freeport"),
    ("manhattan", "ny", "Manhattan"), ("new rochelle", "ny", "New Rochelle"),
    ("newrochelle", "ny", "New Rochelle"), ("new york", "ny", "New York"),
    ("newyork", "ny", "New York"), ("newyor", "ny", "New York City"),
    ("niagara falls", "ny", "Niagara Falls"), ("niagarafalls", "ny", "Niagara Falls"),
    ("ramapo", "ny", "Ramapo"), ("rochester", "ny", "Rochester"),
    ("rome", "ny", "Rome"), ("smithtown", "ny", "Smithtown"),
    ("staten island", "ny", "Staten Island"), ("statenisland", "ny", "Staten Island"),
    ("syracuse", "ny", "Syracuse"), ("troy", "ny", "Troy"),
    ("utica", "ny", "Utica"), ("white plains", "ny", "White Plains"),
    ("whiteplains", "ny", "White Plains"), ("yonkers", "ny", "Yonkers"),
    ("bronx", "ny", "The Bronx"), ("buffalo", "ny", "Buffalo"),
    ("nassau county", "ny", "Nassau County"), ("nassaucounty", "ny", "Nassau County"),
    ("orange county", "ny", "Orange County"), ("orangecountyny", "ny", "Orange County"),
    ("putnam county", "ny", "Putnam County"), ("putnamcounty", "ny", "Putnam County"),
    ("rockland county", "ny", "Rockland County"), ("rocklandcounty", "ny", "Rockland County"),
    ("suffolk county", "ny", "Suffolk County"), ("suffolkcounty", "ny", "Suffolk County"),
    ("ulster county", "ny", "Ulster County"), ("ulstercounty", "ny", "Ulster County"),
    ("westchester county", "ny", "Westchester County"),
    ("westchestercounty", "ny", "Westchester County"),

    # North Carolina
    ("asheville", "nc", "Asheville"), ("burlington", "nc", "Burlington"),
    ("cary", "nc", "Cary"), ("charlotte", "nc", "Charlotte"),
    ("concord", "nc", "Concord"), ("durham", "nc", "Durham"),
    ("fayetteville", "nc", "Fayetteville"), ("gastonia", "nc", "Gastonia"),
    ("greensboro", "nc", "Greensboro"), ("greenville", "nc", "Greenville"),
    ("hickory", "nc", "Hickory"), ("high point", "nc", "High Point"),
    ("highpoint", "nc", "High Point"), ("huntersville", "nc", "Huntersville"),
    ("jacksonville", "nc", "Jacksonville"), ("kannapolis", "nc", "Kannapolis"),
    ("morrisville", "nc", "Morrisville"), ("raleigh", "nc", "Raleigh"),
    ("rocky mount", "nc", "Rocky Mount"), ("rockymount", "nc", "Rocky Mount"),
    ("wake forest", "nc", "Wake Forest"), ("wakeforest", "nc", "Wake Forest"),
    ("wilmington", "nc", "Wilmington"), ("wilson", "nc", "Wilson"),
    ("winston salem", "nc", "Winston-Salem"), ("winstonsalem", "nc", "Winston-Salem"),
    ("cabarrus county", "nc", "Cabarrus County"),
    ("cabarruscounty", "nc", "Cabarrus County"),
    ("catawba", "nc", "Catawba"), ("catawbacounty", "nc", "Catawba County"),
    ("davidson county", "nc", "Davidson County"), ("davidsoncountync", "nc", "Davidson County"),
    ("forsyth county", "nc", "Forsyth County"), ("forsythcountync", "nc", "Forsyth County"),
    ("guilford county", "nc", "Guilford County"), ("guilfordcounty", "nc", "Guilford County"),
    ("henderson county", "nc", "Henderson County"),
    ("hendersoncounty", "nc", "Henderson County"),
    ("iredell county", "nc", "Iredell County"), ("iredellcounty", "nc", "Iredell County"),
    ("johnston county", "nc", "Johnston County"), ("johnstoncounty", "nc", "Johnston County"),
    ("mecklenburg county", "nc", "Mecklenburg County"),
    ("mecklenburgcounty", "nc", "Mecklenburg County"),
    ("new hanover county", "nc", "New Hanover County"),
    ("newhanovercounty", "nc", "New Hanover County"),
    ("orange county", "nc", "Orange County"), ("orangecountync", "nc", "Orange County"),
    ("union county", "nc", "Union County"), ("unioncountync", "nc", "Union County"),
    ("wake county", "nc", "Wake County"), ("wakecounty", "nc", "Wake County"),

    # North Dakota
    ("bismarck", "nd", "Bismarck"), ("fargo", "nd", "Fargo"),
    ("grand forks", "nd", "Grand Forks"), ("grandforks", "nd", "Grand Forks"),
    ("minot", "nd", "Minot"),

    # Ohio
    ("akron", "oh", "Akron"), ("barberton", "oh", "Barberton"),
    ("beavercreek", "oh", "Beaver Creek"), ("cambridge", "oh", "Cambridge"),
    ("canton", "oh", "Canton"), ("cincinnati", "oh", "Cincinnati"),
    ("cleveland", "oh", "Cleveland"), ("cleveland heights", "oh", "Cleveland Heights"),
    ("clevelandheights", "oh", "Cleveland Heights"),
    ("columbus", "oh", "Columbus"), ("cuyahoga falls", "oh", "Cuyahoga Falls"),
    ("cuyahogafalls", "oh", "Cuyahoga Falls"), ("dayton", "oh", "Dayton"),
    ("elyria", "oh", "Elyria"), ("euclid", "oh", "Euclid"),
    ("grove city", "oh", "Grove City"), ("grovecity", "oh", "Grove City"),
    ("hamilton", "oh", "Hamilton"), ("kettering", "oh", "Kettering"),
    ("lakewood", "oh", "Lakewood"), ("lancaster", "oh", "Lancaster"),
    ("lima", "oh", "Lima"), ("lorain", "oh", "Lorain"),
    ("mansfield", "oh", "Mansfield"), ("mason", "oh", "Mason"),
    ("medina", "oh", "Medina"), ("mentor", "oh", "Mentor"),
    ("middletown", "oh", "Middletown"), ("newark", "oh", "Newark"),
    ("north olmsted", "oh", "North Olmsted"), ("northolmsted", "oh", "North Olmsted"),
    ("norwood", "oh", "Norwood"), ("parma", "oh", "Parma"),
    ("rocky river", "oh", "Rocky River"), ("rocky river oh", "oh", "Rocky River"),
    ("sandusky", "oh", "Sandusky"), ("shaker heights", "oh", "Shaker Heights"),
    ("shakerheights", "oh", "Shaker Heights"),
    ("springfield", "oh", "Springfield"), ("stow", "oh", "Stow"),
    ("strongsville", "oh", "Strongsville"), ("toledo", "oh", "Toledo"),
    ("warren", "oh", "Warren"), ("westerville", "oh", "Westerville"),
    ("youngstown", "oh", "Youngstown"), ("zanesville", "oh", "Zanesville"),
    ("cuyahoga county", "oh", "Cuyahoga County"), ("cuyahogacounty", "oh", "Cuyahoga County"),
    ("delaware county", "oh", "Delaware County"), ("delawarecounty", "oh", "Delaware County"),
    ("fairfield county", "oh", "Fairfield County"), ("fairfieldcounty", "oh", "Fairfield County"),
    ("franklin county", "oh", "Franklin County"), ("franklincounty", "oh", "Franklin County"),
    ("hamilton county", "oh", "Hamilton County"), ("hamiltoncountyoh", "oh", "Hamilton County"),
    ("lake county", "oh", "Lake County"), ("lakecountyoh", "oh", "Lake County"),
    ("licking county", "oh", "Licking County"), ("lickingcounty", "oh", "Licking County"),
    ("lorain county", "oh", "Lorain County"), ("loraincounty", "oh", "Lorain County"),
    ("lucas county", "oh", "Lucas County"), ("lucascounty", "oh", "Lucas County"),
    ("mahoning county", "oh", "Mahoning County"), ("mahoningcounty", "oh", "Mahoning County"),
    ("medina county", "oh", "Medina County"), ("medinacounty", "oh", "Medina County"),
    ("montgomery county", "oh", "Montgomery County"),
    ("montgomerycountyoh", "oh", "Montgomery County"),
    ("portage county", "oh", "Portage County"),
    ("stark county", "oh", "Stark County"), ("starkcounty", "oh", "Stark County"),
    ("summit county", "oh", "Summit County"), ("summitcounty", "oh", "Summit County"),
    ("trumbull county", "oh", "Trumbull County"), ("trumbullcounty", "oh", "Trumbull County"),
    ("tuscarawas", "oh", "Tuscarawas"),
    ("wayne county", "oh", "Wayne County"), ("waynecountyoh", "oh", "Wayne County"),
    ("wood county", "oh", "Wood County"), ("woodcounty", "oh", "Wood County"),

    # Oklahoma
    ("broken arrow", "ok", "Broken Arrow"), ("brokenarrow", "ok", "Broken Arrow"),
    ("edmond", "ok", "Edmond"), ("enid", "ok", "Enid"),
    ("lawton", "ok", "Lawton"), ("midwest city", "ok", "Midwest City"),
    ("midwestcity", "ok", "Midwest City"), ("moore", "ok", "Moore"),
    ("muskogee", "ok", "Muskogee"), ("normanok", "ok", "Norman"),
    ("oklahoma city", "ok", "Oklahoma City"), ("oklahomacity", "ok", "Oklahoma City"),
    ("owasso", "ok", "Owasso"), ("stillwater", "ok", "Stillwater"),
    ("tulsa", "ok", "Tulsa"), ("yukon", "ok", "Yukon"),
    ("cleveland county", "ok", "Cleveland County"),
    ("clevelandcountyok", "ok", "Cleveland County"),
    ("oklahoma county", "ok", "Oklahoma County"),
    ("oklahomacounty", "ok", "Oklahoma County"),
    ("tulsa county", "ok", "Tulsa County"), ("tulsacounty", "ok", "Tulsa County"),

    # Oregon
    ("albany", "or", "Albany"), ("beaverton", "or", "Beaverton"),
    ("bend", "or", "Bend"), ("corvallis", "or", "Corvallis"),
    ("eugene", "or", "Eugene"), ("gresham", "or", "Gresham"),
    ("hillsboro", "or", "Hillsboro"), ("lake oswego", "or", "Lake Oswego"),
    ("lakeoswego", "or", "Lake Oswego"), ("medford", "or", "Medford"),
    ("portland", "or", "Portland"), ("salem", "or", "Salem"),
    ("springfield", "or", "Springfield"), ("tigard", "or", "Tigard"),
    ("clackamas county", "or", "Clackamas County"),
    ("clackamascounty", "or", "Clackamas County"),
    ("lane county", "or", "Lane County"), ("lanecounty", "or", "Lane County"),
    ("linn county", "or", "Linn County"), ("linncounty", "or", "Linn County"),
    ("marion county", "or", "Marion County"), ("marioncountyor", "or", "Marion County"),
    ("multnomah", "or", "Multnomah"), ("multnomaah", "or", "Multnomah"),
    ("washington county", "or", "Washington County"),
    ("washingtoncountyor", "or", "Washington County"),

    # Pennsylvania
    ("allentown", "pa", "Allentown"), ("altoona", "pa", "Altoona"),
    ("bethlehem", "pa", "Bethlehem"), ("erie", "pa", "Erie"),
    ("harrisburg", "pa", "Harrisburg"), ("lancaster", "pa", "Lancaster"),
    ("philadelphia", "pa", "Philadelphia"), ("pittsburgh", "pa", "Pittsburgh"),
    ("reading", "pa", "Reading"), ("scranton", "pa", "Scranton"),
    ("wilkes barre", "pa", "Wilkes-Barre"), ("wilkesbarre", "pa", "Wilkes-Barre"),
    ("york", "pa", "York"), ("allegheny county", "pa", "Allegheny County"),
    ("alleghenycounty", "pa", "Allegheny County"),
    ("bucks county", "pa", "Bucks County"), ("buckscounty", "pa", "Bucks County"),
    ("chester county", "pa", "Chester County"), ("chestercounty", "pa", "Chester County"),
    ("cumberland county", "pa", "Cumberland County"),
    ("cumberlandcounty", "pa", "Cumberland County"),
    ("dauphin county", "pa", "Dauphin County"), ("dauphincounty", "pa", "Dauphin County"),
    ("delaware county", "pa", "Delaware County"), ("delawarecountypa", "pa", "Delaware County"),
    ("lancaster county", "pa", "Lancaster County"),
    ("lancastercounty", "pa", "Lancaster County"),
    ("lehigh county", "pa", "Lehigh County"), ("lehighcounty", "pa", "Lehigh County"),
    ("luzerne county", "pa", "Luzerne County"), ("luzernecounty", "pa", "Luzerne County"),
    ("lycoming county", "pa", "Lycoming County"), ("lycomingcounty", "pa", "Lycoming County"),
    ("monroe county", "pa", "Monroe County"), ("monroecountypa", "pa", "Monroe County"),
    ("montgomery county", "pa", "Montgomery County"),
    ("montgomerycountypa", "pa", "Montgomery County"),
    ("northampton county", "pa", "Northampton County"),
    ("northamptoncounty", "pa", "Northampton County"),
    ("westmoreland county", "pa", "Westmoreland County"),
    ("westmorelandcounty", "pa", "Westmoreland County"),
    ("york county", "pa", "York County"), ("yorkcounty", "pa", "York County"),

    # Rhode Island (many already known)
    ("barrington", "ri", "Barrington"), ("bristol", "ri", "Bristol"),
    ("central falls", "ri", "Central Falls"), ("centralfalls", "ri", "Central Falls"),
    ("cranston", "ri", "Cranston"), ("cumberland", "ri", "Cumberland"),
    ("east greenwich", "ri", "East Greenwich"), ("east providence", "ri", "East Providence"),
    ("foster", "ri", "Foster"), ("lincoln", "ri", "Lincoln"),
    ("north kingstown", "ri", "North Kingstown"), ("north providence", "ri", "North Providence"),
    ("north smithfield", "ri", "North Smithfield"),
    ("northsmithfield", "ri", "North Smithfield"),
    ("pawtucket", "ri", "Pawtucket"), ("portsmouth", "ri", "Portsmouth"),
    ("providence", "ri", "Providence"), ("richmond", "ri", "Richmond"),
    ("south kingstown", "ri", "South Kingstown"),
    ("tiverton", "ri", "Tiverton"), ("warren", "ri", "Warren"),
    ("warwick", "ri", "Warwick"), ("west greenwich", "ri", "West Greenwich"),
    ("westgreenwich", "ri", "West Greenwich"),
    ("west warwick", "ri", "West Warwick"), ("woonsocket", "ri", "Woonsocket"),

    # South Carolina
    ("charleston", "sc", "Charleston"), ("columbia", "sc", "Columbia"),
    ("florence", "sc", "Florence"), ("greenville", "sc", "Greenville"),
    ("greenwood", "sc", "Greenwood"), ("hilton head", "sc", "Hilton Head Island"),
    ("hiltonhead", "sc", "Hilton Head Island"), ("myrtle beach", "sc", "Myrtle Beach"),
    ("myrtlebeach", "sc", "Myrtle Beach"), ("north charleston", "sc", "North Charleston"),
    ("northcharleston", "sc", "North Charleston"), ("rock hill", "sc", "Rock Hill"),
    ("rockhill", "sc", "Rock Hill"), ("spartanburg", "sc", "Spartanburg"),
    ("summerville", "sc", "Summerville"), ("sumter", "sc", "Sumter"),
    ("york county", "sc", "York County"), ("yorkcountysc", "sc", "York County"),
    ("horry county", "sc", "Horry County"), ("horrycounty", "sc", "Horry County"),
    ("berkeley county", "sc", "Berkeley County"),
    ("berkeleycounty", "sc", "Berkeley County"),
    ("dorchester county", "sc", "Dorchester County"),
    ("dorchestercountysc", "sc", "Dorchester County"),
    ("greenville county", "sc", "Greenville County"),
    ("greenvillecounty", "sc", "Greenville County"),
    ("lexington county", "sc", "Lexington County"),
    ("lexingtoncounty", "sc", "Lexington County"),
    ("richland county", "sc", "Richland County"),
    ("richlandcounty", "sc", "Richland County"),

    # South Dakota
    ("aberdeen", "sd", "Aberdeen"), ("brookings", "sd", "Brookings"),
    ("rapid city", "sd", "Rapid City"), ("rapidcity", "sd", "Rapid City"),
    ("sioux falls", "sd", "Sioux Falls"), ("siouxfalls", "sd", "Sioux Falls"),
    ("watertown", "sd", "Watertown"),

    # Tennessee
    ("bartlett", "tn", "Bartlett"), ("brentwood", "tn", "Brentwood"),
    ("chattanooga", "tn", "Chattanooga"), ("clarksville", "tn", "Clarksville"),
    ("collierville", "tn", "Collierville"), ("columbia", "tn", "Columbia"),
    ("franklin", "tn", "Franklin"), ("germantown", "tn", "Germantown"),
    ("hendersonville", "tn", "Hendersonville"), ("jackson", "tn", "Jackson"),
    ("johnson city", "tn", "Johnson City"), ("johnsoncitytn", "tn", "Johnson City"),
    ("kingsport", "tn", "Kingsport"), ("knoxville", "tn", "Knoxville"),
    ("memphis", "tn", "Memphis"), ("morristown", "tn", "Morristown"),
    ("murfreesboro", "tn", "Murfreesboro"), ("nashville", "tn", "Nashville"),
    ("smyrna", "tn", "Smyrna"), ("spring hill", "tn", "Spring Hill"),
    ("davidson county", "tn", "Davidson County"),
    ("hamiltoncountytn", "tn", "Hamilton County"),
    ("knoxcounty", "tn", "Knox County"), ("rutherford county", "tn", "Rutherford County"),
    ("rutherfordcounty", "tn", "Rutherford County"),
    ("shelby county", "tn", "Shelby County"), ("shelbycounty", "tn", "Shelby County"),
    ("sullivan county", "tn", "Sullivan County"), ("sullivancounty", "tn", "Sullivan County"),
    ("williamson county", "tn", "Williamson County"),
    ("williamsoncounty", "tn", "Williamson County"),
    ("wilson county", "tn", "Wilson County"),

    # Texas
    ("abilene", "tx", "Abilene"), ("allen", "tx", "Allen"),
    ("amarillo", "tx", "Amarillo"), ("arlington", "tx", "Arlington"),
    ("austin", "tx", "Austin"), ("baytown", "tx", "Baytown"),
    ("beaumont", "tx", "Beaumont"), ("brownsville", "tx", "Brownsville"),
    ("burleson", "tx", "Burleson"), ("carrollton", "tx", "Carrollton"),
    ("cedar hill", "tx", "Cedar Hill"), ("cedarhill", "tx", "Cedar Hill"),
    ("cedar park", "tx", "Cedar Park"), ("cedarpark", "tx", "Cedar Park"),
    ("college station", "tx", "College Station"),
    ("collegestation", "tx", "College Station"),
    ("conroe", "tx", "Conroe"), ("corpus christi", "tx", "Corpus Christi"),
    ("corpuschristi", "tx", "Corpus Christi"), ("dallas", "tx", "Dallas"),
    ("denton", "tx", "Denton"), ("desoto", "tx", "DeSoto"),
    ("edinburg", "tx", "Edinburg"), ("el paso", "tx", "El Paso"),
    ("elpasotx", "tx", "El Paso"), ("euless", "tx", "Euless"),
    ("flower mound", "tx", "Flower Mound"), ("flowermound", "tx", "Flower Mound"),
    ("fort worth", "tx", "Fort Worth"), ("fortworth", "tx", "Fort Worth"),
    ("frisco", "tx", "Frisco"), ("garland", "tx", "Garland"),
    ("grand prairie", "tx", "Grand Prairie"), ("grandprairie", "tx", "Grand Prairie"),
    ("grapevine", "tx", "Grapevine"), ("harlingen", "tx", "Harlingen"),
    ("houston", "tx", "Houston"), ("huntsville", "tx", "Huntsville"),
    ("hurst", "tx", "Hurst"), ("irving", "tx", "Irving"),
    ("killeen", "tx", "Killeen"), ("kyle", "tx", "Kyle"),
    ("laredo", "tx", "Laredo"), ("league city", "tx", "League City"),
    ("leaguecity", "tx", "League City"), ("lewisville", "tx", "Lewisville"),
    ("longview", "tx", "Longview"), ("lubbock", "tx", "Lubbock"),
    ("mcallen", "tx", "McAllen"), ("mckinney", "tx", "McKinney"),
    ("mesquite", "tx", "Mesquite"), ("midland", "tx", "Midland"),
    ("mission", "tx", "Mission"), ("missouri city", "tx", "Missouri City"),
    ("missouricity", "tx", "Missouri City"), ("new braunfels", "tx", "New Braunfels"),
    ("newbraunfels", "tx", "New Braunfels"), ("north richland hills", "tx", "North Richland Hills"),
    ("northrichlandhills", "tx", "North Richland Hills"),
    ("odessa", "tx", "Odessa"), ("pasadena", "tx", "Pasadena"),
    ("pearland", "tx", "Pearland"), ("pflugerville", "tx", "Pflugerville"),
    ("plano", "tx", "Plano"), ("port arthur", "tx", "Port Arthur"),
    ("portarthur", "tx", "Port Arthur"), ("richardson", "tx", "Richardson"),
    ("round rock", "tx", "Round Rock"), ("roundrock", "tx", "Round Rock"),
    ("rowlett", "tx", "Rowlett"), ("san angelo", "tx", "San Angelo"),
    ("sanangelo", "tx", "San Antonio"), ("san antonio", "tx", "San Antonio"),
    ("sanantonio", "tx", "San Antonio"), ("san marcos", "tx", "San Marcos"),
    ("sanmarcos", "tx", "San Marcos"), ("sugar land", "tx", "Sugar Land"),
    ("sugarland", "tx", "Sugar Land"), ("temple", "tx", "Temple"),
    ("texarkana", "tx", "Texarkana"), ("texas city", "tx", "Texas City"),
    ("texascity", "tx", "Texas City"), ("tyler", "tx", "Tyler"),
    ("waco", "tx", "Waco"), ("wichita falls", "tx", "Wichita Falls"),
    ("wichitafalls", "tx", "Wichita Falls"), ("wylie", "tx", "Wylie"),
    ("bexar county", "tx", "Bexar County"),
    ("brazoria county", "tx", "Brazoria County"),
    ("brazoriacounty", "tx", "Brazoria County"),
    ("collin county", "tx", "Collin County"), ("collincounty", "tx", "Collin County"),
    ("dallas county", "tx", "Dallas County"), ("dallascounty", "tx", "Dallas County"),
    ("denton county", "tx", "Denton County"), ("dentoncounty", "tx", "Denton County"),
    ("el paso county", "tx", "El Paso County"), ("elpasocountytx", "tx", "El Paso County"),
    ("fort bend county", "tx", "Fort Bend County"),
    ("fortbendcounty", "tx", "Fort Bend County"),
    ("galveston county", "tx", "Galveston County"),
    ("harris county", "tx", "Harris County"), ("harriscounty", "tx", "Harris County"),
    ("hays county", "tx", "Hays County"), ("hayscounty", "tx", "Hays County"),
    ("hidalgo county", "tx", "Hidalgo County"), ("hidalgocounty", "tx", "Hidalgo County"),
    ("montgomery county", "tx", "Montgomery County"),
    ("montgomerycountytx", "tx", "Montgomery County"),
    ("nueces county", "tx", "Nueces County"), ("nuevescounty", "tx", "Nueces County"),
    ("tarrant county", "tx", "Tarrant County"), ("tarrantcounty", "tx", "Tarrant County"),
    ("travis county", "tx", "Travis County"), ("traviscounty", "tx", "Travis County"),
    ("williamson county", "tx", "Williamson County"),
    ("williamsoncountytx", "tx", "Williamson County"),

    # Utah
    ("american fork", "ut", "American Fork"), ("americanfork", "ut", "American Fork"),
    ("herriman", "ut", "Herriman"), ("layton", "ut", "Layton"),
    ("lehi", "ut", "Lehi"), ("logan", "ut", "Logan"),
    ("midvale", "ut", "Midvale"), ("millcreek", "ut", "Millcreek"),
    ("murray", "ut", "Murray"), ("ogden", "ut", "Ogden"),
    ("orem", "ut", "Orem"), ("provo", "ut", "Provo"),
    ("riverton", "ut", "Riverton"), ("st george", "ut", "St. George"),
    ("stgeorge", "ut", "St. George"), ("salt lake", "ut", "Salt Lake"),
    ("saltlake", "ut", "Salt Lake City"), ("salt lake city", "ut", "Salt Lake City"),
    ("saltlakecity", "ut", "Salt Lake City"),
    ("sandy", "ut", "Sandy"), ("south jordan", "ut", "South Jordan"),
    ("southjordan", "ut", "South Jordan"), ("spanish fork", "ut", "Spanish Fork"),
    ("spanishfork", "ut", "Spanish Fork"), ("springville", "ut", "Springville"),
    ("taylorsville", "ut", "Taylorsville"), ("west jordan", "ut", "West Jordan"),
    ("westjordan", "ut", "West Jordan"), ("west valley", "ut", "West Valley City"),
    ("westvalley", "ut", "West Valley City"),
    ("davis county", "ut", "Davis County"), ("daviscounty", "ut", "Davis County"),
    ("salt lake county", "ut", "Salt Lake County"),
    ("saltlakecounty", "ut", "Salt Lake County"),
    ("utah county", "ut", "Utah County"), ("utahcounty", "ut", "Utah County"),
    ("weber county", "ut", "Weber County"), ("webercounty", "ut", "Weber County"),

    # Vermont
    ("brattleboro", "vt", "Brattleboro"), ("burlington", "vt", "Burlington"),
    ("essex junction", "vt", "Essex Junction"), ("essexjunction", "vt", "Essex Junction"),
    ("montpelier", "vt", "Montpelier"), ("rutland", "vt", "Rutland"),
    ("south burlington", "vt", "South Burlington"),
    ("southburlington", "vt", "South Burlington"),
    ("st albans", "vt", "St. Albans"), ("winooski", "vt", "Winooski"),

    # Virginia
    ("alexandria", "va", "Alexandria"), ("arlington", "va", "Arlington"),
    ("charlottesville", "va", "Charlottesville"), ("chesapeake", "va", "Chesapeake"),
    ("falls church", "va", "Falls Church"), ("fallschurch", "va", "Falls Church"),
    ("fredericksburg", "va", "Fredericksburg"), ("hampton", "va", "Hampton"),
    ("harrisonburg", "va", "Harrisonburg"), ("lynchburg", "va", "Lynchburg"),
    ("manassas", "va", "Manassas"), ("newport news", "va", "Newport News"),
    ("newportnews", "va", "Newport News"), ("norfolk", "va", "Norfolk"),
    ("portsmouth", "va", "Portsmouth"), ("richmond", "va", "Richmond"),
    ("roanoke", "va", "Roanoke"), ("suffolk", "va", "Suffolk"),
    ("virginia beach", "va", "Virginia Beach"), ("virginiabeach", "va", "Virginia Beach"),
    ("arlington county", "va", "Arlington County"),
    ("arlingtoncounty", "va", "Arlington County"),
    ("chesterfield county", "va", "Chesterfield County"),
    ("chesterfieldcounty", "va", "Chesterfield County"),
    ("fairfax county", "va", "Fairfax County"), ("fairfaxcounty", "va", "Fairfax County"),
    ("henrico county", "va", "Henrico County"), ("henricocounty", "va", "Henrico County"),
    ("loudoun county", "va", "Loudoun County"), ("loudouncounty", "va", "Loudoun County"),
    ("montgomery county", "va", "Montgomery County"),
    ("prince william county", "va", "Prince William County"),
    ("princewilliamcounty", "va", "Prince William County"),
    ("roanoke county", "va", "Roanoke County"), ("roanokecounty", "va", "Roanoke County"),
    ("stafford county", "va", "Stafford County"), ("staffordcounty", "va", "Stafford County"),

    # Washington
    ("auburn", "wa", "Auburn"), ("bellevue", "wa", "Bellevue"),
    ("bellingham", "wa", "Bellingham"), ("bothell", "wa", "Bothell"),
    ("burien", "wa", "Burien"), ("edmonds", "wa", "Edmonds"),
    ("everett", "wa", "Everett"), ("federal way", "wa", "Federal Way"),
    ("federalway", "wa", "Federal Way"), ("kent", "wa", "Kent"),
    ("kennewick", "wa", "Kennewick"), ("kirkland", "wa", "Kirkland"),
    ("lacey", "wa", "Lacey"), ("lakewood", "wa", "Lakewood"),
    ("longview", "wa", "Longview"), ("lynnwood", "wa", "Lynnwood"),
    ("marysville", "wa", "Marysville"), ("olympia", "wa", "Olympia"),
    ("pasco", "wa", "Pasco"), ("puyallup", "wa", "Puyallup"),
    ("redmond", "wa", "Redmond"), ("renton", "wa", "Renton"),
    ("richland", "wa", "Richland"), ("sammamish", "wa", "Sammamish"),
    ("seattle", "wa", "Seattle"), ("shoreline", "wa", "Shoreline"),
    ("spokane", "wa", "Spokane"), ("spokane valley", "wa", "Spokane Valley"),
    ("spokanevalley", "wa", "Spokane Valley"),
    ("tacoma", "wa", "Tacoma"), ("tukwila", "wa", "Tukwila"),
    ("vancouver", "wa", "Vancouver"), ("walla walla", "wa", "Walla Walla"),
    ("wallawalla", "wa", "Walla Walla"), ("wenatchee", "wa", "Wenatchee"),
    ("yakima", "wa", "Yakima"), ("clark county", "wa", "Clark County"),
    ("clarkcountywa", "wa", "Clark County"),
    ("king county", "wa", "King County"), ("kingcounty", "wa", "King County"),
    ("kitsap county", "wa", "Kitsap County"), ("kitsapcounty", "wa", "Kitsap County"),
    ("pierce county", "wa", "Pierce County"), ("piercecounty", "wa", "Pierce County"),
    ("skagit county", "wa", "Skagit County"), ("skagitcounty", "wa", "Skagit County"),
    ("snohomish county", "wa", "Snohomish County"),
    ("snohomishcounty", "wa", "Snohomish County"),
    ("spokane county", "wa", "Spokane County"), ("spokanecounty", "wa", "Spokane County"),
    ("thurston county", "wa", "Thurston County"),
    ("thurstoncounty", "wa", "Thurston County"),
    ("whatcom county", "wa", "Whatcom County"), ("whatcomcounty", "wa", "Whatcom County"),
    ("yakima county", "wa", "Yakima County"), ("yakimacounty", "wa", "Yakima County"),

    # West Virginia
    ("charleston", "wv", "Charleston"), ("huntington", "wv", "Huntington"),
    ("morgantown", "wv", "Morgantown"), ("parkersburg", "wv", "Parkersburg"),
    ("wheeling", "wv", "Wheeling"),

    # Wisconsin
    ("appleton", "wi", "Appleton"), ("beloit", "wi", "Beloit"),
    ("eau claire", "wi", "Eau Claire"), ("eauclaire", "wi", "Eau Claire"),
    ("fond du lac", "wi", "Fond du Lac"), ("fonddulac", "wi", "Fond du Lac"),
    ("green bay", "wi", "Green Bay"), ("greenbay", "wi", "Green Bay"),
    ("janesville", "wi", "Janesville"), ("kenosha", "wi", "Kenosha"),
    ("la crosse", "wi", "La Crosse"), ("lacrosse", "wi", "La Crosse"),
    ("madison", "wi", "Madison"), ("manitowoc", "wi", "Manitowoc"),
    ("milwaukee", "wi", "Milwaukee"), ("mount pleasant", "wi", "Mount Pleasant"),
    ("oshkosh", "wi", "Oshkosh"), ("racine", "wi", "Racine"),
    ("sheboygan", "wi", "Sheboygan"), ("superior", "wi", "Superior"),
    ("waukesha", "wi", "Waukesha"), ("wausau", "wi", "Wausau"),
    ("west allis", "wi", "West Allis"), ("westallis", "wi", "West Allis"),
    ("brown county", "wi", "Brown County"), ("browncounty", "wi", "Brown County"),
    ("dane county", "wi", "Dane County"), ("danecounty", "wi", "Dane County"),
    ("kenosha county", "wi", "Kenosha County"), ("kenoshacounty", "wi", "Kenosha County"),
    ("la crosse county", "wi", "La Crosse County"),
    ("lacrossecounty", "wi", "La Crosse County"),
    ("marathon county", "wi", "Marathon County"), ("marathoncounty", "wi", "Marathon County"),
    ("milwaukee county", "wi", "Milwaukee County"),
    ("milwaukeecounty", "wi", "Milwaukee County"),
    ("outagamie county", "wi", "Outagamie County"),
    ("outagamiecounty", "wi", "Outagamie County"),
    ("ozaukee county", "wi", "Ozaukee County"), ("ozaukeecounty", "wi", "Ozaukee County"),
    ("racine county", "wi", "Racine County"), ("racinecounty", "wi", "Racine County"),
    ("rock county", "wi", "Rock County"), ("rockcounty", "wi", "Rock County"),
    ("sauk county", "wi", "Sauk County"),
    ("sheboygan county", "wi", "Sheboygan County"),
    ("sheboygancounty", "wi", "Sheboygan County"),
    ("washington county", "wi", "Washington County"),
    ("washingtoncountywi", "wi", "Washington County"),
    ("waukesha county", "wi", "Waukesha County"),
    ("waukeshacounty", "wi", "Waukesha County"),
    ("winnebago county", "wi", "Winnebago County"),
    ("winnebagocounty", "wi", "Winnebago County"),

    # Wyoming
    ("casper", "wy", "Casper"), ("cheyenne", "wy", "Cheyenne"),
    ("gillette", "wy", "Gillette"), ("laramie", "wy", "Laramie"),
    ("rock springs", "wy", "Rock Springs"), ("rocksprings", "wy", "Rock Springs"),
    ("campbell county", "wy", "Campbell County"),
    ("campbellcounty", "wy", "Campbell County"),
    ("laramie county", "wy", "Laramie County"), ("laramiecounty", "wy", "Laramie County"),
    ("natrona county", "wy", "Natrona County"),

    # DC
    ("dc", "dc", "Washington DC"), ("washingtondc", "dc", "Washington DC"),

    # States as entities
    ("stateofmaine", "me", "State of Maine"),
    ("stateofvermont", "vt", "State of Vermont"),
    ("stateofnewhampshire", "nh", "State of New Hampshire"),
    ("stateofalaska", "ak", "State of Alaska"),
    ("stateofmontana", "mt", "State of Montana"),
    ("stateofwyoming", "wy", "State of Wyoming"),
    ("stateofcolorado", "co", "State of Colorado"),
    ("stateofutah", "ut", "State of Utah"),
    ("stateofnewmexico", "nm", "State of New Mexico"),
    ("stateofnevada", "nv", "State of Nevada"),
    ("stateofidaho", "id", "State of Idaho"),
    ("stateofwashington", "wa", "State of Washington"),
    ("stateoforegon", "or", "State of Oregon"),
    ("stateofcalifornia", "ca", "State of California"),
    ("stateofarizona", "az", "State of Arizona"),
    ("stateofhawaii", "hi", "State of Hawaii"),
]

# ── Additional patterns from existing slugs (special cases) ──────────────────
SPECIAL_CANDIDATES = [
    # "countyof" prefix variants
    ("countyofmaricopa", "az", "Maricopa County"),
    ("countyofpima", "az", "Pima County"),
    ("countyofsandiego", "ca", "San Diego County"),
    ("countyofsacramentoca", "ca", "Sacramento County"),
    ("countyofsonoma", "ca", "Sonoma County"),
    ("countyofmarin", "ca", "Marin County"),
    ("countyofplacer", "ca", "Placer County"),
    ("countyofventura", "ca", "Ventura County"),
    ("countyofstanislaus", "ca", "Stanislaus County"),
    ("countyofkern", "ca", "Kern County"),
    ("countyofcolusa", "ca", "Colusa County"),
    ("countyofnapa", "ca", "Napa County"),
    ("countyofsolano", "ca", "Solano County"),
    ("countyofsanbenito", "ca", "San Benito County"),
    ("countyofmontereynca", "ca", "Monterey County"),
    ("countyofmonterey", "ca", "Monterey County"),
    ("countyofsanluisobispo", "ca", "San Luis Obispo County"),
    ("countyofhumboldt", "ca", "Humboldt County"),
    ("countyofmendocino", "ca", "Mendocino County"),
    ("countyofsiskiyou", "ca", "Siskiyou County"),
    ("countyoftrinityca", "ca", "Trinity County"),
    ("countyofshasta", "ca", "Shasta County"),
    ("countyofglen", "ca", "Glenn County"),
    ("countyofbutteca", "ca", "Butte County"),
    ("countyofbutte", "ca", "Butte County"),
    ("countyofsacramento", "ca", "Sacramento County"),
    ("countyofsanjoaquin", "ca", "San Joaquin County"),
    ("countyofstanislausca", "ca", "Stanislaus County"),
    ("countyoftulare", "ca", "Tulare County"),
    ("countyofkings", "ca", "Kings County"),
    ("countyofmadera", "ca", "Madera County"),
    ("countyoffresno", "ca", "Fresno County"),
    ("countyofmerced", "ca", "Merced County"),
    ("countyofimperial", "ca", "Imperial County"),
    ("countyofsanbernardino", "ca", "San Bernardino County"),
    ("countyofriverside", "ca", "Riverside County"),
    ("countyoforange", "ca", "Orange County"),
    ("countyoflosangeles", "ca", "Los Angeles County"),
    ("countyofalameda", "ca", "Alameda County"),
    ("countyofcontracosta", "ca", "Contra Costa County"),
    ("countyofsantacruz", "ca", "Santa Cruz County"),
    ("countyofsantaclara", "ca", "Santa Clara County"),
    ("countyofsanfrancisco", "ca", "San Francisco County"),
    ("countyofsanmateo", "ca", "San Mateo County"),
    ("countyofeldorado", "ca", "El Dorado County"),
    ("countyofamador", "ca", "Amador County"),
    ("countyofcalaveras", "ca", "Calaveras County"),
    ("countyoftuolumne", "ca", "Tuolumne County"),
    ("countyofalpine", "ca", "Alpine County"),
    ("countyofmono", "ca", "Mono County"),
    ("countyofinyo", "ca", "Inyo County"),
    ("countyoflassen", "ca", "Lassen County"),
    ("countyofmodoc", "ca", "Modoc County"),
    ("countyofdelNorte", "ca", "Del Norte County"),
    ("countyofyuba", "ca", "Yuba County"),
    ("countyofsutter", "ca", "Sutter County"),
    ("countyofnorfolk", "ma", "Norfolk County"),
    ("countyofmiddlesex", "ma", "Middlesex County"),
    ("countyofsuffolk", "ma", "Suffolk County"),
    ("countyofessex", "ma", "Essex County"),
    ("countyofworcester", "ma", "Worcester County"),
    ("countyofhampshire", "ma", "Hampshire County"),
    ("countyofhampden", "ma", "Hampden County"),
    ("countyoffranklin", "ma", "Franklin County"),
    ("countyofberkshire", "ma", "Berkshire County"),
    ("countyofplymouth", "ma", "Plymouth County"),
    ("countyofbarnstable", "ma", "Barnstable County"),
    ("countyofdukes", "ma", "Dukes County"),
    ("countyofnantucket", "ma", "Nantucket County"),
    ("countyofbristolma", "ma", "Bristol County"),
    ("countyofbristol", "ma", "Bristol County"),
    ("countyoffairfield", "ct", "Fairfield County"),
    ("countyofhartford", "ct", "Hartford County"),
    ("countyofnewhaven", "ct", "New Haven County"),
    ("countyofnewlondon", "ct", "New London County"),
    ("countyoftolland", "ct", "Tolland County"),
    ("countyofwindham", "ct", "Windham County"),
    ("countyoflitchfield", "ct", "Litchfield County"),
    ("countyofmiddlesexct", "ct", "Middlesex County"),
    ("countyofprovidence", "ri", "Providence County"),
    ("countyofkent", "ri", "Kent County"),
    ("countyofwashington", "ri", "Washington County"),
    ("countyofnewport", "ri", "Newport County"),
    ("countyofbristolri", "ri", "Bristol County"),
    # townof prefix variants
    ("townofcanton", "ma", "Canton MA"), ("townofneedham", "ma", "Needham MA"),
    ("townofwalpole", "ma", "Walpole MA"), ("townofshrewsbury", "ma", "Shrewsbury MA"),
    ("townofwestborough", "ma", "Westborough MA"), ("townofmilford", "ma", "Milford MA"),
    ("townofmansfield", "ma", "Mansfield MA"), ("townofnorth reading", "ma", "North Reading MA"),
    ("townofnorthreading", "ma", "North Reading MA"),
    ("townofandover", "ma", "Andover MA"), ("townofbillerica", "ma", "Billerica MA"),
    ("townofchelmsford", "ma", "Chelmsford MA"), ("townofdracut", "ma", "Dracut MA"),
    ("townoflowell", "ma", "Lowell MA"),
    ("townofnorwood", "ma", "Norwood MA"), ("townofplymouth", "ma", "Plymouth MA"),
    ("townofstoughton", "ma", "Stoughton MA"), ("townofquincy", "ma", "Quincy MA"),
    ("townofbrockton", "ma", "Brockton MA"), ("townofarlington", "ma", "Arlington MA"),
    ("townofattleboro", "ma", "Attleboro MA"),
    ("townofgrafton", "ma", "Grafton MA"), ("townofhopkinton", "ma", "Hopkinton MA"),
    ("townofmarlborough", "ma", "Marlborough MA"), ("townofmillis", "ma", "Millis MA"),
    ("townofmillbury", "ma", "Millbury MA"), ("townofnorfolk", "ma", "Norfolk MA"),
    ("townofwrentham", "ma", "Wrentham MA"), ("townofmedfield", "ma", "Medfield MA"),
    ("townofdedham", "ma", "Dedham MA"), ("townofwestwood", "ma", "Westwood MA"),
    ("townofhingham", "ma", "Hingham MA"), ("townofcohasset", "ma", "Cohasset MA"),
    ("townofscituate", "ma", "Scituate MA"), ("townofmarshfield", "ma", "Marshfield MA"),
    ("townofpembroke", "ma", "Pembroke MA"), ("townofduxbury", "ma", "Duxbury MA"),
    ("townofplympton", "ma", "Plympton MA"), ("townofrockland", "ma", "Rockland MA"),
    ("townofhanover", "ma", "Hanover MA"), ("townofabington", "ma", "Abington MA"),
    ("townofreading", "ma", "Reading MA"), ("townofwellesley", "ma", "Wellesley MA"),
    ("townofnewton", "ma", "Newton MA"), ("townofwaltham", "ma", "Waltham MA"),
    ("townofwatertown", "ma", "Watertown MA"), ("townofbelmont", "ma", "Belmont MA"),
    ("townofcambridge", "ma", "Cambridge MA"), ("townofsomerville", "ma", "Somerville MA"),
    ("townofmedford", "ma", "Medford MA"), ("townofmalden", "ma", "Malden MA"),
    ("townofeverett", "ma", "Everett MA"), ("townofsaugus", "ma", "Saugus MA"),
    ("townofpeabody", "ma", "Peabody MA"), ("townofsalem", "ma", "Salem MA"),
    ("townofbeverly", "ma", "Beverly MA"), ("townofgloucester", "ma", "Gloucester MA"),
    ("townofrokport", "ma", "Rockport MA"), ("townofipswich", "ma", "Ipswich MA"),
    ("townofboxford", "ma", "Boxford MA"), ("townoftopsfield", "ma", "Topsfield MA"),
    ("townofhamilton", "ma", "Hamilton MA"), ("townofwenham", "ma", "Wenham MA"),
    ("townofdanvers", "ma", "Danvers MA"), ("townofmiddleton", "ma", "Middleton MA"),
    ("townoftopsham", "me", "Topsham ME"), ("townoffalmouth", "me", "Falmouth ME"),
    ("townofwindham", "me", "Windham ME"), ("townofgorham", "me", "Gorham ME"),
    ("townofbuxton", "me", "Buxton ME"), ("townofcumberland", "me", "Cumberland ME"),
    ("townofbridgton", "me", "Bridgton ME"), ("townoffreyburg", "me", "Fryeburg ME"),
    ("townofconway", "nh", "Conway NH"), ("townofbarrington", "nh", "Barrington NH"),
    ("townofgoffstown", "nh", "Goffstown NH"), ("townofbedford", "nh", "Bedford NH"),
    ("townofhooksett", "nh", "Hooksett NH"), ("townofmerrimack", "nh", "Merrimack NH"),
    ("townofmilford", "nh", "Milford NH"), ("townofpelham", "nh", "Pelham NH"),
    ("townofwindham", "nh", "Windham NH"), ("townofhampton", "nh", "Hampton NH"),
    ("townofexeter", "nh", "Exeter NH"), ("townofnewmarket", "nh", "Newmarket NH"),
    ("townofwilton", "nh", "Wilton NH"), ("townofamherst", "nh", "Amherst NH"),
    ("townofmason", "nh", "Mason NH"), ("townoflyndeborough", "nh", "Lyndeborough NH"),
    ("townofstoddard", "nh", "Stoddard NH"), ("townofhillsborough", "nh", "Hillsborough NH"),
    ("townofatkinson", "nh", "Atkinson NH"), ("townofplaistow", "nh", "Plaistow NH"),
    ("townofnewton", "nh", "Newton NH"), ("townofkingston", "nh", "Kingston NH"),
    ("townofeast kingston", "nh", "East Kingston NH"),
    ("townofeastkingston", "nh", "East Kingston NH"),
    ("townofgreenland", "nh", "Greenland NH"), ("townofnewington", "nh", "Newington NH"),
    ("townofstratham", "nh", "Stratham NH"), ("townofrye", "nh", "Rye NH"),
    ("townofnorthampton", "ny", "Northampton NY"),
    ("townofrochester", "ny", "Rochester NY"), ("townofsouthampton", "ny", "Southampton NY"),
    ("townofeasthampton", "ny", "East Hampton NY"),
    ("townofhempstead", "ny", "Hempstead NY"), ("townofislip", "ny", "Islip NY"),
    ("townofbabylon", "ny", "Babylon NY"), ("townofoyster bay", "ny", "Oyster Bay NY"),
    ("townofoysterby", "ny", "Oyster Bay NY"), ("townofnorthhempstead", "ny", "North Hempstead NY"),
    ("townofhempstead ny", "ny", "Hempstead NY"),
    # cityof prefix
    ("cityofatlanta", "ga", "Atlanta GA"), ("cityofaustin", "tx", "Austin TX"),
    ("cityofboston", "ma", "Boston MA"), ("cityofchicago", "il", "Chicago IL"),
    ("cityofdallas", "tx", "Dallas TX"), ("cityofdenver", "co", "Denver CO"),
    ("cityofhouston", "tx", "Houston TX"), ("cityoflasvegas", "nv", "Las Vegas NV"),
    ("cityoflosangeles", "ca", "Los Angeles CA"),
    ("cityofmiami", "fl", "Miami FL"), ("cityofmilwaukee", "wi", "Milwaukee WI"),
    ("cityofmontgomery", "al", "Montgomery AL"),
    ("cityofnashville", "tn", "Nashville TN"), ("cityofneworleans", "la", "New Orleans LA"),
    ("cityofnewyork", "ny", "New York NY"), ("cityofoakland", "ca", "Oakland CA"),
    ("cityofphiladelphia", "pa", "Philadelphia PA"),
    ("cityofphoenix", "az", "Phoenix AZ"), ("cityofportland", "or", "Portland OR"),
    ("cityofsacramento", "ca", "Sacramento CA"),
    ("cityofsanantonio", "tx", "San Antonio TX"),
    ("cityofsandiego", "ca", "San Diego CA"),
    ("cityofsanfrancisco", "ca", "San Francisco CA"),
    ("cityofsanjose", "ca", "San Jose CA"),
    ("cityofseattle", "wa", "Seattle WA"), ("cityofspokane", "wa", "Spokane WA"),
    ("cityofstlouis", "mo", "St. Louis MO"), ("cityoftacoma", "wa", "Tacoma WA"),
    ("cityoftulsa", "ok", "Tulsa OK"), ("cityofvancouver", "wa", "Vancouver WA"),
    ("cityofwichita", "ks", "Wichita KS"),
    ("cityofopelika", "al", "Opelika AL"), ("cityofauburnalabama", "al", "Auburn AL"),
    ("cityofgadsden", "al", "Gadsden AL"), ("cityofhuntsville", "al", "Huntsville AL"),
    ("cityofmobile", "al", "Mobile AL"), ("cityoftuscaloosa", "al", "Tuscaloosa AL"),
    ("cityofbirmingham", "al", "Birmingham AL"), ("cityofdothan", "al", "Dothan AL"),
    ("cityofhoover", "al", "Hoover AL"),
    # Metropolitan/regional
    ("metronashville", "tn", "Metro Nashville"),
    ("metrorichmond", "va", "Metro Richmond"),
    ("metroatlanta", "ga", "Metro Atlanta"),
    ("metropolisil", "il", "Metropolis IL"),
    ("metrospokane", "wa", "Metro Spokane"),
    # Common alternate slugs
    ("jacksontnms", "ms", "Jackson MS"),
    ("jacksontn", "tn", "Jackson TN"),
    ("jacksonmitn", "mi", "Jackson MI"),
    ("jacksonms", "ms", "Jackson MS"),
    ("jacksonmi", "mi", "Jackson MI"),
    ("jacksonoh", "oh", "Jackson OH"),
    ("jacksonmo", "mo", "Jackson MO"),
    ("jacksonwy", "wy", "Jackson WY"),
    # COG / regional orgs
    ("capecodcommission", "ma", "Cape Cod Commission"),
    ("mapcog", "ma", "MAPC MA"),
    ("westernmacog", "ma", "Western MA COG"),
    ("pvpcog", "ma", "PVPC MA"),
]

# ── Build full candidate set ───────────────────────────────────────────────────
def build_candidates():
    """Generate all slug candidates to probe."""
    candidates = set()
    known = set(KNOWN_SLUGS)

    # From city candidates
    for city, state, name in CITY_CANDIDATES:
        # Clean the city part
        slug = re.sub(r'[^a-z0-9]', '', city.lower())
        if slug:
            full_slug = slug + state
            candidates.add((full_slug, state, name))
            # Also try without state suffix (some portals)
            # candidates.add((slug, state, name))

    # From special candidates
    for slug, state, name in SPECIAL_CANDIDATES:
        clean = re.sub(r'[^a-z0-9]', '', slug.lower())
        if clean:
            candidates.add((clean, state, name))

    # Remove already-known slugs
    new_candidates = [(s, st, n) for s, st, n in candidates if s not in known]
    return new_candidates


# ── Probe a single slug ────────────────────────────────────────────────────────
async def probe_slug(session, slug, state, name, semaphore):
    url = f"https://api-east.viewpointcloud.com/v2/{slug}/categories"
    async with semaphore:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    try:
                        data = await resp.json()
                        count = len(data.get("data", []))
                        return {"slug": slug, "state": state.upper(), "name": name,
                                "categories": count, "status": "new"}
                    except Exception:
                        return {"slug": slug, "state": state.upper(), "name": name,
                                "categories": 0, "status": "new"}
                else:
                    return None
        except asyncio.TimeoutError:
            return None
        except Exception:
            return None


# ── Also verify known slugs (get their category counts) ──────────────────────
async def get_known_slug_info(session, slug, semaphore):
    url = f"https://api-east.viewpointcloud.com/v2/{slug}/categories"
    async with semaphore:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    try:
                        data = await resp.json()
                        count = len(data.get("data", []))
                        return {"slug": slug, "state": "??", "name": slug, "categories": count}
                    except Exception:
                        return {"slug": slug, "state": "??", "name": slug, "categories": 0}
                else:
                    return None
        except Exception:
            return None


async def main():
    print("Building candidate list...")
    candidates = build_candidates()
    print(f"Generated {len(candidates)} new candidates to probe")
    print(f"Known slugs: {len(KNOWN_SLUGS)}")

    # Rate limiting: 5 concurrent, 0.5s between batches
    semaphore = asyncio.Semaphore(5)
    found_new = []
    found_known = []

    connector = aiohttp.TCPConnector(limit=10, ssl=False)
    headers = {"User-Agent": "Mozilla/5.0 (compatible; permit-research/1.0)"}

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        # First, verify all known slugs and get their category counts
        print("\nVerifying known slugs...")
        tasks = [get_known_slug_info(session, slug, semaphore) for slug in KNOWN_SLUGS]
        batch_size = 20
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            results = await asyncio.gather(*batch)
            for r in results:
                if r:
                    found_known.append(r)
            await asyncio.sleep(0.5)
            if (i // batch_size) % 5 == 0:
                print(f"  Verified {min(i+batch_size, len(tasks))}/{len(tasks)} known slugs, {len(found_known)} valid")

        print(f"Known slugs verified: {len(found_known)}/{len(KNOWN_SLUGS)} responding")

        # Now probe new candidates
        print(f"\nProbing {len(candidates)} new candidates...")
        tasks = [probe_slug(session, slug, state, name, semaphore)
                 for slug, state, name in candidates]

        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            results = await asyncio.gather(*batch)
            for r in results:
                if r:
                    found_new.append(r)
            await asyncio.sleep(0.5)
            done = min(i + batch_size, len(tasks))
            if (i // batch_size) % 10 == 0 or done == len(tasks):
                print(f"  Progress: {done}/{len(tasks)} probed | {len(found_new)} new portals found")

    # Merge known + new
    # Try to infer state/name for known slugs
    state_map = {v.lower().replace(" ", ""): k for k, v in STATES.items()}
    def infer_state(slug):
        for abbr in sorted(STATES.keys(), key=len, reverse=True):
            if slug.endswith(abbr):
                return abbr.upper()
        return "??"

    def infer_name(slug):
        for abbr in sorted(STATES.keys(), key=len, reverse=True):
            if slug.endswith(abbr):
                city_part = slug[:-len(abbr)]
                return city_part.title() + " " + abbr.upper()
        return slug.title()

    all_portals = []
    for r in found_known:
        state = infer_state(r["slug"])
        name = infer_name(r["slug"]) if r["name"] == r["slug"] else r["name"]
        all_portals.append({
            "slug": r["slug"],
            "name": name,
            "state": state,
            "categories": r["categories"],
        })

    for r in found_new:
        all_portals.append({
            "slug": r["slug"],
            "name": r["name"],
            "state": r["state"],
            "categories": r["categories"],
        })

    # Deduplicate by slug
    seen = set()
    deduped = []
    for p in all_portals:
        if p["slug"] not in seen:
            seen.add(p["slug"])
            deduped.append(p)

    # Sort by state then name
    deduped.sort(key=lambda x: (x["state"], x["name"]))

    output_path = "/home/will/permit-api/scripts/opengov_portals.json"
    with open(output_path, "w") as f:
        json.dump(deduped, f, indent=2)

    print(f"\n{'='*60}")
    print(f"RESULTS:")
    print(f"  Known slugs verified: {len(found_known)}")
    print(f"  New portals discovered: {len(found_new)}")
    print(f"  Total unique portals: {len(deduped)}")
    print(f"  Saved to: {output_path}")
    print(f"{'='*60}")

    if found_new:
        print("\nNEWLY DISCOVERED PORTALS:")
        for p in sorted(found_new, key=lambda x: x["state"]):
            print(f"  {p['slug']:40s} {p['state']:5s} {p['name']} ({p['categories']} categories)")

    return deduped


if __name__ == "__main__":
    asyncio.run(main())
