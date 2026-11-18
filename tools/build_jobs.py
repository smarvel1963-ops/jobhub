# build_jobs.py
# Save to: C:\MarvelApps\jobhub\tools\build_jobs.py
# Builds the daily Job Hub feed: pulls company job boards (Greenhouse, Lever, Ashby), Jobicy remote jobs and
# USAJOBS federal jobs, then writes jobs.json (small list for phones) + jobs/detail_00..63.json (job details)
# + jobs/vocab.json (skill words the phone uses to read a pasted job or a resume).
#
# Run:   python tools/build_jobs.py                 (normal daily build)
#        python tools/build_jobs.py --force         (skip the "new total under half of last time" guard)
#        python tools/build_jobs.py --check-employers   (only re-check every board in employers.json; writes nothing)
# Env:   USAJOBS_KEY + USAJOBS_EMAIL (optional; without both, USAJOBS is skipped). Keys are never printed or stored.
# Python 3.12+, standard library only.

import html
import json
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ----------------------------------------------------------------------------------------------------------
# Settings
# ----------------------------------------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent          # C:\MarvelApps\jobhub
EMPLOYERS_FILE = ROOT / "tools" / "employers.json"
MAIN_FILE = ROOT / "jobs.json"
DETAIL_DIR = ROOT / "jobs"

UA = "JobHubFeed/1.0 (Marvel Corp)"
TIMEOUT = 30                 # seconds per request
WORKERS = 8                  # parallel requests (be polite)
MAX_AGE_DAYS = 60            # keep jobs posted or updated in the last 60 days
SHARDS = 64                  # detail_00 .. detail_63
KW_MAX = 25                  # keywords per job
MAX_JOBS = 25000             # hard cap on rows in jobs.json
MAX_BYTES = 3_000_000        # jobs.json size budget (uncompressed); lowest-priority rows are dropped to fit
MAX_PER_COMPANY = 200        # one huge board (e.g. 2,000+ openings) never crowds out everyone else
SUM_CHARS = 500              # detail summary length
REQ_MAX = 8                  # requirement lines per job
REQ_CHARS = 160              # max length of one requirement line
JOBICY_MAX_PAGES = 15        # cursor pages of 100 (the Jobicy feed only holds the last 7 days)
USAJOBS_MAX_PAGES = 10       # pages of 500

CREDIT = ("Jobs from companies' own job boards (Greenhouse, Lever, Ashby), Jobicy (remote) and USAJOBS (federal).")
FIELDS = ["id", "title", "company", "loc", "state", "remote", "min", "max", "period", "posted", "url", "src",
          "kw", "shard", "ind", "type"]   # "type" added 11/17 (Job type filter, Scott's Welcome look); "" when the board doesn't say

NOW = datetime.now(timezone.utc)
CUTOFF = NOW - timedelta(days=MAX_AGE_DAYS)


def log(*a):
    print(*a, flush=True)


# ----------------------------------------------------------------------------------------------------------
# HTTP
# ----------------------------------------------------------------------------------------------------------
def cached_get_json(url, cache_dir, max_age=3600, headers=None):
    """get_json with a small on-disk cache (used for Jobicy, whose terms ask for at most one poll an hour,
    and for every source when JOBHUB_CACHE_DIR is set while testing). Never caches failures."""
    path = Path(cache_dir) / (f"{zlib.crc32(url.encode('utf-8')):08x}.json")
    try:
        if path.exists() and time.time() - path.stat().st_mtime < max_age:
            return 200, json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    st, d = get_json(url, headers=headers)
    if st == 200 and d is not None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(d), encoding="utf-8")
        except Exception:
            pass
    return st, d


DEV_CACHE = os.environ.get("JOBHUB_CACHE_DIR", "").strip()          # testing only: reuse responses for 1 hour
JOBICY_CACHE = Path(tempfile.gettempdir()) / "jobhub_jobicy_cache"  # Jobicy: never poll more than once an hour


def board_get_json(url):
    return cached_get_json(url, DEV_CACHE) if DEV_CACHE else get_json(url)


def get_json(url, headers=None, tries=3):
    """GET a URL and parse JSON. Returns (status, data). Retries 429 / 5xx / network errors with a backoff."""
    h = {"User-Agent": UA, "Accept": "application/json"}
    if headers:
        h.update(headers)
    last = None
    for attempt in range(tries):
        try:
            req = urllib.request.Request(url, headers=h)
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                return r.status, json.loads(r.read().decode("utf-8", "replace"))
        except urllib.error.HTTPError as e:
            last = e.code
            if e.code in (429, 500, 502, 503, 504) and attempt < tries - 1:
                wait = 5 * (attempt + 1)
                ra = e.headers.get("Retry-After") if e.headers else None
                if ra and ra.isdigit():
                    wait = min(int(ra), 60)
                time.sleep(wait)
                continue
            return e.code, None
        except Exception as e:  # timeout, DNS, bad JSON
            last = type(e).__name__
            if attempt < tries - 1:
                time.sleep(3 * (attempt + 1))
                continue
    return last if isinstance(last, int) else -1, None


# ----------------------------------------------------------------------------------------------------------
# Text helpers: HTML -> lines, summary, requirement lines
# ----------------------------------------------------------------------------------------------------------
_BLOCK_END = re.compile(r"(?i)<\s*(?:br\s*/?|/p|/div|/h[1-6]|/tr|/ul|/ol|/section|/article|/blockquote)\s*>")
_HEAD_OPEN = re.compile(r"(?i)<\s*h[1-6][^>]*>")
_LI_OPEN = re.compile(r"(?i)<\s*li[^>]*>")
_LI_CLOSE = re.compile(r"(?i)<\s*/li\s*>")
_TAG = re.compile(r"<[^>]+>")
_SCRIPT = re.compile(r"(?is)<\s*(script|style)[^>]*>.*?<\s*/\s*\1\s*>")
_WS = re.compile(r"[ \t\r\f\v\u00a0\u200b\u2009\u202f]+")
_BULLET_CHARS = "•·▪●◦‣∙*-–—"


def html_lines(raw):
    """Turn job-description HTML into a list of (is_bullet, text) lines."""
    if not raw:
        return []
    s = raw
    if "&lt;" in s and "<" not in s[:200]:      # Greenhouse sends escaped HTML
        s = html.unescape(s)
    s = _SCRIPT.sub(" ", s)
    s = _LI_OPEN.sub("\n\x01", s)
    s = _LI_CLOSE.sub("\n", s)
    s = _HEAD_OPEN.sub("\n", s)
    s = _BLOCK_END.sub("\n", s)
    s = _TAG.sub(" ", s)
    s = html.unescape(s)
    out = []
    for line in s.split("\n"):
        bullet = line.lstrip().startswith("\x01")
        t = _WS.sub(" ", line.replace("\x01", " ")).strip()
        if not t:
            continue
        if not bullet and t[0] in _BULLET_CHARS and len(t) > 2 and t[1] == " ":
            bullet, t = True, t[2:].strip()
        if bullet:
            t = t.lstrip(_BULLET_CHARS + " ")
        if t:
            out.append((bullet, t))
    return out


def plain_lines(text):
    """Plain text (USAJOBS etc.) -> list of (is_bullet, text) lines."""
    out = []
    for line in (text or "").replace("\r", "\n").split("\n"):
        t = _WS.sub(" ", line).strip()
        if not t:
            continue
        bullet = t[0] in _BULLET_CHARS and len(t) > 2 and t[1] == " "
        if bullet:
            t = t[2:].strip()
        out.append((bullet, t))
    return out


def summary(lines, n=SUM_CHARS):
    parts = []
    size = 0
    for bullet, t in lines:
        piece = ("• " + t) if bullet else t
        parts.append(piece)
        size += len(piece) + 1
        if size > n + 50:
            break
    s = " ".join(parts)
    if len(s) > n:
        cut = s[:n].rsplit(" ", 1)[0]
        s = cut.rstrip(" ,;:-") + "…"
    return s


_REQ_HEAD = re.compile(
    r"(requirement|qualification|what you(?:'|’)?ll need|what you need|what you(?:'|’)?ll bring|what you bring|"
    r"you bring|who you are|you have|you(?:'|’)?ll have|you might have|about you|skills|experience|must[- ]have|"
    r"nice to have|ideal candidate|looking for|you may be a fit|minimum|preferred|basic|competenc|education|"
    r"what it takes|you should have|we(?:'|’)?d love|bonus points|what we(?:'|’)?re looking for|your background|"
    r"good fit|great fit|strong fit|what makes you|required|prerequisite)",
    re.I)
_NOT_REQ_HEAD = re.compile(
    r"(benefit|perks|what we offer|we offer|compensation|salary|pay range|responsibilit|what you(?:'|’)?ll do|"
    r"what you will do|day[- ]to[- ]day|duties|about us|about the company)", re.I)
_OTHER_HEAD = re.compile(
    r"(benefit|perks|what we offer|we offer|compensation|salary|pay range|about us|about the company|about the team|"
    r"responsibilit|what you(?:'|’)?ll do|what you will do|the role|day[- ]to[- ]day|your impact|how we work|"
    r"why join|why you|our culture|equal opportunity|eeo|location|schedule|duties|in this role)",
    re.I)
_REQ_CUE = re.compile(
    r"(\byears?\b|degree|experience (?:in|with)|proficien|knowledge of|ability to|familiar|certif|licen[cs]e|"
    r"background in|track record|skills? in|must have|required|bachelor|master|diploma|clearance)", re.I)


_SKIP_HEAD = re.compile(
    r"(benefit|perks|what we offer|we offer|compensation|salary|pay range|pay transparency|total rewards|why join|"
    r"why work|life at|our culture|our values|equal (?:employment )?opportunit|eeo|accommodation|^about (?!the role|"
    r"the job|the position|this role|the opportunity|you\b|the team|the work)|who we are|our story|our mission|"
    r"privacy|disclaimer|"
    r"e-verify|diversity|inclusion)", re.I)
_BOILER = re.compile(
    r"(equal opportunity|equal employment|without regard to|regardless of|reasonable accommodation|veteran status|"
    r"dental|vision insurance|401\s?\(?k|paid time off|\bpto\b|parental leave|health insurance|life insurance|"
    r"disability insurance|e-verify|pay transparency|privacy (?:notice|policy)|wellness|stipend|commuter|"
    r"equity package|stock options|employee assistance|fertility|hsa\b|fsa\b)", re.I)


def _is_heading(t):
    if len(t) > 80 or t.endswith("."):
        return False
    words = len(t.split())
    if t.endswith(":"):
        return words <= 12
    return words <= 7 and bool(_REQ_HEAD.search(t) or _OTHER_HEAD.search(t) or _SKIP_HEAD.search(t))


def heading_flags(lines):
    """True for lines that act as section headings: known heading words, a trailing colon, or a short
    line sitting right above a bullet list."""
    flags = []
    for i, (bullet, t) in enumerate(lines):
        if bullet:
            flags.append(False)
            continue
        next_is_bullet = i + 1 < len(lines) and lines[i + 1][0]
        short = len(t) <= 70 and len(t.split()) <= 8 and not t.endswith((".", ",", ";", "!", "?"))
        flags.append(_is_heading(t) or (short and next_is_bullet))
    return flags


_QUAL_WORD = re.compile(r"qualification|requirement", re.I)


def kw_lines(lines):
    """Lines worth mining for skills: drops benefits / EEO / 'about the company' sections and boilerplate lines."""
    out, skip = [], False
    for (bullet, t), head in zip(lines, heading_flags(lines)):
        if head:
            skip = bool(_SKIP_HEAD.search(t)) and not _QUAL_WORD.search(t)
            continue
        if skip or _BOILER.search(t):
            continue
        out.append((bullet, t))
    return out


def requirements(lines, n=REQ_MAX):
    reqs, section = [], None
    for (bullet, t), head in zip(lines, heading_flags(lines)):
        if head:
            if _QUAL_WORD.search(t) or (_REQ_HEAD.search(t) and not _NOT_REQ_HEAD.search(t)):
                section = "req"
            else:
                section = "other"
            continue
        if not bullet:
            continue
        if section == "req" and len(t) > 3:
            reqs.append(_clip(t))
            if len(reqs) >= n:
                return reqs
    if not reqs:  # no clear section: fall back to bullets that read like requirements
        for bullet, t in lines:
            if bullet and _REQ_CUE.search(t):
                reqs.append(_clip(t))
                if len(reqs) >= n:
                    break
    return reqs


def _clip(t, n=REQ_CHARS):
    t = t.strip()
    if len(t) <= n:
        return t
    return t[:n].rsplit(" ", 1)[0].rstrip(" ,;:-") + "…"


# ----------------------------------------------------------------------------------------------------------
# Keyword vocabulary (~400 skills, tools, certifications, degrees). Format: "canonical = alias | alias".
# Aliases are literal text (matched case-insensitively on word edges). An alias starting with "!" is matched
# case-sensitively on the original text (for short acronyms like RN). Groups are listed in priority order:
# the first groups win when a job has more than KW_MAX matches.
# ----------------------------------------------------------------------------------------------------------
VOCAB_GROUPS = [
    # --- 0: degrees, licenses, certifications, clearances ---
    """
    high school diploma = high school diploma | high school degree | ged | high school education
    associate's degree = associate's degree | associates degree | associate degree | associate's in
    bachelor's degree = bachelor's degree | bachelors degree | bachelor degree | bachelor's in | bachelor of | b.s. | b.a. | bs/ba | ba/bs | bs degree | ba degree | undergraduate degree | four-year degree | 4-year degree
    master's degree = master's degree | masters degree | master degree | master's in | master of | m.s. | ms degree | graduate degree
    mba = mba
    phd = phd | ph.d | ph.d. | doctorate | doctoral degree
    jd = juris doctor | j.d. | !JD
    pharmd = pharmd | pharm.d
    bsn = bsn
    msn = msn
    rn = registered nurse | !RN | rn license
    lpn = licensed practical nurse | !LPN | !LVN | licensed vocational nurse
    cna = certified nursing assistant | !CNA
    nurse practitioner = nurse practitioner | !APRN | !FNP | !PMHNP
    physician assistant = physician assistant | !PA-C
    physician = physician | !MD/DO | board certified | board-certified
    pharmacist = pharmacist
    pharmacy technician = pharmacy technician | pharmacy tech | !CPhT
    emt = !EMT | emergency medical technician
    paramedic = paramedic
    medical assistant = medical assistant | !CMA
    phlebotomy = phlebotomy | phlebotomist
    radiologic technologist = radiologic technologist | rad tech | !ARRT
    dental hygienist = dental hygienist
    physical therapist = physical therapist | physical therapy | !DPT
    occupational therapist = occupational therapist | occupational therapy
    speech language pathologist = speech-language pathologist | speech language pathologist | !SLP | !CCC-SLP
    lcsw = !LCSW | licensed clinical social worker
    lpc = !LPC | licensed professional counselor
    lmft = !LMFT | marriage and family therapist
    psychologist = psychologist | psyd | psy.d
    bcba = !BCBA | board certified behavior analyst
    rbt = !RBT | registered behavior technician
    medical coding = medical coding | medical coder | !CPC | !CCS | coding certification
    rhit = !RHIT | !RHIA
    bls = !BLS | basic life support
    acls = !ACLS | advanced cardiac life support
    pals = !PALS
    cpr = cpr | cpr certification
    first aid = first aid
    ccrn = !CCRN
    cdl = cdl | commercial driver's license | commercial drivers license | cdl-a | cdl class a | class a cdl | cdl-b | class b cdl
    hazmat = hazmat | hazardous materials endorsement
    driver's license = driver's license | drivers license | driving license | valid license
    forklift certification = forklift certification | forklift certified | forklift license
    osha = osha | osha 10 | osha 30
    servsafe = servsafe | food handler | food handler's
    cpa = !CPA | certified public accountant
    cfa = !CFA | chartered financial analyst
    cfp = !CFP | certified financial planner
    series 7 = series 7
    series 63 = series 63
    series 65 = series 65 | series 66
    finra = finra
    insurance license = insurance license | producer license | licensed insurance agent | p&c license | life and health license
    real estate license = real estate license | licensed real estate | realtor
    notary = notary
    pmp = !PMP | project management professional
    scrum master = scrum master | certified scrummaster | !CSM | !PSM
    six sigma = six sigma | lean six sigma | green belt | black belt
    itil = itil
    cissp = cissp
    cism = cism
    cisa = cisa
    comptia security+ = security+ | comptia security | sec+
    comptia a+ = comptia a+ | a+ certification
    comptia network+ = network+
    ccna = ccna
    ccnp = ccnp
    aws certification = aws certified | aws certification | solutions architect associate
    shrm = shrm | shrm-cp | shrm-scp | !PHR | !SPHR
    security clearance = security clearance | active clearance | clearance required | clearable | ability to obtain a clearance | dod clearance
    secret clearance = secret clearance | active secret
    top secret = top secret | ts/sci | ts clearance
    public trust = public trust
    polygraph = polygraph
    us citizenship = u.s. citizenship | us citizenship | u.s. citizen | us citizen | united states citizen | citizenship required
    epa 608 = epa 608 | epa certification
    journeyman = journeyman | journeyman license
    master electrician = master electrician
    airframe and powerplant = a&p license | a&p mechanic | airframe and powerplant
    commercial pilot = commercial pilot | !ATP | airline transport pilot | !CFI | pilot license
    faa part 107 = part 107 | faa part 107 | remote pilot certificate
    teaching license = teaching license | teaching certificate | teaching credential | state teaching | certified teacher
    tefl = tefl | tesol | esl certification
    """,
    # --- 1: tools and technical skills ---
    """
    python = python
    java = java
    javascript = javascript | es6
    typescript = typescript
    c++ = c++ | cpp
    c# = c# | csharp
    golang = golang
    rust = rust
    ruby = ruby
    php = php
    swift = swift | swiftui
    kotlin = kotlin
    scala = scala
    r programming = r programming | rstudio | tidyverse | python or r | r or python | python/r | r/python
    matlab = matlab
    sql = sql | t-sql | pl/sql
    nosql = nosql
    postgresql = postgresql | postgres
    mysql = mysql
    mongodb = mongodb
    redis = redis
    elasticsearch = elasticsearch | opensearch
    kafka = kafka
    spark = spark | pyspark
    hadoop = hadoop
    airflow = airflow
    dbt = dbt
    snowflake = snowflake
    databricks = databricks
    bigquery = bigquery
    redshift = redshift
    tableau = tableau
    power bi = power bi | powerbi
    looker = looker
    excel = excel | spreadsheets | pivot tables | vlookup
    vba = vba
    google sheets = google sheets
    sas = !SAS
    spss = spss
    stata = stata
    html = html | html5
    css = css | css3
    react = react | react.js | reactjs
    angular = angular | angularjs
    vue = vue | vue.js | vuejs
    node.js = node.js | nodejs | node
    next.js = next.js | nextjs
    django = django
    flask = flask
    fastapi = fastapi
    ruby on rails = ruby on rails | rails
    spring = spring boot | spring framework
    .net = .net | asp.net | .net core
    graphql = graphql
    rest api = rest api | restful | rest apis | apis
    microservices = microservices | microservice
    aws = aws | amazon web services
    azure = azure
    gcp = gcp | google cloud
    docker = docker | containers
    kubernetes = kubernetes | k8s
    terraform = terraform
    ansible = ansible
    jenkins = jenkins
    ci cd = ci/cd | continuous integration | continuous delivery | github actions
    git = git | version control
    linux = linux | unix
    bash = bash | shell scripting
    powershell = powershell
    windows server = windows server
    active directory = active directory | entra id | azure ad
    vmware = vmware
    networking = networking | tcp/ip | routing and switching | lan/wan
    firewalls = firewall | firewalls | palo alto networks
    siem = siem
    splunk = splunk
    penetration testing = penetration testing | pen testing | pentesting | ethical hacking
    incident response = incident response
    soc 2 = soc 2 | soc2
    iso 27001 = iso 27001
    nist = nist
    hipaa = hipaa
    pci = pci | pci-dss | pci dss
    gdpr = gdpr
    sox = sox | sarbanes-oxley
    machine learning = machine learning | ml models | ml engineering
    deep learning = deep learning | neural networks
    nlp = nlp | natural language processing
    computer vision = computer vision
    llm = llm | llms | large language model | large language models
    generative ai = generative ai | genai | gen ai
    pytorch = pytorch
    tensorflow = tensorflow
    data analysis = data analysis | data analytics | analyze data
    data science = data science
    statistics = statistics | statistical
    ab testing = a/b testing | a/b tests | ab testing | experimentation
    etl = etl | elt | data pipelines
    data modeling = data modeling
    data warehousing = data warehouse | data warehousing
    ios = ios
    android = android
    react native = react native
    flutter = flutter
    unity = unity3d | unity engine
    unreal engine = unreal engine | unreal
    figma = figma
    sketch = sketch app
    adobe creative suite = adobe creative suite | adobe creative cloud | adobe cc
    photoshop = photoshop
    illustrator = illustrator
    indesign = indesign
    after effects = after effects
    premiere pro = premiere pro | premiere
    autocad = autocad
    solidworks = solidworks
    revit = revit
    catia = catia
    cad = cad | computer-aided design
    plc = plc | plcs | programmable logic
    scada = scada
    labview = labview
    embedded = embedded systems | embedded software | embedded c
    firmware = firmware
    fpga = fpga
    verilog = verilog | vhdl | systemverilog
    salesforce = salesforce | sfdc
    hubspot = hubspot
    marketo = marketo
    zendesk = zendesk
    servicenow = servicenow
    jira = jira
    confluence = confluence
    sap = !SAP
    oracle = oracle
    netsuite = netsuite
    workday = workday
    quickbooks = quickbooks
    epic ehr = epic ehr | epic systems | epic certification | epic certified | epic emr
    cerner = cerner | oracle health
    ehr = ehr | emr | electronic health record | electronic medical record
    sharepoint = sharepoint
    microsoft office = microsoft office | ms office | office 365 | microsoft 365 | m365
    powerpoint = powerpoint
    google workspace = google workspace | g suite | gsuite
    ms project = ms project | microsoft project
    visio = visio
    crm = crm
    erp = erp
    seo = seo | search engine optimization
    sem = sem | search engine marketing | paid search | ppc
    google analytics = google analytics | ga4
    google ads = google ads | adwords
    paid social = paid social | facebook ads | meta ads
    wordpress = wordpress
    shopify = shopify
    ux design = ux design | user experience | ux
    ui design = ui design | user interface design
    user research = user research | usability testing
    prototyping = prototyping | prototypes
    accessibility = accessibility | wcag | 508 compliance
    agile = agile
    scrum = scrum
    devops = devops
    sre = site reliability | !SRE
    observability = observability | monitoring tools
    datadog = datadog
    blockchain = blockchain | web3 | crypto
    solidity = solidity
    test automation = test automation | automated testing | automation testing
    selenium = selenium
    cypress = cypress
    playwright = playwright
    qa testing = qa testing | quality assurance testing | manual testing | software testing
    technical writing = technical writing | technical documentation
    cobol = cobol | mainframe
    gis = gis | arcgis
    robotics = robotics | ros
    cloud computing = cloud computing | cloud infrastructure | cloud platforms
    cybersecurity = cybersecurity | cyber security | information security | infosec
    identity and access management = identity and access management | iam
    system administration = system administration | systems administration | sysadmin
    network administration = network administration | network engineering
    help desk = help desk | helpdesk | service desk
    technical support = technical support | tech support | desktop support | it support
    troubleshooting = troubleshooting | troubleshoot
    computer science = computer science
    electrical engineering = electrical engineering
    mechanical engineering = mechanical engineering
    civil engineering = civil engineering
    chemical engineering = chemical engineering
    industrial engineering = industrial engineering
    mathematics = mathematics | applied math
    physics = physics
    chemistry = chemistry
    biology = biology | life sciences
    accounting = accounting
    economics = economics
    nursing = nursing
    """,
    # --- 2: work skills and fields ---
    """
    project management = project management | project manager | managing projects
    program management = program management
    product management = product management | product manager
    customer service = customer service | customer support | customer care
    customer success = customer success
    account management = account management | account manager
    sales = sales experience | quota | closing deals | inside sales | outside sales
    b2b sales = b2b sales | enterprise sales | saas sales
    business development = business development
    lead generation = lead generation | prospecting | cold calling
    negotiation = negotiation | negotiating
    recruiting = recruiting | talent acquisition | sourcing candidates
    human resources = human resources | hr generalist | hr business partner | hrbp
    payroll = payroll
    benefits administration = benefits administration
    training = training programs | train new | trainer
    bookkeeping = bookkeeping
    accounts payable = accounts payable
    accounts receivable = accounts receivable | collections
    general ledger = general ledger | journal entries | month-end close | month end close
    financial analysis = financial analysis | financial analyst
    financial modeling = financial modeling | financial models
    financial planning and analysis = fp&a | financial planning and analysis
    budgeting = budgeting | budget management
    forecasting = forecasting
    auditing = auditing | auditor | internal audit | external audit | audit experience | financial audit
    tax = tax preparation | tax compliance | taxation
    gaap = gaap | us gaap
    reconciliation = reconciliation | reconciliations
    underwriting = underwriting | underwriter
    claims = claims processing | claims adjuster | claims
    risk management = risk management
    compliance = compliance
    aml = aml | anti-money laundering | bsa/aml
    kyc = kyc | know your customer
    legal research = legal research
    contract management = contract management | contract negotiation | contract administration
    paralegal = paralegal
    litigation = litigation
    procurement = procurement | purchasing | strategic sourcing
    supply chain = supply chain
    logistics = logistics
    inventory management = inventory management | inventory control | inventory
    warehouse = warehouse | distribution center | fulfillment center
    forklift = forklift | pallet jack | reach truck
    shipping and receiving = shipping and receiving | shipping & receiving
    order picking = order picking | picking and packing | pick and pack
    dispatch = dispatch | dispatcher
    fleet management = fleet management | fleet manager | fleet maintenance
    route planning = route planning | route optimization
    delivery driver = delivery driver | package delivery | delivery route | local delivery
    truck driving = truck driving | truck driver | otr | tractor-trailer
    heavy equipment = heavy equipment | equipment operator
    construction = construction
    carpentry = carpentry | carpenter
    electrical = electrician | electrical work | electrical systems
    plumbing = plumbing | plumber
    hvac = hvac | hvac-r
    welding = welding | welder
    maintenance = maintenance technician | preventive maintenance | maintenance mechanic | building maintenance
    facilities = facilities management | facilities maintenance | facility maintenance | facilities manager
    janitorial = janitorial | custodial | cleaning
    manufacturing = manufacturing
    assembly = assembly | assembler
    machining = machining | machinist
    cnc = cnc
    quality assurance = quality assurance
    quality control = quality control | inspection
    iso 9001 = iso 9001
    gmp = gmp | cgmp | good manufacturing practice
    fda regulations = fda | 21 cfr
    clinical research = clinical research
    clinical trials = clinical trials | clinical trial | gcp guidelines
    regulatory affairs = regulatory affairs | regulatory submissions
    laboratory = laboratory | lab experience | wet lab
    patient care = patient care | direct patient care
    case management = case management | case manager
    care coordination = care coordination | care coordinator
    behavioral health = behavioral health
    mental health = mental health | psychotherapy | counseling
    social work = social work | social worker
    telehealth = telehealth | telemedicine | virtual care
    utilization review = utilization review | utilization management | prior authorization
    medical billing = medical billing | billing and coding
    revenue cycle = revenue cycle | rcm
    icd 10 = icd-10 | icd10 | cpt codes
    triage = triage
    home health = home health | home care
    hospice = hospice | palliative
    pediatrics = pediatrics | pediatric
    geriatrics = geriatrics | geriatric | senior care
    oncology = oncology
    cardiology = cardiology
    emergency medicine = emergency medicine | emergency department
    icu = icu | intensive care | critical care
    med surg = med-surg | med surg | medical-surgical
    labor and delivery = labor and delivery
    operating room = operating room | perioperative
    dental = dental assistant | dental office | dental practice | dentistry | dental hygiene
    veterinary = veterinary | animal care
    teaching = teaching | teacher | classroom
    curriculum development = curriculum development | curriculum design | curriculum
    instructional design = instructional design | instructional designer
    tutoring = tutoring | tutor
    special education = special education | iep
    early childhood education = early childhood | preschool | child care | childcare
    lms = lms | learning management system | canvas lms
    elearning = e-learning | elearning | online learning
    admissions = admissions | enrollment
    fundraising = fundraising | donor relations | development officer
    grant writing = grant writing | grant management | grant proposals
    event planning = event planning | event management | event coordination
    public relations = public relations | media relations
    communications = communications strategy | corporate communications | internal communications
    journalism = journalism | journalist | editorial
    copywriting = copywriting | copywriter
    content marketing = content marketing | content strategy | content creation
    social media = social media
    email marketing = email marketing | lifecycle marketing | crm marketing
    video production = video production | video editing
    photography = photography
    graphic design = graphic design | graphic designer
    animation = animation | motion graphics
    marketing = marketing
    digital marketing = digital marketing | performance marketing | growth marketing
    brand management = brand management | brand strategy
    market research = market research
    product marketing = product marketing
    partnerships = partnerships | channel partners
    operations = operations management | business operations
    process improvement = process improvement | continuous improvement | lean manufacturing | kaizen
    data entry = data entry
    administrative support = administrative support | administrative assistant | clerical
    executive assistant = executive assistant | calendar management
    scheduling = scheduling | scheduler
    reception = receptionist | front desk | reception
    office management = office management | office manager
    call center = call center | contact center | inbound calls | outbound calls
    bilingual = bilingual
    spanish = spanish
    french = french
    german = german
    mandarin = mandarin | chinese
    japanese = japanese
    portuguese = portuguese
    korean = korean
    arabic = arabic
    vietnamese = vietnamese
    asl = american sign language | !ASL
    food service = food service | food preparation | line cook | cook
    barista = barista
    bartending = bartending | bartender
    hospitality = hospitality
    housekeeping = housekeeping
    retail sales = retail sales | retail experience | retail store | sales associate
    merchandising = merchandising | visual merchandising
    cashier = cashier | point of sale | pos
    loss prevention = loss prevention
    store management = store manager | store management
    security officer = security officer | security guard
    law enforcement = law enforcement
    military experience = military experience | military service
    real estate = real estate
    property management = property management | property manager
    leasing = leasing
    mortgage = mortgage
    lending = lending | loan origination | loan officer
    banking = banking
    trading = trading | trader
    investment banking = investment banking
    private equity = private equity
    venture capital = venture capital
    wealth management = wealth management | financial advisor | financial planning
    insurance = insurance industry | insurance experience | insurance agency | insurance carrier | insurance products | property and casualty | p&c insurance
    actuarial = actuarial | actuary
    consulting = consulting | consultant
    strategy = strategic planning | business strategy
    people management = people management | managing teams | direct reports | team lead | supervisory | supervisor
    leadership = leadership
    mentoring = mentoring | mentorship
    stakeholder management = stakeholder management | stakeholders
    cross functional = cross-functional | cross functional
    startup experience = startup experience | startup environment | fast-paced startup
    """,
    # --- 3: schedule and physical requirements (lowest priority; generic soft skills are left out on purpose:
    #        nearly every posting lists them, so they only crowd the 25 keyword slots) ---
    """
    travel = travel required | willingness to travel | ability to travel
    night shift = night shift | overnight shift | nights
    weekend availability = weekends | weekend availability
    lifting = lift up to | lifting up to | heavy lifting | lift 25 | lift 40 | lift 50 | lift 75 | lift heavy
    drug test = drug screen | drug test | drug testing
    """,
]


_NOT_WORD_CHARS = re.compile(r"[^a-z0-9+#'. ]+")


def norm_word(s, joined=False):
    """Lowercase; keep only a-z 0-9 + # ' . and single spaces (the form the phone app matches against).
    joined=True drops the other characters instead of turning them into spaces ("ci/cd" -> "cicd")."""
    s = s.lower().replace("’", "'")
    s = _NOT_WORD_CHARS.sub("" if joined else " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _build_vocab():
    ci_alias, cs_alias, rank, aliases_of = {}, {}, {}, {}
    order = 0
    for g, block in enumerate(VOCAB_GROUPS):
        for line in block.strip().splitlines():
            line = line.strip()
            if not line or "=" not in line or line.startswith("#"):
                continue
            canon, aliases = line.split("=", 1)
            canon = norm_word(canon)          # keyword values are always in the app's word form
            rank.setdefault(canon, (g, order))
            order += 1
            for a in aliases.split("|"):
                a = a.strip()
                if not a:
                    continue
                if a.startswith("!"):
                    cs_alias[a[1:]] = canon
                    aliases_of.setdefault(canon, []).append(a[1:])
                else:
                    ci_alias[a.lower()] = canon
                    aliases_of.setdefault(canon, []).append(a)

    def alt(keys):
        return "|".join(re.escape(k).replace(r"\ ", r"\s+") for k in sorted(keys, key=len, reverse=True))

    ci = re.compile(r"(?<![a-z0-9])(?:" + alt(ci_alias) + r")(?![a-z0-9+#])")
    cs = re.compile(r"(?<![A-Za-z0-9])(?:" + alt(cs_alias) + r")(?![A-Za-z0-9+#])")
    return ci, ci_alias, cs, cs_alias, rank, aliases_of


_VOCAB_CI, _ALIAS_CI, _VOCAB_CS, _ALIAS_CS, _RANK, _ALIASES_OF = _build_vocab()
VOCAB_MAX = 600
# Word forms left out of vocab.json because, once lowercased, they match ordinary words or read as noise.
VOCAB_SKIP = {"pals", "pa c", "pac", "md do", "mddo", "es6", "node", "lean", "apis", "containers", "presenting",
              "prioritize", "nights", "weekends", "fleet", "cook", "trader", "trading", "teacher", "audit", "claims",
              "contracts", "sourcing", "inventory", "inspection", "routing", "delivery", "events", "grants", "retail",
              "reception", "cleaning", "pos", "unreal", "premiere", "rails", "ux", "crypto", "ros", "otr", "rcm"}


def vocab_words(freq):
    """Canonical skill names + aliases in the app's word form, capped at VOCAB_MAX, in this order:
    canonicals used in today's feed (most common first), their aliases, then unused canonicals, then their aliases.
    So every keyword that appears in a jobs.json row is always in the list. Returns (words, alias -> canonical)."""
    order = sorted(_RANK, key=lambda c: (-freq.get(c, 0), _RANK[c]))
    used = [c for c in order if freq.get(c, 0) > 0]
    unused = [c for c in order if freq.get(c, 0) <= 0]
    words, seen, amap = [], set(), {}

    def add(w, canon):
        if len(words) >= VOCAB_MAX or len(w) < 2 or w in seen or (w != canon and w in VOCAB_SKIP):
            return
        seen.add(w)
        words.append(w)
        if w != canon:
            amap[w] = canon

    for group in (used, unused):
        for c in group:
            add(c, c)
        for c in group:
            for a in _ALIASES_OF.get(c, []):
                add(norm_word(a), c)
                add(norm_word(a, joined=True), c)
    return words, amap
_NUMWORDS = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9,
             "ten": 10, "twelve": 12, "fifteen": 15}
_YEARS = re.compile(
    r"(?<![\d.])(\d{1,2}|one|two|three|four|five|six|seven|eight|nine|ten|twelve|fifteen)\s*\+?\s*"
    r"(?:(?:-|–|to)\s*\d{1,2}\s*\+?\s*)?(?:\(\d{1,2}\)\s*)?years?(?:'|’)?\s+(?:of\s+)?"
    r"(?:[a-z/&,\-]+\s+){0,5}?(?:experience|exp\b)", re.I)


def keywords(title, lines):
    raw = title + "\n" + "\n".join(t for _, t in kw_lines(lines))
    raw = raw.replace("’", "'")
    low = raw.lower()
    found = {}
    for m in _VOCAB_CI.finditer(low):
        c = _ALIAS_CI.get(re.sub(r"\s+", " ", m.group(0)))
        if c and c not in found:
            found[c] = m.start()
    for m in _VOCAB_CS.finditer(raw):
        c = _ALIAS_CS.get(re.sub(r"\s+", " ", m.group(0)))
        if c and c not in found:
            found[c] = m.start()
    kws = sorted(found, key=lambda c: (_RANK[c][0], found[c]))
    years = None
    for m in _YEARS.finditer(low):
        v = m.group(1).lower()
        n = _NUMWORDS.get(v) if not v.isdigit() else int(v)
        if n is not None and 0 < n <= 25:
            years = n if years is None else min(years, n)
    out = ([f"years:{years}"] if years else []) + kws
    return out[:KW_MAX]


# ----------------------------------------------------------------------------------------------------------
# Location helpers
# ----------------------------------------------------------------------------------------------------------
STATE_CODES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA", "colorado": "CO",
    "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS", "kentucky": "KY", "louisiana": "LA",
    "maine": "ME", "maryland": "MD", "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
    "mississippi": "MS", "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY", "north carolina": "NC",
    "north dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA",
    "rhode island": "RI", "south carolina": "SC", "south dakota": "SD", "tennessee": "TN", "texas": "TX",
    "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC", "puerto rico": "PR",
}
VALID_CODES = set(STATE_CODES.values())
CITY_STATE = {
    "san francisco": "CA", "new york city": "NY", "nyc": "NY", "manhattan": "NY", "brooklyn": "NY",
    "seattle": "WA", "austin": "TX", "boston": "MA", "chicago": "IL", "los angeles": "CA", "denver": "CO",
    "atlanta": "GA", "miami": "FL", "dallas": "TX", "houston": "TX", "washington dc": "DC",
    "washington d.c.": "DC", "washington, d.c.": "DC", "philadelphia": "PA", "phoenix": "AZ",
    "san diego": "CA", "salt lake city": "UT", "nashville": "TN", "pittsburgh": "PA", "minneapolis": "MN",
    "detroit": "MI", "columbus": "OH", "raleigh": "NC", "charlotte": "NC", "san jose": "CA",
    "palo alto": "CA", "mountain view": "CA", "menlo park": "CA", "sunnyvale": "CA", "redwood city": "CA",
    "oakland": "CA", "jersey city": "NJ", "st. louis": "MO", "saint louis": "MO", "kansas city": "MO",
    "indianapolis": "IN", "little rock": "AR", "bentonville": "AR", "rogers, ar": "AR", "memphis": "TN",
    "new orleans": "LA", "oklahoma city": "OK", "tulsa": "OK", "baltimore": "MD", "san antonio": "TX",
    "irvine": "CA", "santa monica": "CA", "hawthorne": "CA", "el segundo": "CA", "boulder": "CO",
    "ann arbor": "MI", "tampa": "FL", "orlando": "FL", "las vegas": "NV", "sacramento": "CA",
    "cincinnati": "OH", "cleveland": "OH", "milwaukee": "WI", "omaha": "NE", "reston": "VA", "mclean": "VA",
    "huntsville": "AL", "colorado springs": "CO", "albuquerque": "NM", "honolulu": "HI", "boise": "ID",
    "lehi": "UT", "provo": "UT", "scottsdale": "AZ", "tempe": "AZ", "plano": "TX", "frisco": "TX",
    "irving": "TX", "fort worth": "TX", "chapel hill": "NC", "san mateo": "CA", "south san francisco": "CA",
    "emeryville": "CA", "bay area": "CA", "silicon valley": "CA", "louisville": "KY", "birmingham, al": "AL",
    "jacksonville": "FL", "fort lauderdale": "FL", "durham, nc": "NC", "richmond, va": "VA",
    "arlington, va": "VA", "cambridge, ma": "MA", "somerville": "MA", "hoboken": "NJ", "stamford": "CT",
    "long beach": "CA", "pasadena": "CA", "burbank": "CA", "fremont": "CA", "santa clara": "CA",
    "cupertino": "CA", "los gatos": "CA", "berkeley": "CA", "madison, wi": "WI", "des moines": "IA",
    "knoxville": "TN", "chattanooga": "TN", "greenville, sc": "SC", "charleston, sc": "SC", "savannah": "GA",
    "baton rouge": "LA", "jackson, ms": "MS", "fayetteville, ar": "AR", "springdale": "AR",
}
_US_WORDS = re.compile(r"\b(united states|usa|u\.s\.a?\.?|us|america|nationwide|us-based|us only)\b", re.I)
_FOREIGN = re.compile(
    r"\b(canada|toronto|vancouver|montreal|ottawa|calgary|ontario|quebec|british columbia|alberta|mexico city|"
    r"(?<!new )mexico|brazil|s[aã]o paulo|argentina|buenos aires|colombia|bogot[aá]|chile|peru|latam|"
    r"united kingdom|\buk\b|england|london|manchester|edinburgh|scotland|ireland|dublin|wales|"
    r"germany|berlin|munich|hamburg|france|paris|spain|madrid|barcelona|portugal|lisbon|italy|milan|rome|"
    r"netherlands|amsterdam|belgium|brussels|switzerland|zurich|geneva|austria|vienna|sweden|stockholm|"
    r"norway|oslo|denmark|copenhagen|finland|helsinki|poland|warsaw|krak[oó]w|czech|prague|romania|"
    r"bucharest|hungary|budapest|greece|athens|serbia|belgrade|ukraine|kyiv|estonia|tallinn|lithuania|"
    r"latvia|bulgaria|sofia|croatia|turkey|istanbul|israel|tel aviv|uae|dubai|abu dhabi|saudi|riyadh|"
    r"qatar|egypt|cairo|nigeria|lagos|kenya|nairobi|south africa|cape town|johannesburg|india|bangalore|"
    r"bengaluru|hyderabad|mumbai|pune|chennai|delhi|gurgaon|gurugram|noida|pakistan|bangladesh|sri lanka|"
    r"singapore|malaysia|kuala lumpur|indonesia|jakarta|philippines|manila|vietnam|thailand|bangkok|"
    r"china|beijing|shanghai|shenzhen|hong kong|taiwan|taipei|japan|tokyo|osaka|korea|seoul|australia|"
    r"sydney|melbourne|brisbane|perth|new zealand|auckland|emea|apac|europe|asia)\b", re.I)
_REMOTE = re.compile(r"\b(remote|anywhere|work from home|wfh|distributed|telecommute|virtual|home[- ]based)\b", re.I)


def detect_state(text):
    """2-letter US state code from free text, or ''."""
    if not text:
        return ""
    for m in re.finditer(r"(?:,|\s-|\(|\|)\s*([A-Z]{2})\b(?!\.?[a-z])", text):
        code = m.group(1)
        if code in VALID_CODES:
            return code
    low = text.lower()
    for name, code in STATE_CODES.items():
        if re.search(r"(?<!west )\b" + re.escape(name) + r"\b", low) if name == "virginia" else \
                re.search(r"\b" + re.escape(name) + r"\b", low):
            if name == "washington" and re.search(r"washington,?\s*d\.?c", low):
                return "DC"
            return code
    for city, code in CITY_STATE.items():
        if city in low:
            return code
    return ""


def place_info(loc_text, extra_us=None, remote_flag=False):
    """Return (state, remote, us_tier) where us_tier is 0 = US or US-remote, 1 = unknown, 2 = outside the US."""
    txt = loc_text or ""
    foreign = bool(_FOREIGN.search(txt))
    us_word = bool(_US_WORDS.search(txt))
    state = "" if (foreign and not us_word) else detect_state(txt)
    remote = 1 if (remote_flag or _REMOTE.search(txt)) else 0
    if state or us_word or extra_us is True:
        tier = 0
    elif foreign or extra_us is False:
        tier = 2
    elif remote:
        tier = 1
    else:
        tier = 1
    return state, remote, tier


def short_loc(parts):
    """First location (shortened) plus '+N' when a job lists several."""
    parts = [p.strip() for p in parts if p and p.strip()]
    seen, uniq = set(), []
    for p in parts:
        k = p.lower()
        if k not in seen:
            seen.add(k)
            uniq.append(p)
    if not uniq:
        return ""
    first = uniq[0]
    first = re.sub(r"\bUnited States(?: of America)?\b", "US", first)
    first = re.sub(r"\s+", " ", first).strip(" ,;-")
    if len(first) > 40:
        first = first[:39].rstrip(" ,;-") + "…"
    if len(uniq) > 1:
        first += f" +{len(uniq) - 1}"
    return first


def split_locs(text):
    if not text:
        return []
    return [p for p in re.split(r"\s*(?:;|\||/(?=\s)|\s•\s|\sor\s|\band\b(?=\s+[A-Z]))\s*", text) if p.strip()]


# ----------------------------------------------------------------------------------------------------------
# Salary helpers
# ----------------------------------------------------------------------------------------------------------
HOURS_PER_YEAR = 2080
_NUM = r"(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?)"
_RANGE = re.compile(r"\$\s?" + _NUM + r"\s?([kK])?(?:\s?USD)?\s*(?:-|–|—|to)\s*(?:USD\s?)?\$?\s?" + _NUM +
                    r"\s?([kKmMbB](?![a-z]))?")
_SINGLE_HOUR = re.compile(r"\$\s?(\d{1,3}(?:\.\d{1,2})?)\s?(?:/|per\s)\s?(?:hour|hr)\b", re.I)
_SAL_CONTEXT = re.compile(r"(salary|pay|compensation|range|base|wage|hourly|annual|ote|earn|rate|usd)", re.I)
_NOT_SALARY = re.compile(r"(bonus|stipend|reimburs|allowance|equity|401|relocation|tuition|referral|funding|"
                         r"raised|revenue|valuation|investment|budget|per month credit|wellness)", re.I)


def _num(s):
    return float(s.replace(",", ""))


def annualize(lo, hi, unit):
    """unit in hour/day/week/month/year -> (annual_min, annual_max, period) or None."""
    mult = {"hour": HOURS_PER_YEAR, "day": 260, "week": 52, "biweek": 26, "month": 12, "year": 1}.get(unit)
    if not mult or lo is None:
        return None
    hi = hi if hi else lo
    if hi < lo:
        lo, hi = hi, lo
    a, b = lo * mult, hi * mult
    if a < 15000 or b > 1_500_000 or (a > 0 and b / a > 5):
        return None
    return int(round(a, -2)), int(round(b, -2)), ("hour" if unit == "hour" else "year")


def salary_from_text(text):
    if not text or "$" not in text:
        return None
    years, hours = [], []
    for m in _RANGE.finditer(text):
        lo, k1, hi, k2 = _num(m.group(1)), m.group(2), _num(m.group(3)), m.group(4)
        if k2 and k2.lower() in "mb":
            continue
        after = text[m.end():m.end() + 45].lower()
        before = text[max(0, m.start() - 150):m.start()]
        if re.match(r"\s*(million|billion|mm\b|bn\b)", after) or _NOT_SALARY.search(before[-60:]):
            continue
        if k1:
            lo *= 1000
        if k2:
            hi *= 1000
            if not k1 and lo < 1000:
                lo *= 1000
        if re.search(r"(per hour|/\s?hour|/\s?hr\b|an hour|hourly|per hr)", after):
            unit = "hour"
        elif re.search(r"(per year|/\s?year|/\s?yr\b|annual|a year|per annum|yearly)", after):
            unit = "year"
        elif re.search(r"(per month|/\s?month|/\s?mo\b|monthly)", after):
            unit = "month"
        elif re.search(r"(per week|/\s?week|/\s?wk\b|weekly)", after):
            unit = "week"
        else:
            if not _SAL_CONTEXT.search(before + after):
                continue
            unit = "hour" if hi < 300 else ("year" if hi >= 20000 else None)
        r = annualize(lo, hi, unit) if unit else None
        if r:
            (hours if r[2] == "hour" else years).append(r)
    if not years and not hours:
        m = _SINGLE_HOUR.search(text)
        if m:
            r = annualize(_num(m.group(1)), None, "hour")
            if r:
                hours.append(r)
    pick = years or hours
    if not pick:
        return None
    return min(r[0] for r in pick), max(r[1] for r in pick), pick[0][2]


# ----------------------------------------------------------------------------------------------------------
# Small helpers
# ----------------------------------------------------------------------------------------------------------
def parse_dt(v):
    if v is None or v == "":
        return None
    try:
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v / 1000 if v > 1e11 else v, tz=timezone.utc)
        s = str(v).strip().replace("Z", "+00:00")
        d = datetime.fromisoformat(s)
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def clean_title(t):
    t = _WS.sub(" ", html.unescape(t or "")).strip()
    return t if len(t) <= 90 else t[:89].rstrip() + "…"


def job_type_from(raw, title=""):
    s = (raw or "").lower().replace("_", "").replace("-", "").replace(" ", "")
    table = [("fulltime", "Full-time"), ("permanent", "Full-time"), ("parttime", "Part-time"),
             ("intern", "Internship"), ("contract", "Contract"), ("freelance", "Contract"),
             ("temporary", "Temporary"), ("temp", "Temporary"), ("seasonal", "Seasonal"),
             ("fixedterm", "Temporary"), ("regular", "Full-time"), ("standard", "Full-time")]
    for k, v in table:
        if k in s:
            return v
    tl = (title or "").lower()
    if "intern" in tl and "internal" not in tl and "international" not in tl:
        return "Internship"
    if "part-time" in tl or "part time" in tl:
        return "Part-time"
    if "contract" in tl:
        return "Contract"
    return (raw or "").strip()[:20]


def make_job(**kw):
    base = dict(id="", title="", company="", loc="", state="", remote=0, min=0, max=0, period="", posted="",
                url="", src="", lines=[], dept="", type="", ind="", tier=1, fresh=None)
    base.update(kw)
    return base


# ----------------------------------------------------------------------------------------------------------
# Sources
# ----------------------------------------------------------------------------------------------------------
def fetch_greenhouse(emp):
    tok = emp["token"]
    st, d = board_get_json(f"https://boards-api.greenhouse.io/v1/boards/{urllib.parse.quote(tok)}/jobs?content=true")
    if st != 200 or not isinstance(d, dict):
        return None, f"HTTP {st}"
    out = []
    for j in d.get("jobs", []):
        first = parse_dt(j.get("first_published"))
        upd = parse_dt(j.get("updated_at"))
        fresh = max([x for x in (first, upd) if x], default=None)
        if not fresh or fresh < CUTOFF:
            continue
        lines = html_lines(j.get("content") or "")
        loc_name = (j.get("location") or {}).get("name") or ""
        parts = split_locs(loc_name)
        state, remote, tier = place_info(loc_name, remote_flag=bool(re.search(r"\bremote\b", j.get("title", ""), re.I)))
        text = "\n".join(t for _, t in lines)
        sal = salary_from_text(text)
        meta_type = ""
        for m in j.get("metadata") or []:
            if isinstance(m, dict) and re.search(r"employment|job type|time type", str(m.get("name", "")), re.I):
                v = m.get("value")
                meta_type = v if isinstance(v, str) else (v[0] if isinstance(v, list) and v else "")
        dept = ((j.get("departments") or [{}])[0] or {}).get("name", "") if j.get("departments") else ""
        out.append(make_job(
            id=f"gh:{tok}:{j.get('id')}", title=clean_title(j.get("title")), company=emp["name"],
            loc=short_loc(parts) or loc_name[:40], state=state, remote=remote,
            min=sal[0] if sal else 0, max=sal[1] if sal else 0, period=sal[2] if sal else "",
            posted=(first or upd).strftime("%Y-%m-%d"), url=j.get("absolute_url") or "", src="Greenhouse",
            lines=lines, dept=re.sub(r"^\d+\s+", "", dept)[:60], type=job_type_from(meta_type, j.get("title")),
            ind=emp.get("industry", ""), tier=tier, fresh=fresh))
    return out, None


def fetch_lever(emp):
    tok = emp["token"]
    st, d = board_get_json(f"https://api.lever.co/v0/postings/{urllib.parse.quote(tok)}?mode=json")
    if st != 200 or not isinstance(d, list):
        return None, f"HTTP {st}"
    out = []
    for j in d:
        created = parse_dt(j.get("createdAt"))
        upd = parse_dt(j.get("updatedAt"))
        fresh = max([x for x in (created, upd) if x], default=None)
        if not fresh or fresh < CUTOFF:
            continue
        cat = j.get("categories") or {}
        body = j.get("description") or ""
        for lst in j.get("lists") or []:
            body += f"<h3>{lst.get('text', '')}</h3><ul>{lst.get('content', '')}</ul>"
        body += j.get("additional") or ""
        lines = html_lines(body)
        locs = cat.get("allLocations") or ([cat.get("location")] if cat.get("location") else [])
        loc_text = "; ".join(locs)
        country = j.get("country")
        extra_us = True if country == "US" else (False if country else None)
        wpt = (j.get("workplaceType") or "").lower()
        state, remote, tier = place_info(loc_text, extra_us=extra_us, remote_flag=(wpt == "remote"))
        sal = None
        sr = j.get("salaryRange") or {}
        if sr and (sr.get("currency") or "USD").upper() == "USD" and sr.get("min"):
            iv = (sr.get("interval") or "").lower()
            unit = "hour" if "hour" in iv else "month" if "month" in iv else "week" if "week" in iv else "year"
            sal = annualize(float(sr["min"]), float(sr.get("max") or sr["min"]), unit)
        if not sal:
            sal = salary_from_text((j.get("additionalPlain") or "") + "\n" + "\n".join(t for _, t in lines))
        out.append(make_job(
            id=f"lv:{tok}:{j.get('id')}", title=clean_title(j.get("text")), company=emp["name"],
            loc=short_loc(locs), state=state, remote=remote,
            min=sal[0] if sal else 0, max=sal[1] if sal else 0, period=sal[2] if sal else "",
            posted=(created or upd).strftime("%Y-%m-%d"), url=j.get("hostedUrl") or "", src="Lever",
            lines=lines, dept=(cat.get("department") or cat.get("team") or "")[:60],
            type=job_type_from(cat.get("commitment"), j.get("text")), ind=emp.get("industry", ""),
            tier=tier, fresh=fresh))
    return out, None


def fetch_ashby(emp):
    tok = emp["token"]
    st, d = board_get_json(f"https://api.ashbyhq.com/posting-api/job-board/{urllib.parse.quote(tok)}?includeCompensation=true")
    if st != 200 or not isinstance(d, dict):
        return None, f"HTTP {st}"
    out = []
    for j in d.get("jobs", []):
        if j.get("isListed") is False:
            continue
        pub = parse_dt(j.get("publishedAt"))
        upd = parse_dt(j.get("updatedAt"))
        fresh = max([x for x in (pub, upd) if x], default=None)
        if not fresh or fresh < CUTOFF:
            continue
        lines = html_lines(j.get("descriptionHtml") or "") or plain_lines(j.get("descriptionPlain"))
        locs = [j.get("location") or ""] + [s.get("location", "") for s in (j.get("secondaryLocations") or [])]
        pa = ((j.get("address") or {}).get("postalAddress") or {})
        country = (pa.get("addressCountry") or "").strip()
        extra_us = True if country in ("USA", "US", "United States", "United States of America") else \
            (False if country else None)
        region = (pa.get("addressRegion") or "").strip()
        loc_text = "; ".join(locs)
        remote_flag = (j.get("workplaceType") or "").lower() == "remote" or bool(j.get("isRemote"))
        state, remote, tier = place_info(loc_text, extra_us=extra_us, remote_flag=remote_flag)
        if not state and extra_us and region:
            state = region.upper() if region.upper() in VALID_CODES else STATE_CODES.get(region.lower(), "")
        sal = None
        comp = j.get("compensation") or {}
        rng = []
        for c in comp.get("summaryComponents") or []:
            if c.get("compensationType") == "Salary" and (c.get("currencyCode") or "USD") == "USD" and c.get("minValue"):
                iv = (c.get("interval") or "").upper()
                unit = "hour" if "HOUR" in iv else "month" if "MONTH" in iv else "week" if "WEEK" in iv else \
                    "day" if "DAY" in iv else "year"
                r = annualize(float(c["minValue"]), float(c.get("maxValue") or c["minValue"]), unit)
                if r:
                    rng.append(r)
        if rng:
            sal = (min(r[0] for r in rng), max(r[1] for r in rng), rng[0][2])
        if not sal:
            sal = salary_from_text("\n".join(t for _, t in lines))
        out.append(make_job(
            id=f"as:{tok}:{j.get('id')}", title=clean_title(j.get("title")), company=emp["name"],
            loc=short_loc(locs), state=state, remote=remote,
            min=sal[0] if sal else 0, max=sal[1] if sal else 0, period=sal[2] if sal else "",
            posted=(pub or upd).strftime("%Y-%m-%d"), url=j.get("jobUrl") or "", src="Ashby",
            lines=lines, dept=(j.get("department") or j.get("team") or "")[:60],
            type=job_type_from(j.get("employmentType"), j.get("title")), ind=emp.get("industry", ""),
            tier=tier, fresh=fresh))
    return out, None


def fetch_jobicy():
    """Jobicy remote jobs. Terms: credit Jobicy, keep links on the Jobicy job page, poll at most hourly."""
    out, cursor, pages = [], None, 0
    while pages < JOBICY_MAX_PAGES:
        url = "https://jobicy.com/api/v2/remote-jobs?count=100"
        if cursor:
            url += "&cursor=" + urllib.parse.quote(cursor)
        st, d = cached_get_json(url, JOBICY_CACHE, max_age=3600)
        if st != 200 or not isinstance(d, dict):
            if pages == 0:
                return None, f"HTTP {st}"
            break
        pages += 1
        for j in d.get("jobs") or []:
            pub = parse_dt(j.get("pubDate"))
            if not pub or pub < CUTOFF:
                continue
            geo = _WS.sub(" ", j.get("jobGeo") or "").strip()
            lines = html_lines(j.get("jobDescription") or "")
            extra_us = True if re.search(r"\b(usa|us|united states|anywhere)\b", geo, re.I) else None
            state, remote, tier = place_info(geo, extra_us=extra_us, remote_flag=True)
            if re.search(r"\banywhere\b", geo, re.I):
                tier = 0
            sal = None
            if j.get("salaryMin") and (j.get("salaryCurrency") or "USD").upper() == "USD":
                per = (j.get("salaryPeriod") or "yearly").lower()
                unit = "hour" if "hour" in per else "month" if "month" in per else "week" if "week" in per else "year"
                try:
                    sal = annualize(float(j["salaryMin"]), float(j.get("salaryMax") or j["salaryMin"]), unit)
                except (TypeError, ValueError):
                    sal = None
            if not sal:
                sal = salary_from_text("\n".join(t for _, t in lines))
            jt = j.get("jobType") or []
            ji = j.get("jobIndustry") or []
            out.append(make_job(
                id=f"jc:{j.get('id')}", title=clean_title(html.unescape(j.get("jobTitle") or "")),
                company=html.unescape(j.get("companyName") or "")[:60],
                loc=("Remote - " + geo if geo else "Remote")[:40], state=state, remote=1,
                min=sal[0] if sal else 0, max=sal[1] if sal else 0, period=sal[2] if sal else "",
                posted=pub.strftime("%Y-%m-%d"), url=j.get("url") or "", src="Jobicy", lines=lines,
                dept=html.unescape(ji[0] if isinstance(ji, list) and ji else str(ji or ""))[:60],
                type=job_type_from(jt[0] if isinstance(jt, list) and jt else str(jt or "")), ind="",
                tier=tier, fresh=pub))
        cursor = d.get("nextCursor")
        if not cursor or not d.get("hasMore"):
            break
        time.sleep(1)
    return out, None


def fetch_usajobs():
    key = os.environ.get("USAJOBS_KEY", "").strip()
    email = os.environ.get("USAJOBS_EMAIL", "").strip()
    if not key or not email:
        log("USAJOBS: no key yet - skipped")
        return [], "skipped (no key)"
    headers = {"Host": "data.usajobs.gov", "User-Agent": email, "Authorization-Key": key}
    out, page, pages = [], 1, 1
    rate = {"PA": "year", "PER YEAR": "year", "PH": "hour", "PER HOUR": "hour", "PM": "month", "PER MONTH": "month",
            "PW": "week", "PER WEEK": "week", "BW": "biweek", "PER TWO WEEKS": "biweek", "PD": "day", "PER DAY": "day"}
    while page <= min(pages, USAJOBS_MAX_PAGES):
        st, d = get_json(f"https://data.usajobs.gov/api/search?ResultsPerPage=500&Page={page}&WhoMayApply=public",
                         headers=headers)
        if st != 200 or not isinstance(d, dict):
            if page == 1:
                return None, f"HTTP {st}"
            break
        sr = d.get("SearchResult") or {}
        try:
            pages = int((sr.get("UserArea") or {}).get("NumberOfPages") or 1)
        except (TypeError, ValueError):
            pages = 1
        for item in sr.get("SearchResultItems") or []:
            m = item.get("MatchedObjectDescriptor") or {}
            pub = parse_dt(m.get("PublicationStartDate"))
            if not pub or pub < CUTOFF:
                continue
            det = ((m.get("UserArea") or {}).get("Details") or {})
            plocs = m.get("PositionLocation") or []
            disp = m.get("PositionLocationDisplay") or ""
            names = [p.get("LocationName", "") for p in plocs if p.get("LocationName")] or [disp]
            state = ""
            if len(plocs) == 1:
                sub = (plocs[0].get("CountrySubDivisionCode") or "").strip()
                state = STATE_CODES.get(sub.lower(), sub.upper() if sub.upper() in VALID_CODES else "")
            if not state and len(plocs) <= 1:
                state = detect_state(disp)
            remote = 1 if (det.get("RemoteIndicator") is True or re.search(r"remote|anywhere", disp, re.I)) else 0
            countries = {p.get("CountryCode") for p in plocs if p.get("CountryCode")}
            tier = 0 if (not countries or countries & {"United States", "US", "USA"}) else 2
            sal = None
            for r in m.get("PositionRemuneration") or []:
                code = (r.get("RateIntervalCode") or r.get("Description") or "").upper().strip()
                unit = rate.get(code)
                try:
                    lo, hi = float(r.get("MinimumRange") or 0), float(r.get("MaximumRange") or 0)
                except (TypeError, ValueError):
                    continue
                if unit and lo > 0:
                    sal = annualize(lo, hi or lo, unit)
                    if sal:
                        break
            text = "\n".join([det.get("JobSummary") or "", m.get("QualificationSummary") or "",
                              "\n".join("• " + x for x in (det.get("MajorDuties") or []) if isinstance(x, str))])
            lines = plain_lines(text)
            reqs = [_clip(x) for x in (det.get("KeyRequirements") or []) if isinstance(x, str) and x.strip()]
            sched = (m.get("PositionSchedule") or [{}])[0].get("Name", "") if m.get("PositionSchedule") else ""
            cat = (m.get("JobCategory") or [{}])[0].get("Name", "") if m.get("JobCategory") else ""
            jid = item.get("MatchedObjectId") or m.get("PositionID")
            job = make_job(
                id=f"us:{jid}", title=clean_title(m.get("PositionTitle")),
                company=(m.get("OrganizationName") or m.get("DepartmentName") or "US Government")[:60],
                loc=short_loc(names), state=state, remote=remote,
                min=sal[0] if sal else 0, max=sal[1] if sal else 0, period=sal[2] if sal else "",
                posted=pub.strftime("%Y-%m-%d"), url=m.get("PositionURI") or "", src="USAJOBS", lines=lines,
                dept=(m.get("DepartmentName") or cat)[:60], type=job_type_from(sched, m.get("PositionTitle")),
                ind="Government", tier=tier, fresh=pub)
            job["req_override"] = reqs
            out.append(job)
        page += 1
        time.sleep(1)
    return out, None


FETCHERS = {"greenhouse": fetch_greenhouse, "lever": fetch_lever, "ashby": fetch_ashby}
SRC_NAME = {"greenhouse": "Greenhouse", "lever": "Lever", "ashby": "Ashby"}


# ----------------------------------------------------------------------------------------------------------
# Build
# ----------------------------------------------------------------------------------------------------------
def load_employers():
    data = json.loads(EMPLOYERS_FILE.read_text(encoding="utf-8"))
    emps = data["employers"] if isinstance(data, dict) else data
    return [e for e in emps if e.get("ats") in FETCHERS and e.get("token")]


def run_employer(emp):
    try:
        jobs, err = FETCHERS[emp["ats"]](emp)
    except Exception as e:  # one bad board never stops the build
        jobs, err = None, f"{type(e).__name__}: {str(e)[:80]}"
    return emp, jobs, err


def shard_of(job_id):
    return zlib.crc32(job_id.encode("utf-8")) % SHARDS


def row_of(j):
    return [j["id"], j["title"], j["company"], j["loc"], j["state"], j["remote"], j["min"], j["max"], j["period"],
            j["posted"], j["url"], j["src"], j["kw"], j["shard"], j["ind"], j.get("type") or ""]


def dumps(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def write_atomic(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8", newline="\n")
    os.replace(tmp, path)


def previous_total():
    try:
        return len(json.loads(MAIN_FILE.read_text(encoding="utf-8")).get("m") or [])
    except Exception:
        return 0


def check_employers():
    emps = load_employers()
    log(f"Checking {len(emps)} boards ...")
    dead = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for emp, jobs, err in ex.map(run_employer, emps):
            if err or not jobs:
                dead.append((emp, err or "0 jobs in the last 60 days"))
    for emp, why in dead:
        log(f"  DEAD/EMPTY  {emp['ats']:<10} {emp['token']:<28} {emp['name']}  ({why})")
    log(f"{len(emps) - len(dead)} of {len(emps)} boards have recent jobs.")


def main():
    t0 = time.time()
    force = "--force" in sys.argv
    if "--check-employers" in sys.argv:
        check_employers()
        return 0

    emps = load_employers()
    log(f"Job Hub feed build - {NOW.strftime('%Y-%m-%d %H:%M UTC')} - {len(emps)} company boards")
    raw = {"Greenhouse": 0, "Lever": 0, "Ashby": 0, "Jobicy": 0, "USAJOBS": 0}
    failed = []
    all_jobs = []

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(run_employer, e) for e in emps]
        jc_fut = ex.submit(lambda: fetch_jobicy())
        us_fut = ex.submit(lambda: fetch_usajobs())
        for f in futs:
            emp, jobs, err = f.result()
            if err:
                failed.append(f"{emp['ats']}:{emp['token']} ({err})")
                continue
            raw[SRC_NAME[emp["ats"]]] += len(jobs)
            all_jobs.extend(jobs)
        for name, fut in (("Jobicy", jc_fut), ("USAJOBS", us_fut)):
            try:
                jobs, err = fut.result()
            except Exception as e:
                jobs, err = None, f"{type(e).__name__}: {str(e)[:80]}"
            if jobs is None:
                failed.append(f"{name} ({err})")
                log(f"{name}: FAILED - {err}")
                continue
            raw[name] += len(jobs)
            all_jobs.extend(jobs)
    log(f"Fetched in {time.time() - t0:.0f}s (last {MAX_AGE_DAYS} days): " +
        ", ".join(f"{k} {v}" for k, v in raw.items()))
    if failed:
        log(f"{len(failed)} source(s) failed: " + "; ".join(failed[:40]) + (" ..." if len(failed) > 40 else ""))

    # Drop duplicates (same company + title + location), keeping the newest copy.
    all_jobs.sort(key=lambda j: j["fresh"] or CUTOFF, reverse=True)
    seen, uniq = set(), []
    for j in all_jobs:
        if not j["title"] or not j["url"]:
            continue
        key = (j["company"].lower().strip(), j["title"].lower().strip(), j["loc"].lower().strip())
        if key in seen:
            continue
        seen.add(key)
        uniq.append(j)
    log(f"After de-dup: {len(uniq)} jobs")

    # Priority: US / US-remote first, then unknown, then outside the US; newest posting first inside each tier.
    uniq.sort(key=lambda j: (j["tier"], -int(j["posted"].replace("-", "") or 0),
                             -(j["fresh"] or CUTOFF).timestamp()))
    header_bytes = 600
    budget = MAX_BYTES - header_bytes
    kept, size, per_co = [], 0, {}
    for j in uniq:
        if len(kept) >= MAX_JOBS:
            break
        co = j["company"].lower()
        if per_co.get(co, 0) >= MAX_PER_COMPANY:
            continue
        j["kw"] = keywords(j["title"], j["lines"])
        j["shard"] = shard_of(j["id"])
        n = len(dumps(row_of(j)).encode("utf-8")) + 2
        if size + n > budget:
            break
        kept.append(j)
        size += n
        per_co[co] = per_co.get(co, 0) + 1
    dropped = len(uniq) - len(kept)
    kept.sort(key=lambda j: (j["posted"], (j["fresh"] or CUTOFF).timestamp()), reverse=True)

    # Guardrail: never ship a near-empty feed.
    prev = previous_total()
    if not kept:
        log("ERROR: 0 jobs built - keeping the old files.")
        return 2
    if prev and len(kept) < prev / 2 and not force:
        log(f"ERROR: new total {len(kept)} is under half of the previous {prev} - keeping the old files. "
            f"(Run with --force if the drop is expected.)")
        return 3

    made = NOW.strftime("%Y-%m-%dT%H:%M:%SZ")
    counts = {k: 0 for k in raw}
    for j in kept:
        counts[j["src"]] += 1

    # Detail shards (only rewritten when their content changed, to keep git history small).
    shards = [dict() for _ in range(SHARDS)]
    for j in kept:
        reqs = j.get("req_override") or requirements(j["lines"])
        # Summary skips the "about the company" / benefits / EEO blocks when the posting marks them with headings.
        body = kw_lines(j["lines"])
        body = body if sum(len(t) for _, t in body) >= 150 else j["lines"]
        shards[j["shard"]][j["id"]] = {"sum": summary(body), "req": reqs, "dept": j["dept"], "type": j["type"]}
    shard_bytes, changed = 0, 0
    for i, d in enumerate(shards):
        path = DETAIL_DIR / f"detail_{i:02d}.json"
        body = dumps(d)
        old_body = None
        if path.exists():
            try:
                old_body = dumps(json.loads(path.read_text(encoding="utf-8")).get("d"))
            except Exception:
                old_body = None
        text = '{"made":"' + made + '","d":' + body + "}\n"
        if old_body != body:
            write_atomic(path, text)
            changed += 1
        shard_bytes += path.stat().st_size

    # Skill word list for the phone (finds skill words in a pasted job or resume). Written every run.
    freq = {}
    for j in kept:
        for k in j["kw"]:
            if not k.startswith("years:"):
                freq[k] = freq.get(k, 0) + 1
    words, amap = vocab_words(freq)
    missing = [k for k in freq if k not in set(words)]
    if missing:
        log(f"WARNING: {len(missing)} keywords used in jobs.json are not in vocab.json: {missing[:10]}")
    write_atomic(DETAIL_DIR / "vocab.json", dumps({"made": made, "words": words, "map": amap}) + "\n")

    # Main file: one row per line (smaller git diffs).
    head = {"made": made, "credit": CREDIT, "counts": counts, "fields": FIELDS}
    head_txt = dumps(head)[:-1]
    rows_txt = ",\n".join(dumps(row_of(j)) for j in kept)
    write_atomic(MAIN_FILE, head_txt + ',"m":[\n' + rows_txt + "\n]}\n")

    main_size = MAIN_FILE.stat().st_size
    log("")
    log("Jobs in feed by source: " + ", ".join(f"{k} {v}" for k, v in counts.items()))
    log(f"Total {len(kept)} jobs (previous {prev}); {dropped} lower-priority jobs left out to fit "
        f"{MAX_JOBS} jobs / {MAX_BYTES / 1e6:.1f} MB")
    us_share = sum(1 for j in kept if j["tier"] == 0)
    log(f"US or remote-US: {us_share}; with a state: {sum(1 for j in kept if j['state'])}; "
        f"remote: {sum(1 for j in kept if j['remote'])}; with salary: {sum(1 for j in kept if j['min'])}")
    log(f"vocab.json {len(words)} words ({(DETAIL_DIR / 'vocab.json').stat().st_size / 1e3:.0f} KB)")
    log(f"jobs.json {main_size / 1e6:.2f} MB; {SHARDS} detail shards {shard_bytes / 1e6:.2f} MB total "
        f"({changed} rewritten, biggest {max((DETAIL_DIR / f'detail_{i:02d}.json').stat().st_size for i in range(SHARDS)) / 1e3:.0f} KB)")
    log(f"Done in {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
