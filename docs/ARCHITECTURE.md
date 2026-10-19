# Job Apply Agent - System Architecture

For a high-level overview see `README.md`.

This document describes the internal architecture and technical design of Job Apply Agent.

---

## Overview

Job Apply Agent is a modular job discovery and application assistant that automates repetitive job search tasks while keeping a human in control of all final decisions.

The system performs four main tasks:

1. Discover remote job listings from multiple sources
2. Evaluate opportunities against a candidate profile
3. Assist with application form prefilling
4. Require human approval before submission

---

## System Pipeline

```text
python run_pipeline.py full-run
        |
        v
[FETCH]
  -> pull jobs from all enabled sources
  -> normalize to unified schema
  -> deduplicate (URL, external_id, company+title+canonical location, description fingerprint)
  -> insert new jobs into SQLite

        |
        v
[EVALUATE]
  -> compute remote_eligibility (rule-based classifier)
  -> compute rule-based fit_score
  -> assign rule_status (shortlisted / review / rejected)
  -> initialize or preserve final status
  -> select recommended resume

        |
        v
[ANALYZE]
  -> send review-status jobs to Ollama (local or cloud)
  -> structured JSON reasoning (fit score, strengths, gaps)
  -> conservative promotion/demotion
  -> persist LLM fields
```

Result persisted per job:

```text
job metadata
rule_status + fit_score
recommended_resume
LLM reasoning + confidence
final status
```

---

## State Model

Each job passes through three layers:

| Layer | Fields | Notes |
|---|---|---|
| Deterministic | `rule_status`, `fit_score`, `remote_eligibility`, `matched_skills`, `reject_code`, `reject_detail` | Always refreshed on re-evaluate |
| Semantic (LLM) | `llm_fit_score`, `recommendation`, `llm_confidence`, `llm_status`, `fit_explanation`, `llm_strengths`, `skill_gaps` | Set by Ollama; preserved across re-evaluate |
| Decision | `status` | Lifecycle: `new` → `review`/`shortlisted`/`rejected`; `rejected` ↔ `archived`; `expired` is prune-only. Initialized from rule layer; updated by LLM; manually overridable. `evaluate --all-jobs` skips `applied`, `deferred`, `archived`, and `expired`. |

Evaluation policy: `evaluate` always refreshes `rule_status` but only touches final `status` when the job has not already been refined by a successful LLM analysis. This prevents `evaluate --all-jobs` from erasing prior LLM promotions or manual decisions.

---

## Core Components

### Job Sources

Sources are implemented as `BaseConnector` subclasses in `connectors/`.

How to add a board (pagination caps, inspect, register, when to ask): [CONNECTOR_PLAYBOOK.md](CONNECTOR_PLAYBOOK.md).

**Aggregate job boards (JSON/RSS):**

| Connector | Source | Notes |
|---|---|---|
| `RemotiveConnector` | [Remotive](https://remotive.com) | General remote tech jobs |
| `RemoteOKConnector` | [RemoteOK](https://remoteok.com) | Only jobs with extractable ATS links (avoids subscription wall) |
| `WeWorkRemotelyConnector` | [WeWorkRemotely](https://weworkremotely.com) | Curated remote tech jobs (RSS). Apply is paywalled; scoring caps at review. |
| `ArbeitnowConnector` | [Arbeitnow](https://www.arbeitnow.com) | EU-focused remote jobs |
| `JobicyConnector` | [Jobicy](https://jobicy.com) | Remote tech jobs |
| `JobspressoConnector` | [Jobspresso](https://jobspresso.co) | Curated remote jobs |
| `DynamiteJobsConnector` | [Dynamite Jobs](https://dynamitejobs.com) | Remote-first jobs |
| `GetOnBoardConnector` | [GetOnBoard](https://www.getonbrd.com) | LatAm-focused; fully remote and hybrid (office-required hybrid dropped at persist) |
| `HimalayasConnector` | [Himalayas](https://himalayas.app) | Worldwide-only remote jobs |
| `AdzunaConnector` | [Adzuna](https://www.adzuna.com) | 8 countries (gb/de/fr/nl/at/be/au/ca), remote-filtered |
| `RealWorkFromAnywhereConnector` | [Real Work From Anywhere](https://www.realworkfromanywhere.com) | Worldwide-only curated remote jobs (RSS) |
| `EURemoteJobsConnector` | [EU Remote Jobs](https://euremotejobs.com) | European timezone remote jobs (RSS, `/job-listings/feed/`) |
| `RemoteAIJobsConnector` | Real Work From Anywhere — AI category | AI/ML-specific remote jobs (RSS) |
| `NodeskConnector` | [Nodesk](https://nodesk.co) | Guest Algolia `jobPosts` + JobPosting JSON-LD; engineering slug filter; skips expired/stale postings |
| `Remote100kConnector` | [Remote100K](https://remote100k.com) | Sitemap + JSON-LD; ATS apply URL extracted from page HTML; `?ref=` tracking params stripped |
| `RemoteJobsIoConnector` | [RemoteJobs.io](https://www.remotejobs.io/work-from-home/developer) | Next.js `__NEXT_DATA__` listing scrape; engineering title filter; remaining pages 4 at a time; apply paywalled |
| `RemoteJobsFinderConnector` | [RemoteJobsFinder](https://remotejobsfinder.co/en) | Guest public jobs API; profile `target_roles` + `engineering`; USA + hourly floor; mixed-date `skip` walk, 3 searches at a time; detail `descriptionHtml`; employer `jobUrl` |
| `DailyRemoteConnector` | [DailyRemote](https://dailyremote.com/remote-software-development-jobs) | Software-board HTML cards; relative dates; company/apply Premium-gated (review cap) |
| `ArcDevConnector` | [Arc.dev](https://arc.dev/remote-jobs) | Public `__NEXT_DATA__` board + engineering categories; Fast apply gated (review cap) |
| `FlexJobsConnector` | [FlexJobs](https://www.flexjobs.com) | Playwright login + homepage `/search` `__NEXT_DATA__`; opt-in `--source flexjobs`; apply paywalled (review cap) |
| `YCombinatorConnector` | [Y Combinator jobs](https://www.ycombinator.com/jobs/role/software-engineer/remote) | Guest Inertia `jobPostings`; truncated mixed-date list, no age filter; apply account-gated (review cap) |
| `WaasConnector` | [Work at a Startup](https://www.workatastartup.com/companies?jobType=fulltime&remote=yes&remote=only&role=eng&sortBy=created_desc) | Playwright login (`WAAS_EMAIL` / `WAAS_PASSWORD`); infinite-scroll full-time remote eng directory then `/companies/fetch` jobs; apply account-gated (review cap) |
| `TechJobsForGoodConnector` | [Tech Jobs for Good](https://techjobsforgood.com/jobs/?q=&remote_jobs=on&page=2&sort_by=date) | Guest remote HTML list + JobPosting JSON-LD; `sort_by=date` newest-first; apply account-gated (review cap) |
| `RemoteComConnector` | [Remote](https://remote.com/jobs/all?workplaceLocation=remote&country=anywhere&country=USA) | Guest RSC `jobsData.jobs` list + JobPosting JSON-LD; page 1 mixed, page 2+ newest-first (cap 30); apply account-gated (review cap) |
| `RemoteCoConnector` | [Remote.co](https://remote.co/remote-jobs/search?remoteoptions=100%25%20Remote%20Work&useclocation=false&anywhereinus=1) | Guest `__NEXT_DATA__` search (exact listing URL; first 10 pages); Chrome-TLS `curl_cffi`; apply/company often empty (review cap) |
| `DevRemoteConnector` | [DevRemote](https://devremote.io/) | Guest `POST /api/jobs/filter` (`pageSize`/`skip`); newest-first stale-page stop |
| `WeAreDevelopersConnector` | [WeAreDevelopers](https://www.wearedevelopers.com/jobs?q=&country=US) | Guest `/jobs.md` US list; newest-first stale-page stop; listing then parallel details |
| `AnywherePositionsConnector` | [Anywhere Positions](https://www.anywherepositions.com/) | Guest `api-jobs` `search`+`regions` (Anywhere and US); `target_roles` plus `software` and `AI`, 4 at a time |
| `RemoteRocketshipConnector` | [Remote Rocketship](https://www.remoterocketship.com/remote-jobs/?page=1&sort=DateAdded) | Guest `POST /api/fetch_job_openings/`; page 1 / 40 items; 64 title×location×seniority combos |
| `DiceConnector` | [Dice](https://www.dice.com/jobs?filters.workplaceTypes=Remote%7CHybrid) | Guest MCP `search_jobs`; profile roles+keywords; newest-first stale-page stop; skip detail when the listing matches a stored posting |
| `WorkableConnector` | [Workable jobs](https://jobs.workable.com/search?day_range=7&workplace=remote&workplace=hybrid&experience=mid_senior_level&experience=director) | Guest `/api/v1/jobs`; profile `target_roles` + `engineering`; mixed-date `pageToken` walk; apply on jobs.workable.com |
| `RemoteScout24Connector` | [RemoteScout24](https://remotescout24.com/en/jobs/search?page=1&country=us&worktype=remote&experience=professional%2Csenior%2Cmanager&jobtitle=engineer) | Guest SSR search + `GET /api/jobs/job?id=`; profile `target_roles` + `engineer`; remote only; skip known URLs before detail; stop when a page has no fresh date; employer `targetUrl` |
| `TrulyRemoteConnector` | [Truly Remote](https://trulyremote.co/?category=Development&locations=North+America%252BAnywhere+in+the+world) | Guest `POST /api/getListing`; Development + North America/Anywhere; later pages send `offset`+`industry` cursor; newest-first stale-page stop; teaser `listingSummary`; employer `roleApplyURL` |
| `AIJobsConnector` | [AIJobs.com](https://www.aijobs.com/jobs?remote=1&order=posted_at) | Guest remote Date HTML list; engineering title filter; newest-first stale-page stop; JobPosting JSON-LD; apply 302 to employer ATS |
| `AIJobsAIConnector` | [AIJobs.ai](https://aijobs.ai/remote) | Guest Latest Jobs HTML list (skip Featured); engineering title filter; newest-first stale-page stop; job-page description; employer ATS href |
| `JustJoinConnector` | [JustJoin](https://justjoin.it/job-offers/remote?remote-work-options=hybrid&experience-levels=mid,senior,team-leader-manager&languages=en&sortBy=newest) | Poland-focused; opt-in (`--source justjoin`, not in `all`). Guest `/api/candidate-api/offers`; mid/senior/lead + English; remote+hybrid; newest-first cursor |
| `BrenxorConnector` | [Brenxor](https://brenxor.com/remote-software-development-jobs) | Guest mid/senior/lead × Anywhere/USA HTML lists; engineering title filter; newest-first stale-page stop; 500 retry then skip combo; JobPosting JSON-LD; apply 302 to employer ATS |
| `JobgetherConnector` | [Jobgether](https://jobgether.com/search-offers?sort=date&location=anywhere) | Guest `GET /api/v1/jobs`; `sort=date` + `locations=anywhere`; engineering keyword walks; page cap 10×25; newest-first stale-page stop; JobPosting JSON-LD |
| `PostJobFreeConnector` | [PostJobFree](https://www.postjobfree.com/jobs?t=software+engineer&l=United+States&r=100) | Guest HTML `t=` title walks + `l=United States` + `r=100`; mixed-date `p=` walk (empty/repeat stop); listing skip before detail |
| `TopSalariesConnector` | [TopSalaries](https://topsalaries.tech/) | Guest homepage SSR (all cards); engineering title filter; newest-first first-stale-card stop; listing skip before detail; JobPosting JSON-LD; employer ATS href |
| `LevelsFyiConnector` | [Levels.fyi](https://www.levels.fyi/jobs?locationSlug=united-states&sortBy=date_published&workArrangements=remote) | Guest Playwright Chrome (AWS WAF); skip Promoted; engineering title filter; newest-first stale-page stop; listing skip before detail; employer ATS `applicationUrl`; LinkedIn-only apply dropped |
| `WorkewConnector` | [Workew](https://workew.com/remote-jobs/) | Guest `GET /wp-json/wp/v2/job-listings`; engineering title filter; newest-first first-stale-job stop; listing skip before persist; employer ATS `meta._application`; Workew/LinkedIn apply dropped |
| `LaddersConnector` | [Ladders](https://www.theladders.com/jobs/searchresults-jobs?keywords=Software%20Engineer&sortBy=PUBLICATION_DATE&daysPublished=7&remoteFlags=Remote) | Guest Playwright Chrome (Cloudflare); no `/api/*`; unique `target_roles` + `software engineer`; engineering title filter; newest-first, 5 pages per query; hung listing retries on a new tab; listing skip before detail |
| `StartupJobsConnector` | [Startup.jobs](https://startup.jobs/remote-jobs?w=remote&c=full-time%2Cpart-time%2Ccontractor&since=7d&page=1) | Guest Algolia JSON (homepage search-only key); Cloudflare listing unused; no `q=`; `published_at_i` from `max_job_age_days`; engineering title filter; mixed-date page walk; listing skip before persist; JobPosting JSON-LD over HTTP; never `/apply/` |
| `FourDayWeekConnector` | [4DayWeek](https://4dayweek.io/job-search?category=engineering%2Cdata%2Cdevops&work_arrangements=remote&worldwide_only=true) | Guest `GET /api/v2/jobs`; `sort=date` newest-first; `posted_after` from `max_job_age_days`; no `level`/`q=`; engineering title filter; first-stale-job stop; list JD |
| `BuiltinConnector` | [BuiltIn](https://builtin.com/jobs/remote/engineering?daysSinceUpdated=3&city=&state=&country=USA&allLocations=true) | Guest listing HTML over HTTP; remote engineering / AI / data / PM categories; `daysSinceUpdated` from `max_job_age_days`; no `search=`/seniority path; engineering title filter; mixed-date `?page=` walk per category; listing skip before persist; `requests` never `/apply/`; CloakBrowser (`CLOAKBROWSER_USER_DATA`, default `.cloak-builtin`, optional `CLOAKBROWSER_PROXY`) opens `/job/…` only for the off-site Apply URL |
| `VirtualVocationsConnector` | [VirtualVocations](https://www.virtualvocations.com/jobs/s-date) | Guest job sitemap; newest-first `lastmod`; first-stale stop; engineering title from slug; listing skip before detail; guest Job Summary |
| `Up2StaffConnector` | [Up2Staff](https://up2staff.com/) | Guest WPJM `GET /jm-ajax/get_listings/` (`orderby=date`); unfiltered newest-first + engineering title filter; stop at first job older than `max_job_age_days`; listing skip before persist |
| `RemoteArmyConnector` | [RemoteArmy](https://remotearmy.io/) | Guest eng category SSR; `/search?term=` unbound over HTTP; unique `target_roles` title filter; newest-first first-stale stop; listing skip before persist |
| `RemoteYeahConnector` | [RemoteYeah](https://remoteyeah.com/remote-lead+mid-level+senior+ai-native-engineer+android-developer+artificial-intelligence-engineer+backend-engineer+big-data-engineer+cloud-architect+cloud-engineer+cross-platform-mobile-developer+data-analyst+data-engineer+database-administrator+database-engineer+devops-engineer+frontend-engineer+full-stack-engineer+game-developer+game-engineer+infrastructure-engineer+ios-developer+machine-learning-engineer+machine-learning-ops-engineer+platform-engineer+prompt-engineer+qa-engineer+site-reliability-engineer+software-architect+software-engineer+solution-architect+solutions-engineer+systems-engineer+web-developer-jobs-in-united-states) | Guest listing-path RSS; lead/mid/senior + US engineering slugs; newest-first first-stale stop; engineering title filter; listing skip before persist; optional OS Chrome `REMOTEYEAH_CHROME_USER_DATA` POSTs `/jobs/…/apply` and stores off-site `Location` |
| `RemoteSourceConnector` | [RemoteSource](https://www.remotesource.com/jobs?jobCategory=Engineering+%26+Development%2CData+%26+Analytics&remoteFirstOnly=true&postedWithin=7d&search=Software+Engineer) | Guest `/api/jobs` (SSR filters unbound); unique `target_roles` as `search=`; `postedWithin` 7d/30d; newest-first `offset` + first-stale; listing skip before persist; external apply |
| `RemoteJobsConnector` | [RemoteJobs.org](https://remotejobs.org/remote-jobs?category=programming) | Guest `/api/v1/jobs`; `programming`/`data-science`/`devops`; unique `target_roles` as `q=`; newest-first `offset` + first-stale; listing skip before persist |
| `RemoteFrontJobsConnector` | [RemoteFrontJobs](https://www.remotefrontendjobs.com/) | Guest `/api/jobs?limit=5000` (SSR filters unbound); mixed `isoDate` date-filter; no `seniority=`; engineering title filter; listing skip before persist; external apply link |
| `RemoteFrontConnector` | [RemoteFront](https://www.remotefront.com/remote-jobs?page=1&job_function=eng) | Guest Playwright Chrome (Vercel checkpoint); `job_function=eng`; mixed-date `page=` walk; engineering title filter; listing skip before detail; employer apply URL when present |
| `RemoteWlbConnector` | [RemoteWLB](https://remotewlb.com/jobs) | Guest monthly job sitemaps; reverse oldest-first `lastmod`; first-stale stop; engineering title filter; job pages 4 at a time; JobPosting JSON-LD |
| `OmniJobsConnector` | [OmniJobs](https://omnijobs.io/en/search?location=US&locationType=remote&jobFunction=software+development) | Guest Playwright Chrome (Vercel checkpoint); remote + software development + US/LATAM/world/Europe; newest-first first-stale when `Opened` dates descend; stop at OmniJobs Pro; engineering title filter; listing skip before detail |
| `TechCareersConnector` | [TechCareers](https://www.techcareers.com/jobs/search?soid=1&k=&kt=2&l=&r=40&dp=3&s=1&rem=true) | Guest `/jobs/search/rss`; `rem=1`; unique `target_roles` + `software engineer` merged by id; `dp` from `max_job_age_days`; mixed `pubDate` `pg` walk; engineering title filter; no detail HTTP; unwrapped apply URL |
| `SmartRecruitersConnector` | [SmartRecruiters](https://jobs.smartrecruiters.com/?keyword=software%20engineer&locationType=REMOTE) | Guest `/sr-jobs/search`; `locationType=REMOTE`; unique `target_roles` + `software engineer` merged by id; newest-first `releasedDate` + first-stale; posting JSON for description |
| `HubstaffTalentConnector` | [Hubstaff Talent](https://hubstafftalent.net/search/jobs?search%5Bkeywords%5D=Software+Engineer&search%5Bcountries%5D%5B%5D=US&search%5Bsort_by%5D=date_added) | Guest XHR `/search/jobs`; unique `target_roles` as `search[keywords]`; US + pay $50–100+/hr; newest-first `date_added` + first-stale; location `Remote`; listing skip before persist |
| `TryRemotelyConnector` | [TryRemotely](https://tryremotely.com/jobs?keyword=&location%5B%5D=north-america&work_model%5B%5D=remote&sort_by=date) | Guest `/api/v1/job-listings` (`offset`+`limit` only); `workModel=Remote` and engineering title read per row; newest-first `pubDate` + first-stale; listing skip before persist |
| `FindMyRemoteConnector` | [Find My Remote](https://findmyremote.ai/jobs?location=us&location=ca&category=engineering) | Guest `/api/jobs`; category + `us`/`ca` + employment type OR; newest-first `createdAt` cursor + first-stale; detail JSON for place text; employer apply URL |
| `JobDivaConnector` | [JobDiva](https://www1.jobdiva.com/portal/#/) | Guest `POST searchjobsportal`; empty keyword + `onsiteFlex=-3`; newest-first `postDate` `from`/`to` + first-stale; no title filter; listing description; portal apply URL |

**Direct ATS connectors:**

| Connector | Source | Notes |
|---|---|---|
| `DirectATSConnector` | Ashby / Greenhouse / Lever / Workable | Curated `target_companies` list from `profile.yaml`; ATS auto-detected from `careers_url` host |
| `AshbyConnector` | Ashby API | In-window aggregator Ashby URLs not in the Direct ATS list; runs after aggregators |
| `GreenhouseConnector` | Greenhouse API | In-window aggregator Greenhouse URLs not in the Direct ATS list |
| `LeverConnector` | Lever API | In-window aggregator Lever URLs; runs after aggregators |

**Direct ATS host routing:**

```
jobs.ashbyhq.com          → Ashby  (GET /posting-api/job-board/{slug})
boards.greenhouse.io       → Greenhouse  (GET /v1/boards/{slug}/jobs)
job-boards.greenhouse.io   → Greenhouse
jobs.lever.co              → Lever  (GET /v0/postings/{slug}?mode=json)
apply.workable.com         → Workable  (POST /api/v3/accounts/{slug}/jobs)
```

### Remote Eligibility Filter (`utils/remote_filter.py`)

`classify_remote_eligibility(job, profile)` returns `accept`, `review`, or `reject`.

Key rejection patterns (in order):

1. `raw_location` matches known US-only location strings (`usa`, `united states`, `us`) **unless** the profile `accepted_regions` or `work_authorization` includes the US
2. Greenhouse-style prefixes: `us-remote`, `us-east`, `us-west`, etc. (same US-acceptance gate)
3. US substrings in location (`united states`, ` usa`, `(u.s.)`, `(us)`, etc.) unless a broad-region override (`worldwide`, `emea`, etc.) is also present, or the profile accepts US
4. `Remote - [Country]` pattern where the country is not in the user's `accepted_regions`
5. `remote_only`: drop hybrid when the posting requires regular office attendance (`3 days in office`, `hybrid 3/2`, `on-site required`, RTO), regardless of city. Place-tied hybrid / “Remote available” is kept only when it matches `personal.location` (same US state/city, or Remote(US)/Remote(CA) with no other state). Fully remote and worldwide are not place-tied.
6. Description contains hard-reject keywords (`security clearance required`, plus `us only` / `must reside in the us` when the profile does not accept US)
7. Geographic-only locations with no `remote`/`hybrid`/`worldwide`/`global` hint and no accepted-region match

### Seniority Filter (`utils/seniority.py`)

`seniority_exclusion(job, profile)` skips jobs whose detected level is outside `seniority.preferred` + `seniority.acceptable`. Title words first (`intern`, `junior`, `mid`, `senior`, `staff`, `lead`, `principal`, `director`); else a `Level:` line in the description. Unstated seniority is kept. Staff also allows principal. No `seniority` block in the profile means no filter.

### Ingestion Pipeline

`run_pipeline.py` orchestrates:

- fetching from each connector
- normalizing via `connector.normalize(raw_job)`
- skipping jobs that fail `utils/job_inclusion.py` (fully remote vs place-tied hybrid/remote vs office-required; profile seniority; posting language; accepted regions)
- deleting already-stored jobs that fail the same rules (except applied)
- deduplication via `utils/dedup.py` (URL, external_id, normalized company+title+location, long-description fingerprint across URLs)
- upsert into `jobs` table

### Database Layer

- SQLite via SQLAlchemy
- Migrations via Alembic
- Tables: `jobs`, `application_history`, `pipeline_runs`, `interview_prep_sheets`, `company_profiles`

### Job Apply Intelligence Engine (`utils/scoring.py`, `utils/application_filter.py`)

Evaluates job relevance:

- remote eligibility classification
- rule-based `fit_score` from skill overlap, title match, seniority
- blacklisted company filtering
- resume selection (`utils/resume_selector.py`) — matches job keywords against resume tags

### LLM Job Analysis (`utils/llm_analysis.py`)

Uses **Ollama** (`/api/chat`) on the host selected by `config.OLLAMA_MODE` (`local` → `localhost:11434`, `cloud` → `https://ollama.com` with `OLLAMA_API_KEY` in `.env`):

- structured JSON output with fit score, strengths, gaps, recommendation
- conservative status updates: only promotes review→shortlist or review→rejected
- malformed or failed responses do not break the pipeline
- LLM output stored in dedicated fields; never overwrites `rule_status`

### Company research (`utils/company_research.py`)

On-demand from the Job Panel Company tab (not a pipeline stage):

- cache key is a suffix-stripped company name (`Acme Inc` == `Acme`); a second job at the same employer reuses the row
- official site from JD links, Clearbit autocomplete, Wikidata P856, or DuckDuckGo (never Wikipedia)
- homepage/about + Wikidata facts summarized by Ollama into `company_profiles`

### Application Prefill Agent (`utils/form_inspector.py`, `utils/form_filler.py`)

`open-job` opens a shortlisted job in a Playwright browser window and:

1. Detects ATS from the job URL (`utils/ats_detector.py`)
2. For listing-page URLs (Nodesk, RemoteOK, etc.) follows the employer apply link via `extract_apply_url` before scanning the form
3. Scans visible form fields — captures `tag`, `type`, `name`, `id`, `placeholder`, `label` (via `<label for=...>`, `aria-label`, or `aria-labelledby`)
4. Builds DOM context for unlabeled fields by walking up the element tree (handles Ashby EEO comboboxes and non-native dropdowns)
5. Fills text fields by matching labels against `_TEXT_RULES` (name, email, phone, LinkedIn, GitHub, location, current company, organization, etc.)
6. Smart phone formatting: strips non-digits, applies E.164 or national format as required by the field
7. Age-group dropdowns: selects the matching range from `profile.yaml`
8. Uses the local LLM to generate answers for freeform textarea questions (motivation, cover letters, custom prompts)
9. Gmail IMAP interception: polls inbox for ATS verification emails and auto-fills the security code
10. Handles checkboxes (skills, consent, availability) and radio groups (timezone, career type)
11. Uploads the recommended resume via native file dialog interception with detailed per-attempt logging
12. Bot-protected sites (RemoteOK, WeWorkRemotely, Jobicy) open in the system browser without prefill

### Human Approval Gate

All submissions are manual. The pipeline:

- opens the form in a visible browser window
- prefills what it can
- waits for the user to review, edit, and submit
- prompts to mark the job as `applied` after submission

`application_history` is updated on mark-applied.
