# Job Hub
Save path: C:/MarvelApps/jobhub/README.md

Live (once pushed): https://smarvel1963-ops.github.io/jobhub/ - app #12 of the Marvel Corp Hub family (Scott 2026-11-14 "ad job hunt
hub to list of aps", 11/15 his 6-screen board, 11/17 "Job Hub"). Board + plan: C:/MarvelApps/docs/jobhub_design_2026-11-15/.

This repo is Job Hub's OWN shell plus its daily job feed: index.html, manifest.json, icons, sw.js, privacy.html, jobs.json +
jobs/ (built by tools/build_jobs.py) and the GitHub robot that rebuilds them. The app itself is the shared Hub engine in the Day Hub
repo, run with window.DH_MODE = "job" plus job.js and jobtiles.css (Job Hub's look, loaded only here).

- Screens (the board): Career Command Center (Home), Job Radar, Is This Job Right for Me?, Resume Studio, Your Opportunity Pipeline,
  Smart Alerts. Plus What I'm Looking For (the weights), Check a Job I Found (paste any job from any site), More.
- The jobs: free, legal sources only - companies' own job boards (Greenhouse, Lever, Ashby), Jobicy remote jobs (credited, links go
  to Jobicy) and USAJOBS federal jobs (needs a free key: GitHub secrets USAJOBS_KEY + USAJOBS_EMAIL; without them the robot skips
  USAJOBS and says so). No LinkedIn / Indeed bots. Remotive and The Muse were checked and left out (terms / blocked).
- Rules kept from the plan: never invent experience; the match score shows its evidence (the job's words on the resume, and the ones
  missing); unknown stays unknown; Apply always opens the employer's own posting; resume tips are plain rules, no AI.
- Matching runs on the phone. Weights start at Scott's: pay 30, qualifications 25, remote 20, stability 15, growth 10.
- Own data on the phone (jobhub.v1, S.jh), in the normal backup. Current pay is never sent to Ask.
- Reminders: an interview the evening before (6 PM) and 2 hours before; a follow-up date at 10 AM.
- Icons: gold briefcase over the sunset city, drawn in code - docs/jobhub_design_2026-11-15/make_jobhub_icons.py.
- The feed (11/17 first run): 7,948 US / US-remote jobs from 245 employers (tools/employers.json, 250 boards) + Jobicy, 79% with
  pay, newest first, jobs.json capped at 3 MB (608 KB over the wire; MAX_BYTES in build_jobs.py). Arkansas is thin (6 jobs) until
  USAJOBS is on. About 2-3 minutes a run. Safety: under half of yesterday's total, or zero, stops the build.
- Publishing: .github/workflows/jobs.yml builds the feed and publishes the WHOLE site to Pages from the run (daily, on every push to
  main, and by hand). The feed is never committed (.gitignore) - it changes about 13 MB a day. A failed build still publishes, with
  yesterday's live feed, and the run shows as failed. Needs Settings -> Pages -> Source = "GitHub Actions".
- Release order: create the repo, set Pages Source to GitHub Actions, push THIS repo first (wait for the run), then the Day Hub repo.
- Tests: the Day Hub repo (python tests/test_job.py; plus tests/run_tests.py for the whole engine).
