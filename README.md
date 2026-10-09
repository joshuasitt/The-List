# The List

A personal daily/weekly work manager. Runs locally on your own machine — your data
lives in a SQLite file on your computer and never goes anywhere else.

## Sections

- **Today** — main priorities, smaller to-dos, tomorrow's priorities, mindful moments
- **Master** — the whole week at a glance, plus Top 3 and a bank of unscheduled items
- **Personal** — habit of the week, morning/night routine per day, your own rule lists
- **Weekly Checklist** — a numbered checklist you run through when prepping the week
- **Ideas** — a dump for anything you don't want to lose
- **Articles** — paste a URL or text and get a summary (needs an API key, see below)
- **Archive** — past weeks, with the ability to pull unfinished items back

## Setup

You need Python 3 and a terminal. macOS and Linux have both already.

```bash
git clone https://github.com/joshuasitt/The-List.git
cd The-List
./start.sh
```

Then open http://localhost:8000 in your browser.

`start.sh` creates a virtual environment, installs dependencies, and starts the app.
First launch creates an empty `data.db` — that file is your data, and it's gitignored
so it stays private to you.

To stop the app, press `Ctrl+C` in the terminal. To start it again later, run
`./start.sh` from this folder.

## Optional: article summaries

Summarizing articles uses the Anthropic API. Without a key, everything else works
normally — the Articles tab just won't summarize.

To enable it, copy `.env.example` to `.env` and add your key:

```bash
cp .env.example .env
```

```
ANTHROPIC_API_KEY=sk-ant-...
```

Get a key at https://console.anthropic.com. You can also paste it in the app via
"Set API key" at the bottom of the sidebar.

## Notes

- Data is stored in `data.db` in this folder. Back it up by copying that one file.
- Nothing is sent anywhere except article text, and only if you set an API key.
