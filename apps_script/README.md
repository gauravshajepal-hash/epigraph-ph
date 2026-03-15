# EpiGraph PH UI

This folder contains the Apps Script UI for inspecting the normalized HIV/STI extraction outputs.

## Local Preview

Serve the repository root and open the HTML file directly through a local web server:

```powershell
cd D:\EpiGraph_PH
python -m http.server 8000
```

Then open:

```text
http://localhost:8000/apps_script/Index.html
```

In local mode, the page reads directly from:

- `D:\EpiGraph_PH\data\normalized\dashboard_feed.json`
- `D:\EpiGraph_PH\data\normalized\insights.json`
- `D:\EpiGraph_PH\data\normalized\summary.json`
- `D:\EpiGraph_PH\data\normalized\claims.jsonl`
- `D:\EpiGraph_PH\data\normalized\review_queue.jsonl`

## Apps Script Deployment

1. Upload the exported JSON files to a single Google Drive folder.
2. Set `DATA_FOLDER_ID` in `D:\EpiGraph_PH\apps_script\Code.gs` to that Drive folder ID.
3. Copy `Code.gs`, `Index.html`, and `appsscript.json` into an Apps Script project.
4. Deploy the Apps Script project as a web app.

The sync pipeline now publishes the UI inputs automatically through `D:\EpiGraph_PH\modules\sync_sheets.py`.

## What The UI Shows

- Overview:
  - claims, observations, and review queue totals
  - yearly extraction activity
  - exported highlights
  - chart-ready trend cards
  - disease mix
  - documents with the most extracted claims
- Extracted Data:
  - searchable normalized claims
  - filters by disease, document type, metric, category, year, and chart-ready status
- Review Queue:
  - searchable review backlog
  - filters by reason, priority, disease, document type, metric, and year
