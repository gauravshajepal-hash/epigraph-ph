# Deploying The Web App

The web app is now deployable as a static site. Build the bundle first:

```powershell
cd D:\EpiGraph_PH
python .\build_web_bundle.py
```

This writes a hostable site to [D:\EpiGraph_PH\dist](/D:/EpiGraph_PH/dist) with:
- [D:\EpiGraph_PH\dist\index.html](/D:/EpiGraph_PH/dist/index.html)
- [D:\EpiGraph_PH\dist\data\normalized](/D:/EpiGraph_PH/dist/data/normalized)
- publication SVG and PNG figures under [D:\EpiGraph_PH\dist\data\normalized\publication_figures](/D:/EpiGraph_PH/dist/data/normalized/publication_figures)

Test locally:

```powershell
cd D:\EpiGraph_PH
python -m http.server 8000 --directory dist
```

Open:

```text
http://127.0.0.1:8000
```

## Recommended hosting options

### 1. Firebase Hosting
Best Google-native option for this app.

Use when:
- you want a Google-hosted public site
- you want a simple deploy flow from the `dist` folder

Basic flow:

```powershell
npm install -g firebase-tools
firebase login
firebase init hosting
firebase deploy
```

This repo already includes [D:\EpiGraph_PH\firebase.json](/D:/EpiGraph_PH/firebase.json) pointing Hosting at `dist`.

Official docs:
- [Firebase Hosting overview](https://firebase.google.com/docs/hosting)
- [Firebase Hosting quickstart](https://firebase.google.com/docs/hosting/quickstart)

### 2. GitHub Pages
Best free permanent host for this app.

This repo now includes:
- [D:\EpiGraph_PH\.github\workflows\pages.yml](/D:/EpiGraph_PH/.github/workflows/pages.yml)
- [D:\EpiGraph_PH\dist](/D:/EpiGraph_PH/dist)

The workflow deploys the committed `dist` folder on every push to `main`.

Basic flow:

```powershell
cd D:\EpiGraph_PH
python .\build_web_bundle.py
git add .
git commit -m "Initial GitHub Pages site"
git remote add origin https://github.com/<your-account>/<your-repo>.git
git push -u origin main
```

Then in GitHub:
- open `Settings > Pages`
- set `Build and deployment` source to `GitHub Actions` if it is not already

After the workflow completes, the site will be available at:

```text
https://<your-account>.github.io/<your-repo>/
```

Official docs:
- [GitHub Pages](https://docs.github.com/en/pages)
- [Using custom workflows with GitHub Pages](https://docs.github.com/en/pages/getting-started-with-github-pages/using-custom-workflows-with-github-pages)

### 3. Cloudflare Pages
Best if you want fast static hosting and direct upload or Git-based deploys.

Use when:
- you want a CDN-backed public site
- you want simple drag-and-drop or Git integration

Basic flow:
- build `dist`
- create a Pages project
- upload `dist` or connect the repo and use `dist` as output

Official docs:
- [Cloudflare Pages](https://developers.cloudflare.com/pages/)
- [Get started with Pages](https://developers.cloudflare.com/pages/get-started/)

## Not recommended as the main public host

### Google Apps Script Web App
The repo still supports Apps Script, but it is not the best primary host for the publication dashboard.

Reason:
- this dashboard is now best treated as a static site with exported JSON and publication figures
- Apps Script is still useful if you want a Drive-backed internal view, but it adds backend coupling for a public site

Official docs:
- [Apps Script web apps](https://developers.google.com/apps-script/guides/web)

## Recommended path

For this project, the best order is:

1. Build `dist`
2. Test it locally
3. Deploy `dist` to Firebase Hosting or Cloudflare Pages

If you want the most Google-aligned option, use Firebase Hosting.
