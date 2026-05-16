# MirDB Homepage

Static product homepage for MirDB.

## Local development

```bash
cd homepage
npm install
npm run serve         # serves on http://localhost:5173
```

## Tests

```bash
cd homepage
npm test              # unit + integration via Vitest + jsdom
```

## Layout

The homepage is a component-driven static site. The `js/component-loader.js`
helper fetches `components/<name>/<name>.html` into the matching
`<div data-component="<name>"></div>` placeholder in `index.html`. Each
component owns its own folder under `components/`.
