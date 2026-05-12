# MirDB Homepage

Static marketing homepage for [MirDB](https://github.com/yetone/mirdb) — a persistent
key-value store with the Memcached protocol, written in Rust.

## Layout

- `public/` — final HTML shell, favicon, robots.
- `src/components/<name>/` — per-section markup + scoped CSS.
- `src/styles/` — global tokens, base reset, layout, responsive, accessibility.
- `src/scripts/` — clipboard helper, toast helper, navigation, animations.
- `src/data/content.json` — source-of-truth for copy and links.
- `tests/unit/` — Jest + jsdom unit tests for each component.
- `tests/helpers/` — shared test utilities (partial loader, clipboard mock).

## Develop

```bash
cd homepage
npm install
npm test
```

The homepage is intentionally vanilla HTML / CSS / JavaScript so it can be
statically generated and served from any CDN (no build step required to ship).
