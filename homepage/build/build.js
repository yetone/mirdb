#!/usr/bin/env node
/**
 * Static site build script for the MirDB homepage.
 *
 * Owner: Scenario 10 — Performance & static generation.
 *
 * Responsibilities:
 *   - Parse public/index.html for all <link rel="stylesheet"> and <script src>
 *     references that point inside the src/ tree.
 *   - Concatenate (in document order) the referenced CSS files into one
 *     dist/styles.css bundle and the referenced JS files into one
 *     dist/main.js bundle. Light minification (strip CSS/JS comments and
 *     collapse runs of whitespace inside CSS) keeps gzip size small without
 *     pulling in heavy tooling.
 *   - Rewrite the HTML so the bundle replaces every individual <link>/<script>
 *     and so asset references (logo, favicon, icons) point under
 *     assets/ inside dist/.
 *   - Copy referenced assets (favicon, logo, icons used by SVG <use> or CSS
 *     url(...)) into dist/assets/.
 *
 * The output is fully self-contained, deterministic, and idempotent —
 * running the script repeatedly against unchanged inputs produces byte-
 * identical artifacts. No timestamps, hashes, or platform-dependent
 * separators are embedded.
 *
 * Usage:
 *   node homepage/build/build.js [--out dist] [--root <homepage-dir>] [--quiet]
 *
 * Exit status: 0 on success, non-zero on failure.
 */

'use strict';

const fs = require('fs');
const path = require('path');

const DEFAULT_HOMEPAGE_ROOT = path.resolve(__dirname, '..');

function parseArgs(argv) {
  const out = { outDir: 'dist', root: null, quiet: false };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--out' || arg === '-o') {
      out.outDir = argv[++i];
    } else if (arg === '--root' || arg === '-r') {
      out.root = argv[++i];
    } else if (arg === '--quiet' || arg === '-q') {
      out.quiet = true;
    }
  }
  return out;
}

function minifyCss(source) {
  return source
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .replace(/\s*\n\s*/g, '\n')
    .replace(/[ \t]+/g, ' ')
    .replace(/\s*([{};,:>+~])\s*/g, '$1')
    .replace(/;\}/g, '}')
    .replace(/\n+/g, '\n')
    .trim();
}

function minifyJs(source) {
  return source
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .split('\n')
    .map((line) => {
      const stripped = line.replace(/(^|[^:'"])\/\/.*$/, '$1');
      return stripped.replace(/[ \t]+$/g, '');
    })
    .filter((line) => line.length > 0)
    .join('\n')
    .trim();
}

function rmrf(target) {
  if (!fs.existsSync(target)) return;
  const stat = fs.lstatSync(target);
  if (stat.isDirectory()) {
    for (const entry of fs.readdirSync(target).sort()) {
      rmrf(path.join(target, entry));
    }
    fs.rmdirSync(target);
  } else {
    fs.unlinkSync(target);
  }
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function collectMatches(html, regex) {
  const results = [];
  let m;
  const re = new RegExp(regex.source, regex.flags.includes('g') ? regex.flags : regex.flags + 'g');
  while ((m = re.exec(html)) !== null) {
    results.push({ index: m.index, match: m[0], href: m[1] });
  }
  return results;
}

function normalizeHomepageRef(href) {
  if (!href) return null;
  if (/^(https?:|data:|mailto:|#)/i.test(href)) return null;
  const trimmed = href.replace(/^\.\.\//, '').replace(/^\.\//, '');
  return trimmed;
}

function loadFileOrFail(absPath, kind) {
  if (!fs.existsSync(absPath)) {
    throw new Error(`build: missing ${kind} dependency at ${absPath}`);
  }
  return fs.readFileSync(absPath, 'utf8');
}

function build(options) {
  const homepageRoot = options.root ? path.resolve(options.root) : DEFAULT_HOMEPAGE_ROOT;
  const outDir = path.isAbsolute(options.outDir)
    ? options.outDir
    : path.resolve(process.cwd(), options.outDir);

  const indexHtmlPath = path.join(homepageRoot, 'public', 'index.html');
  if (!fs.existsSync(indexHtmlPath)) {
    throw new Error(`build: cannot find ${indexHtmlPath}`);
  }
  const rawHtml = fs.readFileSync(indexHtmlPath, 'utf8');

  const cssLinkRe = /<link\s+[^>]*rel=["']stylesheet["'][^>]*href=["']([^"']+)["'][^>]*>/gi;
  const scriptTagRe = /<script\s+[^>]*src=["']([^"']+)["'][^>]*>\s*<\/script>/gi;

  const cssHits = collectMatches(rawHtml, cssLinkRe).filter((h) => normalizeHomepageRef(h.href));
  const jsHits = collectMatches(rawHtml, scriptTagRe).filter((h) => normalizeHomepageRef(h.href));

  if (cssHits.length === 0) {
    throw new Error('build: no <link rel="stylesheet"> references found in index.html');
  }
  if (jsHits.length === 0) {
    throw new Error('build: no <script src> references found in index.html');
  }

  const cssParts = [];
  const cssSeen = new Set();
  for (const hit of cssHits) {
    const rel = normalizeHomepageRef(hit.href);
    if (cssSeen.has(rel)) continue;
    cssSeen.add(rel);
    const abs = path.join(homepageRoot, rel);
    cssParts.push(`/* ${rel} */\n` + loadFileOrFail(abs, 'CSS'));
  }
  const cssBundle = minifyCss(cssParts.join('\n'));

  const jsParts = [];
  const jsSeen = new Set();
  for (const hit of jsHits) {
    const rel = normalizeHomepageRef(hit.href);
    if (jsSeen.has(rel)) continue;
    jsSeen.add(rel);
    const abs = path.join(homepageRoot, rel);
    jsParts.push(`/* ${rel} */\n` + loadFileOrFail(abs, 'JS'));
  }
  const jsBundle = minifyJs(jsParts.join('\n'));

  let rewrittenHtml = rawHtml;
  let firstCssEndIndex = -1;
  for (const hit of cssHits) {
    const replacement = hit === cssHits[0] ? '<link rel="stylesheet" href="styles.css">' : '';
    rewrittenHtml = rewrittenHtml.replace(hit.match, replacement);
    if (firstCssEndIndex === -1 && hit === cssHits[0]) {
      firstCssEndIndex = rewrittenHtml.indexOf('<link rel="stylesheet" href="styles.css">');
    }
  }

  for (const hit of jsHits) {
    const replacement = hit === jsHits[0] ? '<script src="main.js" defer></script>' : '';
    rewrittenHtml = rewrittenHtml.replace(hit.match, replacement);
  }

  rewrittenHtml = rewrittenHtml
    .replace(/\s+href=["']\.\.\/src\/assets\/images\/logo\.svg["']/g, ' href="assets/images/logo.svg"')
    .replace(/\s+src=["']\.\.\/src\/assets\/images\/logo\.svg["']/g, ' src="assets/images/logo.svg"')
    .replace(/\s+href=["']\/favicon\.ico["']/g, ' href="favicon.ico"');

  const iconRefRe = /(["'(])\.\.\/src\/assets\/icons\/([a-zA-Z0-9_-]+\.svg)/g;
  rewrittenHtml = rewrittenHtml.replace(iconRefRe, '$1assets/icons/$2');

  rewrittenHtml = rewrittenHtml.replace(/[ \t]+\n/g, '\n').replace(/\n{3,}/g, '\n\n');

  rmrf(outDir);
  ensureDir(outDir);
  const assetsDir = path.join(outDir, 'assets');
  const assetsImagesDir = path.join(assetsDir, 'images');
  const assetsIconsDir = path.join(assetsDir, 'icons');
  ensureDir(assetsImagesDir);
  ensureDir(assetsIconsDir);

  fs.writeFileSync(path.join(outDir, 'index.html'), rewrittenHtml);
  fs.writeFileSync(path.join(outDir, 'styles.css'), cssBundle);
  fs.writeFileSync(path.join(outDir, 'main.js'), jsBundle);

  const logoSrc = path.join(homepageRoot, 'src', 'assets', 'images', 'logo.svg');
  if (fs.existsSync(logoSrc)) {
    fs.copyFileSync(logoSrc, path.join(assetsImagesDir, 'logo.svg'));
  }
  const iconsDir = path.join(homepageRoot, 'src', 'assets', 'icons');
  if (fs.existsSync(iconsDir)) {
    for (const entry of fs.readdirSync(iconsDir).sort()) {
      if (!entry.endsWith('.svg')) continue;
      fs.copyFileSync(path.join(iconsDir, entry), path.join(assetsIconsDir, entry));
    }
  }
  const faviconSrc = path.join(homepageRoot, 'public', 'favicon.ico');
  if (fs.existsSync(faviconSrc)) {
    fs.copyFileSync(faviconSrc, path.join(outDir, 'favicon.ico'));
  }
  const robotsSrc = path.join(homepageRoot, 'public', 'robots.txt');
  if (fs.existsSync(robotsSrc)) {
    fs.copyFileSync(robotsSrc, path.join(outDir, 'robots.txt'));
  }

  const stat = (p) => (fs.existsSync(p) ? fs.statSync(p).size : 0);
  const summary = {
    outDir,
    bytes: {
      'index.html': stat(path.join(outDir, 'index.html')),
      'styles.css': stat(path.join(outDir, 'styles.css')),
      'main.js': stat(path.join(outDir, 'main.js')),
      'assets/images/logo.svg': stat(path.join(assetsImagesDir, 'logo.svg')),
    },
    cssSources: Array.from(cssSeen),
    jsSources: Array.from(jsSeen),
  };

  if (!options.quiet) {
    process.stdout.write(
      `MirDB homepage build: artifacts produced in ${outDir}\n` +
        `  index.html  ${summary.bytes['index.html']} bytes\n` +
        `  styles.css  ${summary.bytes['styles.css']} bytes (${summary.cssSources.length} sources)\n` +
        `  main.js     ${summary.bytes['main.js']} bytes (${summary.jsSources.length} sources)\n` +
        `  assets/images/logo.svg ${summary.bytes['assets/images/logo.svg']} bytes\n`
    );
  }

  return summary;
}

function main() {
  const opts = parseArgs(process.argv.slice(2));
  try {
    build(opts);
    process.exit(0);
  } catch (err) {
    process.stderr.write(`MirDB homepage build failed: ${err.message}\n`);
    if (err.stack) process.stderr.write(err.stack + '\n');
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

module.exports = { build, minifyCss, minifyJs };
