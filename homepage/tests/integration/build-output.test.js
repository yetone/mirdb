/**
 * Integration tests for the static-site build pipeline (Scenario 10, Tests 1-5).
 *
 * Covers:
 *   - Test case 1: build script exits 0 and prints a summary
 *   - Test case 2: required artifacts (index.html, styles.css, main.js,
 *     assets/images/logo.svg) all exist with non-zero size
 *   - Test case 3: dist/index.html contains every required section anchor and
 *     a <footer> element
 *   - Test case 4: total gzip size of html + css + js + assets is within the
 *     budget declared in lighthouse-budget.json
 *   - Test case 5: repeated builds against identical inputs produce
 *     byte-identical artifacts (deterministic build)
 *
 * The test invokes the build script both as a Node child process (mirroring
 * the literal CLI command from the scenario definition) and as an in-process
 * function call (to keep the test fast and easy to debug).
 */

'use strict';

const fs = require('fs');
const path = require('path');
const os = require('os');
const zlib = require('zlib');
const { execFileSync } = require('child_process');
const crypto = require('crypto');

const HOMEPAGE_ROOT = path.resolve(__dirname, '..', '..');
const BUILD_SCRIPT = path.join(HOMEPAGE_ROOT, 'build', 'build.js');
const BUDGET_PATH = path.join(HOMEPAGE_ROOT, 'build', 'lighthouse-budget.json');
const REQUIRED_ANCHORS = ['hero', 'features', 'quick-start', 'status', 'roadmap', 'why-mirdb'];

function mkTempDir(label) {
  return fs.mkdtempSync(path.join(os.tmpdir(), `mirdb-${label}-`));
}

function listFilesRecursive(dir) {
  const out = [];
  const stack = [dir];
  while (stack.length > 0) {
    const cur = stack.pop();
    for (const entry of fs.readdirSync(cur).sort()) {
      const abs = path.join(cur, entry);
      const stat = fs.lstatSync(abs);
      if (stat.isDirectory()) stack.push(abs);
      else out.push(abs);
    }
  }
  return out.sort();
}

function hashDir(dir) {
  const files = listFilesRecursive(dir);
  const hasher = crypto.createHash('sha256');
  for (const f of files) {
    const rel = path.relative(dir, f);
    hasher.update(rel + '\0');
    hasher.update(fs.readFileSync(f));
    hasher.update('\0');
  }
  return hasher.digest('hex');
}

function gzipSize(buf) {
  return zlib.gzipSync(buf, { level: 9 }).length;
}

describe('Build pipeline integration (Scenario 10)', () => {
  let outDir;

  beforeAll(() => {
    outDir = mkTempDir('build-out');
    const stdout = execFileSync(
      process.execPath,
      [BUILD_SCRIPT, '--out', outDir],
      { encoding: 'utf8' }
    );
    expect(stdout).toMatch(/artifacts produced/i);
    expect(stdout).toMatch(/index\.html/);
    expect(stdout).toMatch(/styles\.css/);
    expect(stdout).toMatch(/main\.js/);
  });

  afterAll(() => {
    if (outDir && fs.existsSync(outDir)) {
      fs.rmSync(outDir, { recursive: true, force: true });
    }
  });

  test('test case 1: build script exits 0 and prints a summary', () => {
    expect(fs.existsSync(outDir)).toBe(true);
    expect(fs.existsSync(path.join(outDir, 'index.html'))).toBe(true);
  });

  test('test case 2: required artifacts exist and are non-empty', () => {
    const required = [
      path.join(outDir, 'index.html'),
      path.join(outDir, 'styles.css'),
      path.join(outDir, 'main.js'),
      path.join(outDir, 'assets', 'images', 'logo.svg'),
    ];
    for (const file of required) {
      expect(fs.existsSync(file)).toBe(true);
      const stat = fs.statSync(file);
      expect(stat.isFile()).toBe(true);
      expect(stat.size).toBeGreaterThan(0);
    }
  });

  test('test case 3: index.html contains every required section anchor and a <footer>', () => {
    const html = fs.readFileSync(path.join(outDir, 'index.html'), 'utf8');
    for (const id of REQUIRED_ANCHORS) {
      expect(html).toMatch(new RegExp(`id=["']${id}["']`));
    }
    expect(html).toMatch(/<footer\b/i);
  });

  test('test case 4: combined gzip size of html + css + js + assets is within budget', () => {
    const budget = JSON.parse(fs.readFileSync(BUDGET_PATH, 'utf8'));
    const total = budget.resourceSizes.totalGzipBytes;
    expect(typeof total).toBe('number');
    expect(total).toBeGreaterThan(0);

    let bytes = 0;
    for (const file of listFilesRecursive(outDir)) {
      bytes += gzipSize(fs.readFileSync(file));
    }
    expect(bytes).toBeLessThan(total);
  });

  test('test case 5: build is deterministic across repeated runs', () => {
    const a = mkTempDir('build-deterministic-a');
    const b = mkTempDir('build-deterministic-b');
    try {
      execFileSync(process.execPath, [BUILD_SCRIPT, '--out', a, '--quiet']);
      execFileSync(process.execPath, [BUILD_SCRIPT, '--out', b, '--quiet']);
      const ha = hashDir(a);
      const hb = hashDir(b);
      expect(ha).toBe(hb);

      const filesA = listFilesRecursive(a);
      const filesB = listFilesRecursive(b);
      expect(filesA.map((p) => path.relative(a, p))).toEqual(
        filesB.map((p) => path.relative(b, p))
      );

      for (const fA of filesA) {
        const rel = path.relative(a, fA);
        const fB = path.join(b, rel);
        expect(fs.readFileSync(fA)).toEqual(fs.readFileSync(fB));
      }
    } finally {
      fs.rmSync(a, { recursive: true, force: true });
      fs.rmSync(b, { recursive: true, force: true });
    }
  });

  test('build assembles a self-contained html that only references local bundle + assets', () => {
    const html = fs.readFileSync(path.join(outDir, 'index.html'), 'utf8');
    expect(html).toMatch(/href=["']styles\.css["']/);
    expect(html).toMatch(/src=["']main\.js["']/);
    expect(html).not.toMatch(/\.\.\/src\//);
  });

  test('lighthouse-budget.json declares all required category thresholds', () => {
    const budget = JSON.parse(fs.readFileSync(BUDGET_PATH, 'utf8'));
    for (const key of ['performance', 'accessibility', 'best-practices', 'seo']) {
      expect(budget.categories[key]).toBeDefined();
      expect(budget.categories[key].minScore).toBeGreaterThanOrEqual(90);
    }
    expect(budget.metrics['first-contentful-paint'].maxMs).toBeLessThanOrEqual(2000);
    expect(budget.metrics['total-blocking-time'].maxMs).toBeLessThanOrEqual(200);
  });
});
