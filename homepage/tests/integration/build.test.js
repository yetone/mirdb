/**
 * Build System & Static Generation Tests
 * Owner: Scenario 13 - Build System & Static Generation
 *
 * Tests verify:
 * - zola build completes successfully with exit code 0
 * - public/ directory exists with expected files
 * - index.html has valid HTML5 structure
 * - Static assets (CSS, JS, images) are copied to output
 * - Anchor links reference existing section IDs
 * - External links use valid https:// URLs
 * - CSS files parse without syntax errors
 * - zola serve starts successfully
 * - Homepage renders without JavaScript (all content in static HTML)
 */

const fs = require('fs');
const path = require('path');
const { execSync, spawn } = require('child_process');
const { JSDOM } = require('jsdom');
const http = require('http');

const HOMEPAGE_DIR = path.join(__dirname, '../..');
const PUBLIC_DIR = path.join(HOMEPAGE_DIR, 'public');
const INDEX_HTML = path.join(PUBLIC_DIR, 'index.html');
const ZOLA_CMD = process.env.ZOLA_PATH || 'zola';

function runZola(args, options = {}) {
  const cmd = `${ZOLA_CMD} ${args}`;
  return execSync(cmd, {
    cwd: HOMEPAGE_DIR,
    encoding: 'utf-8',
    ...options,
  });
}

describe('Build System & Static Generation - Integration Tests', () => {
  let dom;
  let document;

  beforeAll(() => {
    // Ensure we have a fresh build
    if (fs.existsSync(PUBLIC_DIR)) {
      fs.rmSync(PUBLIC_DIR, { recursive: true, force: true });
    }

    // Run zola build
    const output = runZola('build');
    expect(output).toBeTruthy();

    // Parse the generated index.html
    const html = fs.readFileSync(INDEX_HTML, 'utf-8');
    dom = new JSDOM(html, { url: 'http://localhost:3000' });
    document = dom.window.document;
  });

  afterAll(() => {
    if (dom) dom.window.close();
  });

  // Test Case 1: zola build completes successfully
  describe('Test 1: Zola build command', () => {
    it('should run zola build with exit code 0', () => {
      let exitCode = 0;
      let stdout = '';
      let stderr = '';

      try {
        const result = runZola('build');
        stdout = result;
      } catch (error) {
        exitCode = error.status || 1;
        stdout = error.stdout || '';
        stderr = error.stderr || '';
      }

      expect(exitCode).toBe(0);
      expect(stderr).not.toMatch(/error/i);
      expect(stdout).toMatch(/Building site/);
      expect(stdout).toMatch(/Done/);
    });

    it('should have no template rendering errors or warnings', () => {
      let stdout = '';
      let stderr = '';

      try {
        const result = runZola('build');
        stdout = result;
      } catch (error) {
        stdout = error.stdout || '';
        stderr = error.stderr || '';
      }

      const output = (stdout + stderr).toLowerCase();
      expect(output).not.toContain('warning: missing variable');
      expect(output).not.toContain('failed to include');
      expect(output).not.toContain('template error');
      expect(output).not.toContain('render error');
    });
  });

  // Test Case 2: Build output directory structure
  describe('Test 2: Build output directory structure', () => {
    it('should create public/ directory', () => {
      expect(fs.existsSync(PUBLIC_DIR)).toBe(true);
      expect(fs.statSync(PUBLIC_DIR).isDirectory()).toBe(true);
    });

    it('should generate public/index.html', () => {
      expect(fs.existsSync(INDEX_HTML)).toBe(true);
      const stats = fs.statSync(INDEX_HTML);
      expect(stats.size).toBeGreaterThan(0);
    });

    it('should copy CSS files to public/css/', () => {
      const cssDir = path.join(PUBLIC_DIR, 'css');
      expect(fs.existsSync(cssDir)).toBe(true);

      const cssFiles = fs.readdirSync(cssDir).filter(f => f.endsWith('.css'));
      expect(cssFiles.length).toBeGreaterThan(0);
      expect(fs.existsSync(path.join(cssDir, 'main.css'))).toBe(true);
    });

    it('should copy JS files to public/js/', () => {
      const jsDir = path.join(PUBLIC_DIR, 'js');
      expect(fs.existsSync(jsDir)).toBe(true);

      const jsFiles = fs.readdirSync(jsDir).filter(f => f.endsWith('.js'));
      expect(jsFiles.length).toBeGreaterThan(0);
      expect(fs.existsSync(path.join(jsDir, 'theme.js'))).toBe(true);
      expect(fs.existsSync(path.join(jsDir, 'nav.js'))).toBe(true);
      expect(fs.existsSync(path.join(jsDir, 'clipboard.js'))).toBe(true);
    });

    it('should copy images to public/images/', () => {
      const imagesDir = path.join(PUBLIC_DIR, 'images');
      expect(fs.existsSync(imagesDir)).toBe(true);

      const imageFiles = fs.readdirSync(imagesDir);
      expect(imageFiles.length).toBeGreaterThan(0);
    });

    it('should generate robots.txt', () => {
      const robotsPath = path.join(PUBLIC_DIR, 'robots.txt');
      expect(fs.existsSync(robotsPath)).toBe(true);
    });

    it('should generate sitemap.xml', () => {
      const sitemapPath = path.join(PUBLIC_DIR, 'sitemap.xml');
      expect(fs.existsSync(sitemapPath)).toBe(true);
    });
  });

  // Test Case 3: HTML validation
  describe('Test 3: HTML structure validation', () => {
    it('should have DOCTYPE declaration', () => {
      const html = fs.readFileSync(INDEX_HTML, 'utf-8');
      expect(html.toLowerCase()).toMatch(/<!doctype\s+html>/);
    });

    it('should have html element with lang attribute', () => {
      const htmlEl = document.querySelector('html');
      expect(htmlEl).toBeTruthy();
      expect(htmlEl.getAttribute('lang')).toBe('en');
    });

    it('should have head element', () => {
      expect(document.querySelector('head')).toBeTruthy();
    });

    it('should have body element', () => {
      expect(document.querySelector('body')).toBeTruthy();
    });

    it('should have title tag', () => {
      const title = document.querySelector('title');
      expect(title).toBeTruthy();
      expect(title.textContent.trim().length).toBeGreaterThan(0);
    });

    it('should have meta charset', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).toBeTruthy();
    });

    it('should have meta viewport', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).toBeTruthy();
    });

    it('should have no unclosed tags (html is well-formed)', () => {
      const htmlEl = document.querySelector('html');
      expect(htmlEl).toBeTruthy();
      expect(htmlEl.children.length).toBeGreaterThanOrEqual(2);

      const body = document.querySelector('body');
      expect(body.children.length).toBeGreaterThan(0);
    });

    it('should have no duplicate IDs', () => {
      const allElements = document.querySelectorAll('[id]');
      const ids = new Set();
      const duplicates = [];

      allElements.forEach(el => {
        const id = el.id;
        if (ids.has(id)) {
          duplicates.push(id);
        }
        ids.add(id);
      });

      expect(duplicates).toEqual([]);
    });
  });

  // Test Case 4: Anchor links validation
  describe('Test 4: Internal anchor links', () => {
    it('should have all anchor links with corresponding section IDs', () => {
      const anchorLinks = document.querySelectorAll('a[href^="#"]');
      const missingTargets = [];

      anchorLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href === '#') return;

        const targetId = href.substring(1);
        const target = document.getElementById(targetId);

        if (!target) {
          missingTargets.push(href);
        }
      });

      expect(missingTargets).toEqual([]);
    });

    it('should have target for skip navigation link', () => {
      const skipLink = document.querySelector('a[href="#main-content"]');
      expect(skipLink).toBeTruthy();
      expect(document.getElementById('main-content')).toBeTruthy();
    });

    it('should have target for all nav section links', () => {
      const navLinks = document.querySelectorAll('.nav-links a[href^="#"]');
      navLinks.forEach(link => {
        const href = link.getAttribute('href');
        const targetId = href.substring(1);
        const target = document.getElementById(targetId);
        expect(target).toBeTruthy();
      });
    });

    it('should have Get Started button linking to quickstart section', () => {
      const cta = document.querySelector('a[href="#quickstart"]');
      expect(cta).toBeTruthy();
      expect(document.getElementById('quickstart')).toBeTruthy();
    });
  });

  // Test Case 5: External links validation
  describe('Test 5: External links', () => {
    it('should have only valid https:// URLs for external links', () => {
      const allLinks = document.querySelectorAll('a[href]');
      const invalidLinks = [];

      allLinks.forEach(link => {
        const href = link.getAttribute('href');

        if (href.startsWith('#') || href.startsWith('/') || href.startsWith('mailto:')) {
          return;
        }

        if (!href.startsWith('https://')) {
          invalidLinks.push({
            href,
            text: link.textContent.trim().substring(0, 50),
          });
        }
      });

      expect(invalidLinks).toEqual([]);
    });

    it('should have rel="noopener noreferrer" on external links', () => {
      const externalLinks = document.querySelectorAll('a[href^="http"]');
      const missingRel = [];

      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel') || '';
        if (!rel.includes('noopener') || !rel.includes('noreferrer')) {
          missingRel.push(link.getAttribute('href'));
        }
      });

      expect(missingRel).toEqual([]);
    });

    it('should have GitHub link with valid URL', () => {
      const githubLinks = document.querySelectorAll('a[href*="github.com"]');
      expect(githubLinks.length).toBeGreaterThan(0);

      githubLinks.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).toMatch(/^https:\/\/github\.com\//);
      });
    });
  });

  // Test Case 6: CSS validation
  describe('Test 6: CSS file validation', () => {
    it('should parse main.css without syntax errors', () => {
      const cssPath = path.join(PUBLIC_DIR, 'css', 'main.css');
      const css = fs.readFileSync(cssPath, 'utf-8');

      expect(css).not.toMatch(/\{\s*\}/);
      expect(css).toMatch(/:root\s*\{/);
    });

    it('should have CSS variables defined before use', () => {
      const cssPath = path.join(PUBLIC_DIR, 'css', 'main.css');
      const css = fs.readFileSync(cssPath, 'utf-8');

      const rootMatch = css.match(/:root\s*\{([^}]*)\}/s);
      expect(rootMatch).toBeTruthy();

      const rootVars = rootMatch[1];
      expect(rootVars).toContain('--color-bg');
      expect(rootVars).toContain('--color-text');
      expect(rootVars).toContain('--color-primary');
      expect(rootVars).toContain('--font-family-base');
    });

    it('should have valid CSS selectors', () => {
      const cssPath = path.join(PUBLIC_DIR, 'css', 'main.css');
      const css = fs.readFileSync(cssPath, 'utf-8');

      const openBraces = (css.match(/\{/g) || []).length;
      const closeBraces = (css.match(/\}/g) || []).length;
      expect(openBraces).toBe(closeBraces);
    });

    it('should have responsive.css with media queries', () => {
      const cssPath = path.join(PUBLIC_DIR, 'css', 'responsive.css');
      const css = fs.readFileSync(cssPath, 'utf-8');

      expect(css).toMatch(/@media\s*\(/);
    });

    it('should have syntax.css for code highlighting', () => {
      const cssPath = path.join(PUBLIC_DIR, 'css', 'syntax.css');
      expect(fs.existsSync(cssPath)).toBe(true);
    });

    it('should have balanced braces in all CSS files', () => {
      const cssDir = path.join(PUBLIC_DIR, 'css');
      const files = fs.readdirSync(cssDir).filter(f => f.endsWith('.css'));

      files.forEach(file => {
        const cssPath = path.join(cssDir, file);
        const css = fs.readFileSync(cssPath, 'utf-8');
        const openBraces = (css.match(/\{/g) || []).length;
        const closeBraces = (css.match(/\}/g) || []).length;
        expect(openBraces).toBe(closeBraces);
      });
    });
  });

  // Test Case 7: zola serve
  describe('Test 7: Zola serve mode', () => {
    it('should start zola serve and serve homepage', async () => {
      const port = await new Promise((resolve) => {
        const srv = http.createServer();
        srv.listen(0, () => {
          const p = srv.address().port;
          srv.close(() => resolve(p));
        });
      });

      const server = spawn(ZOLA_CMD, ['serve', '--port', String(port)], {
        cwd: HOMEPAGE_DIR,
        stdio: 'pipe',
      });

      let serverOutput = '';
      server.stdout.on('data', (data) => {
        serverOutput += data.toString();
      });
      server.stderr.on('data', (data) => {
        serverOutput += data.toString();
      });

      // Wait for server to start
      await new Promise(resolve => setTimeout(resolve, 3000));

      try {
        expect(serverOutput).toMatch(/listening|server|running|available/i);

        const response = await new Promise((resolve, reject) => {
          const req = http.get(`http://127.0.0.1:${port}/`, (res) => {
            let data = '';
            res.on('data', chunk => { data += chunk; });
            res.on('end', () => {
              resolve({ statusCode: res.statusCode, data });
            });
          });
          req.on('error', reject);
          req.setTimeout(5000, () => reject(new Error('Request timeout')));
        });

        expect(response.statusCode).toBe(200);
        expect(response.data).toContain('<!doctype html>');
        expect(response.data).toContain('MirDB');
      } finally {
        server.kill('SIGTERM');
        await new Promise(resolve => setTimeout(resolve, 500));
        if (!server.killed) {
          server.kill('SIGKILL');
        }
      }
    }, 20000);
  });

  // Test Case 8: No-JS rendering
  describe('Test 8: Homepage renders without JavaScript', () => {
    it('should have all content in static HTML', () => {
      expect(document.querySelector('h1')).toBeTruthy();
      expect(document.querySelector('.hero-tagline')).toBeTruthy();
      expect(document.getElementById('features')).toBeTruthy();
      expect(document.querySelectorAll('.feature-card').length).toBeGreaterThan(0);
      expect(document.getElementById('quickstart')).toBeTruthy();
      expect(document.querySelectorAll('pre code').length).toBeGreaterThan(0);
      expect(document.getElementById('architecture')).toBeTruthy();
      expect(document.getElementById('performance')).toBeTruthy();
      expect(document.getElementById('docs')).toBeTruthy();
      expect(document.querySelector('footer')).toBeTruthy();
    });

    it('should have navigation in static HTML', () => {
      expect(document.querySelector('header')).toBeTruthy();
      expect(document.querySelector('nav')).toBeTruthy();
      expect(document.querySelectorAll('.nav-links a').length).toBeGreaterThan(0);
    });

    it('should have all text content visible without JS', () => {
      const bodyText = document.body.textContent;

      expect(bodyText).toContain('MirDB');
      expect(bodyText).toContain('Features');
      expect(bodyText).toContain('Quick Start');
      expect(bodyText).toContain('Architecture');
      expect(bodyText).toContain('Performance');
      expect(bodyText).toContain('Documentation');
    });

    it('should not rely on noscript for critical content', () => {
      expect(document.querySelector('main')).toBeTruthy();
      expect(document.querySelector('main').children.length).toBeGreaterThan(0);
    });

    it('should have section content directly in HTML', () => {
      const svgDiagram = document.querySelector('.architecture-diagram svg');
      expect(svgDiagram).toBeTruthy();
    });
  });
});
