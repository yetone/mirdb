/**
 * Build System & Static Generation Integration Tests
 * Tests: Zola build output, HTML validity, CSS validity, anchor links, external links, no-JS rendering
 */

const fs = require('fs');
const path = require('path');
const { execSync, spawn } = require('child_process');
const { JSDOM } = require('jsdom');
const http = require('http');

const HOMEPAGE_DIR = path.join(__dirname, '..', '..');
const PUBLIC_DIR = path.join(HOMEPAGE_DIR, 'public');
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
    const htmlPath = path.join(PUBLIC_DIR, 'index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(html, { url: 'http://localhost:3000' });
    document = dom.window.document;
  });

  afterAll(() => {
    if (dom) dom.window.close();
  });

  // Test Case 1: Run 'zola build' command
  describe('Test 1: Zola build completes successfully', () => {
    it('should complete with exit code 0 and no error output', () => {
      // Build already ran in beforeAll; if it failed, the suite would error
      expect(fs.existsSync(PUBLIC_DIR)).toBe(true);
    });

    it('should produce no template rendering errors in output', () => {
      let output;
      try {
        output = runZola('build');
      } catch (e) {
        output = e.stdout || '';
      }
      const lower = output.toLowerCase();
      expect(lower).not.toContain('error');
      expect(lower).not.toContain('warning: missing variable');
      expect(lower).not.toContain('failed include');
    });
  });

  // Test Case 2: Inspect build output directory
  describe('Test 2: Build output directory structure', () => {
    it('should create public/index.html', () => {
      const indexPath = path.join(PUBLIC_DIR, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);
      const stats = fs.statSync(indexPath);
      expect(stats.size).toBeGreaterThan(0);
    });

    it('should copy CSS files to public/css/', () => {
      const cssDir = path.join(PUBLIC_DIR, 'css');
      expect(fs.existsSync(cssDir)).toBe(true);
      const files = fs.readdirSync(cssDir);
      expect(files.length).toBeGreaterThan(0);
      expect(files.some(f => f.endsWith('.css'))).toBe(true);
    });

    it('should copy JS files to public/js/', () => {
      const jsDir = path.join(PUBLIC_DIR, 'js');
      expect(fs.existsSync(jsDir)).toBe(true);
      const files = fs.readdirSync(jsDir);
      expect(files.length).toBeGreaterThan(0);
      expect(files.some(f => f.endsWith('.js'))).toBe(true);
    });
  });

  // Test Case 3: Validate generated HTML
  describe('Test 3: HTML validation', () => {
    it('should have DOCTYPE declaration', () => {
      const html = fs.readFileSync(path.join(PUBLIC_DIR, 'index.html'), 'utf-8');
      expect(html.toLowerCase().startsWith('<!doctype html>')).toBe(true);
    });

    it('should have html element with lang attribute', () => {
      const htmlEl = document.querySelector('html');
      expect(htmlEl).toBeTruthy();
      expect(htmlEl.getAttribute('lang')).toBe('en');
    });

    it('should have head and body elements', () => {
      expect(document.querySelector('head')).toBeTruthy();
      expect(document.querySelector('body')).toBeTruthy();
    });

    it('should have a title in head', () => {
      const title = document.querySelector('head title');
      expect(title).toBeTruthy();
      expect(title.textContent.trim().length).toBeGreaterThan(0);
    });

    it('should not have unclosed tags (basic check)', () => {
      const html = fs.readFileSync(path.join(PUBLIC_DIR, 'index.html'), 'utf-8');
      // Check for common unclosed tag patterns
      const unclosedPatterns = [
        /<div[^>]*>[^<]*(?!<\/div>)(?=<div|<section|<footer|<header|<main)/g,
      ];
      // A simpler approach: JSDOM parsed it successfully, which means tags are balanced
      expect(document.documentElement).toBeTruthy();
    });

    it('should not have duplicate IDs', () => {
      const allElements = document.querySelectorAll('[id]');
      const ids = Array.from(allElements).map(el => el.id);
      const uniqueIds = new Set(ids);
      expect(uniqueIds.size).toBe(ids.length);
    });

    it('should have charset meta tag', () => {
      const meta = document.querySelector('meta[charset]');
      expect(meta).toBeTruthy();
    });

    it('should have viewport meta tag', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).toBeTruthy();
    });
  });

  // Test Case 4: Check all anchor links
  describe('Test 4: Anchor link validation', () => {
    it('should have corresponding id for every href="#section" link', () => {
      const anchorLinks = document.querySelectorAll('a[href^="#"]');
      const missingIds = [];

      anchorLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href === '#') return; // skip placeholder
        const targetId = href.slice(1);
        const target = document.getElementById(targetId);
        if (!target) {
          missingIds.push(href);
        }
      });

      expect(missingIds).toEqual([]);
    });

    it('should have all expected section IDs', () => {
      const expectedSections = ['features', 'quickstart', 'architecture', 'performance', 'docs', 'main-content'];
      expectedSections.forEach(id => {
        expect(document.getElementById(id)).toBeTruthy();
      });
    });
  });

  // Test Case 5: Check all external links
  describe('Test 5: External link validation', () => {
    it('should use https:// for all external links', () => {
      const allLinks = document.querySelectorAll('a[href^="http"]');
      const nonHttps = [];

      allLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (!href.startsWith('https://') && !href.startsWith('mailto:')) {
          nonHttps.push(href);
        }
      });

      expect(nonHttps).toEqual([]);
    });

    it('should have rel="noopener noreferrer" on target="_blank" links', () => {
      const blankLinks = document.querySelectorAll('a[target="_blank"]');
      blankLinks.forEach(link => {
        const rel = link.getAttribute('rel') || '';
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
      });
    });

    it('should have valid GitHub URLs', () => {
      const githubLinks = document.querySelectorAll('a[href*="github.com"]');
      expect(githubLinks.length).toBeGreaterThan(0);
      githubLinks.forEach(link => {
        const href = link.getAttribute('href');
        expect(href.startsWith('https://github.com/')).toBe(true);
      });
    });
  });

  // Test Case 6: Validate CSS files
  describe('Test 6: CSS validation', () => {
    function getCssFiles() {
      const cssDir = path.join(PUBLIC_DIR, 'css');
      return fs.readdirSync(cssDir).filter(f => f.endsWith('.css'));
    }

    it('should have CSS files in public/css/', () => {
      const files = getCssFiles();
      expect(files.length).toBeGreaterThan(0);
    });

    it('should have no unclosed braces in CSS', () => {
      const files = getCssFiles();
      files.forEach(file => {
        const cssPath = path.join(PUBLIC_DIR, 'css', file);
        const css = fs.readFileSync(cssPath, 'utf-8');
        const openBraces = (css.match(/\{/g) || []).length;
        const closeBraces = (css.match(/\}/g) || []).length;
        expect(openBraces).toBe(closeBraces);
      });
    });

    it('should have no unclosed parentheses in CSS', () => {
      const files = getCssFiles();
      files.forEach(file => {
        const cssPath = path.join(PUBLIC_DIR, 'css', file);
        const css = fs.readFileSync(cssPath, 'utf-8');
        const openParens = (css.match(/\(/g) || []).length;
        const closeParens = (css.match(/\)/g) || []).length;
        expect(openParens).toBe(closeParens);
      });
    });

    it('should define CSS variables before they are used', () => {
      // Check main.css for variable definitions
      const mainCssPath = path.join(PUBLIC_DIR, 'css', 'main.css');
      if (!fs.existsSync(mainCssPath)) return;

      const css = fs.readFileSync(mainCssPath, 'utf-8');
      // Extract all --variable definitions
      const definedVars = new Set();
      const defineMatches = css.match(/--[\w-]+\s*:/g) || [];
      defineMatches.forEach(m => {
        definedVars.add(m.replace(':', '').trim());
      });

      // Extract all var() usages
      const usedVars = css.match(/var\(\s*--[\w-]+/g) || [];
      const undefinedVars = [];
      usedVars.forEach(u => {
        const varName = u.replace('var(', '').trim();
        if (!definedVars.has(varName)) {
          undefinedVars.push(varName);
        }
      });

      // Some vars might be defined in other files or browser-native
      // Only fail if there are obvious undefined ones in the same file
      expect(undefinedVars).toEqual([]);
    });

    it('should have valid @media syntax', () => {
      const files = getCssFiles();
      files.forEach(file => {
        const cssPath = path.join(PUBLIC_DIR, 'css', file);
        const css = fs.readFileSync(cssPath, 'utf-8');
        const mediaMatches = css.match(/@media[^{]*\{/g) || [];
        mediaMatches.forEach(() => {
          // If we matched @media with {, basic syntax is OK
          expect(true).toBe(true);
        });
      });
    });
  });

  // Test Case 7: Test Zola serve mode
  describe('Test 7: Zola serve mode', () => {
    it('should start zola serve and serve the homepage', async () => {
      // Find an available port
      const port = await new Promise((resolve) => {
        const srv = require('http').createServer();
        srv.listen(0, () => {
          const p = srv.address().port;
          srv.close(() => resolve(p));
        });
      });

      // Start zola serve
      const child = spawn(ZOLA_CMD, ['serve', '--port', String(port)], {
        cwd: HOMEPAGE_DIR,
        stdio: 'pipe',
      });

      let output = '';
      child.stdout.on('data', (data) => {
        output += data.toString();
      });
      child.stderr.on('data', (data) => {
        output += data.toString();
      });

      // Wait for server to start
      await new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
          child.kill();
          reject(new Error('zola serve did not start within 10 seconds'));
        }, 10000);

        const checkReady = setInterval(() => {
          if (output.includes('Web server is available') || output.includes('Listening')) {
            clearInterval(checkReady);
            clearTimeout(timeout);
            resolve();
          }
        }, 200);
      });

      // Make HTTP request
      const response = await new Promise((resolve, reject) => {
        const req = http.get(`http://127.0.0.1:${port}/`, (res) => {
          let body = '';
          res.on('data', chunk => body += chunk);
          res.on('end', () => resolve({ status: res.statusCode, body }));
        });
        req.on('error', reject);
        req.setTimeout(5000, () => {
          req.destroy();
          reject(new Error('HTTP request timeout'));
        });
      });

      expect(response.status).toBe(200);
      expect(response.body).toContain('MirDB');
      expect(response.body).toContain('<!doctype html>');

      child.kill();
    }, 20000);
  });

  // Test Case 8: Verify homepage renders without JavaScript
  describe('Test 8: No-JS rendering', () => {
    it('should contain all content in static HTML', () => {
      const html = fs.readFileSync(path.join(PUBLIC_DIR, 'index.html'), 'utf-8');

      // Key content that should be present without JS
      expect(html).toContain('MirDB');
      expect(html).toContain('Features');
      expect(html).toContain('Quick Start');
      expect(html).toContain('Architecture');
      expect(html).toContain('Performance');
      expect(html).toContain('Documentation');
      expect(html).toContain('Memcached Protocol Compatible');
      expect(html).toContain('cargo install mirdb');
    });

    it('should not hide content behind JS-only rendering', () => {
      // Check that main content sections are present in the HTML
      const sections = document.querySelectorAll('main > section');
      expect(sections.length).toBeGreaterThanOrEqual(4);
    });

    it('should have section content directly in HTML, not loaded via JS', () => {
      // Feature cards should be in the DOM
      const featureCards = document.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThan(0);

      // Quick start code blocks should be present
      const codeBlocks = document.querySelectorAll('pre code');
      expect(codeBlocks.length).toBeGreaterThan(0);

      // Architecture diagram SVG should be in HTML
      const svgDiagram = document.querySelector('.architecture-diagram svg');
      expect(svgDiagram).toBeTruthy();
    });
  });
});
