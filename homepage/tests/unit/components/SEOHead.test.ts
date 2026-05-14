import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import { execSync } from 'node:child_process';
import { parseHTML } from 'linkedom';

const HOMEPAGE_DIR = join(__dirname, '..', '..', '..');
const DIST_DIR = join(HOMEPAGE_DIR, 'dist');
const INDEX_HTML = join(DIST_DIR, 'index.html');

function buildSite() {
  // If built output already exists, skip the build to avoid race conditions
  // with other test files that also trigger Astro builds
  if (existsSync(INDEX_HTML)) {
    return;
  }
  try {
    const astroBin = join(HOMEPAGE_DIR, 'node_modules', 'astro', 'astro.js');
    execSync(`node "${astroBin}" build`, {
      cwd: HOMEPAGE_DIR,
      stdio: 'pipe',
      env: { ...process.env, NODE_ENV: 'production' },
    });
  } catch (e: any) {
    const stderr = e.stderr?.toString() || '';
    throw new Error(`Build failed: ${stderr}`);
  }
}

function parseBuiltHtml() {
  const html = readFileSync(INDEX_HTML, 'utf-8');
  return parseHTML(html);
}

describe('SEO & Metadata', () => {
  beforeAll(() => {
    buildSite();
  });

  describe('Test Case 1: Title tag', () => {
    it('has a title tag that includes MirDB', () => {
      const { document } = parseBuiltHtml();
      const titleEl = document.querySelector('title');
      expect(titleEl).not.toBeNull();
      expect(titleEl!.textContent).toContain('MirDB');
    });

    it('title is between 30-70 characters', () => {
      const { document } = parseBuiltHtml();
      const titleEl = document.querySelector('title');
      const title = titleEl!.textContent!.trim();
      expect(title.length).toBeGreaterThanOrEqual(30);
      expect(title.length).toBeLessThanOrEqual(70);
    });

    it('title includes a descriptor', () => {
      const { document } = parseBuiltHtml();
      const titleEl = document.querySelector('title');
      expect(titleEl!.textContent).toMatch(/MirDB.+Store/);
    });
  });

  describe('Test Case 2: Meta description tag', () => {
    it('has a meta description tag', () => {
      const { document } = parseBuiltHtml();
      const metaDesc = document.querySelector('meta[name="description"]');
      expect(metaDesc).not.toBeNull();
    });

    it('meta description has non-empty content between 50-160 characters', () => {
      const { document } = parseBuiltHtml();
      const metaDesc = document.querySelector('meta[name="description"]');
      const content = metaDesc!.getAttribute('content') || '';
      expect(content.length).toBeGreaterThanOrEqual(50);
      expect(content.length).toBeLessThanOrEqual(160);
    });

    it('meta description summarizes value proposition', () => {
      const { document } = parseBuiltHtml();
      const metaDesc = document.querySelector('meta[name="description"]');
      const content = metaDesc!.getAttribute('content') || '';
      expect(content).toMatch(/key-value|store|persistent|database/i);
    });
  });

  describe('Test Case 3: Viewport meta tag', () => {
    it('has a viewport meta tag', () => {
      const { document } = parseBuiltHtml();
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
    });

    it('viewport includes width=device-width and initial-scale=1', () => {
      const { document } = parseBuiltHtml();
      const viewport = document.querySelector('meta[name="viewport"]');
      const content = viewport!.getAttribute('content') || '';
      expect(content).toContain('width=device-width');
      expect(content).toContain('initial-scale=1');
    });
  });

  describe('Test Case 4: Charset declaration', () => {
    it('has a charset meta tag', () => {
      const { document } = parseBuiltHtml();
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
    });

    it('charset is UTF-8', () => {
      const { document } = parseBuiltHtml();
      const charset = document.querySelector('meta[charset]');
      expect(charset!.getAttribute('charset')?.toUpperCase()).toBe('UTF-8');
    });

    it('charset is the first element in head', () => {
      const { document } = parseBuiltHtml();
      const head = document.querySelector('head');
      expect(head).not.toBeNull();
      const firstChild = head!.firstElementChild;
      expect(firstChild).not.toBeNull();
      expect(firstChild!.tagName.toLowerCase()).toBe('meta');
      expect(firstChild!.hasAttribute('charset')).toBe(true);
    });
  });

  describe('Test Case 5: Open Graph meta tags', () => {
    it('has og:title meta tag', () => {
      const { document } = parseBuiltHtml();
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
      expect(ogTitle!.getAttribute('content')!.length).toBeGreaterThan(0);
    });

    it('has og:description meta tag', () => {
      const { document } = parseBuiltHtml();
      const ogDesc = document.querySelector('meta[property="og:description"]');
      expect(ogDesc).not.toBeNull();
      expect(ogDesc!.getAttribute('content')!.length).toBeGreaterThan(0);
    });

    it('has og:image meta tag', () => {
      const { document } = parseBuiltHtml();
      const ogImage = document.querySelector('meta[property="og:image"]');
      expect(ogImage).not.toBeNull();
      expect(ogImage!.getAttribute('content')!.length).toBeGreaterThan(0);
    });

    it('has og:url meta tag', () => {
      const { document } = parseBuiltHtml();
      const ogUrl = document.querySelector('meta[property="og:url"]');
      expect(ogUrl).not.toBeNull();
      expect(ogUrl!.getAttribute('content')!.length).toBeGreaterThan(0);
    });

    it('has og:type meta tag', () => {
      const { document } = parseBuiltHtml();
      const ogType = document.querySelector('meta[property="og:type"]');
      expect(ogType).not.toBeNull();
      expect(ogType!.getAttribute('content')!).toBe('website');
    });
  });

  describe('Test Case 6: OG image valid path', () => {
    it('og:image points to a valid image URL path', () => {
      const { document } = parseBuiltHtml();
      const ogImage = document.querySelector('meta[property="og:image"]');
      const content = ogImage!.getAttribute('content')!;
      expect(content).toMatch(/\/assets\/logo\.gif/);
    });
  });

  describe('Test Case 7: Semantic HTML elements', () => {
    it('has a header element or role="banner"', () => {
      const { document } = parseBuiltHtml();
      const header = document.querySelector('header');
      const banner = document.querySelector('[role="banner"]');
      expect(header || banner).not.toBeNull();
    });

    it('has a nav element', () => {
      const { document } = parseBuiltHtml();
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();
    });

    it('has a main element', () => {
      const { document } = parseBuiltHtml();
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
    });

    it('has a footer element', () => {
      const { document } = parseBuiltHtml();
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    it('headings (h1-h3) follow logical descending order without skipping levels', () => {
      const { document } = parseBuiltHtml();
      const headings = Array.from(
        document.querySelectorAll('h1, h2, h3, h4, h5, h6'),
      );
      expect(headings.length).toBeGreaterThan(0);

      let lastLevel = 0;
      for (const heading of headings) {
        const level = parseInt(heading.tagName.charAt(1), 10);
        if (lastLevel > 0) {
          expect(level).toBeLessThanOrEqual(lastLevel + 1);
        }
        lastLevel = Math.max(lastLevel, level);
      }
      const hasH1 = headings.some((h) => h.tagName === 'H1');
      expect(hasH1).toBe(true);
    });
  });

  describe('Test Case 8: robots.txt file', () => {
    it('robots.txt exists with 200-equivalent file presence', () => {
      const robotsPath = join(HOMEPAGE_DIR, 'public', 'robots.txt');
      expect(existsSync(robotsPath)).toBe(true);
    });

    it('robots.txt does not block all user-agents', () => {
      const robotsPath = join(HOMEPAGE_DIR, 'public', 'robots.txt');
      const content = readFileSync(robotsPath, 'utf-8');
      expect(content).not.toMatch(/Disallow:\s*\/\s*$/m);
    });

    it('robots.txt allows crawling by all user-agents', () => {
      const robotsPath = join(HOMEPAGE_DIR, 'public', 'robots.txt');
      const content = readFileSync(robotsPath, 'utf-8');
      expect(content).toContain('User-agent: *');
      expect(content).toMatch(/Allow:\s*\//);
    });
  });
});
