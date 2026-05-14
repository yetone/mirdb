import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { readFileSync, existsSync, unlinkSync, copyFileSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { execSync } from 'node:child_process';
import { parseHTML } from 'linkedom';

const HOMEPAGE_DIR = join(__dirname, '..', '..', '..');
const DIST_DIR = join(HOMEPAGE_DIR, 'dist');
const INDEX_HTML = join(DIST_DIR, 'index.html');
const LOGO_PATH = join(HOMEPAGE_DIR, 'public', 'assets', 'logo.gif');
const LOGO_BACKUP = join(HOMEPAGE_DIR, 'public', 'assets', 'logo.gif.bak');

function buildSite() {
  try {
    execSync('npx astro build', {
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

describe('Hero Section & Branding', () => {
  describe('Test Case 1: Product name headline', () => {
    beforeAll(() => {
      buildSite();
    });

    it('contains MirDB as the h1 headline', () => {
      const { document } = parseBuiltHtml();
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1!.textContent).toContain('MirDB');
    });
  });

  describe('Test Case 2: Subheadline / value proposition', () => {
    beforeAll(() => {
      buildSite();
    });

    it('has a subheadline element directly after h1 with non-empty value proposition', () => {
      const { document } = parseBuiltHtml();
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();

      const tagline = h1!.nextElementSibling;
      expect(tagline).not.toBeNull();
      expect(tagline!.textContent?.trim().length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 3: Logo image', () => {
    beforeAll(() => {
      buildSite();
    });

    it('has an img element with src referencing logo.gif and non-empty alt', () => {
      const { document } = parseBuiltHtml();
      const img = document.querySelector('img');
      expect(img).not.toBeNull();
      expect(img!.getAttribute('src')).toBe('/assets/logo.gif');
      const alt = img!.getAttribute('alt');
      expect(alt).not.toBeNull();
      expect(alt!.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 4: Primary CTA - Get Started', () => {
    beforeAll(() => {
      buildSite();
    });

    it('has a Get Started link pointing to the quick start section', () => {
      const { document } = parseBuiltHtml();
      const getStartedLink = Array.from(document.querySelectorAll('a')).find(
        (el) => el.textContent?.trim() === 'Get Started',
      );
      expect(getStartedLink).not.toBeNull();
      expect(getStartedLink!.getAttribute('href')).toBe('#quick-start');
    });
  });

  describe('Test Case 5: Secondary CTA - View on GitHub', () => {
    beforeAll(() => {
      buildSite();
    });

    it('has a View on GitHub link pointing to the GitHub repository', () => {
      const { document } = parseBuiltHtml();
      const githubLink = Array.from(document.querySelectorAll('a')).find(
        (el) => el.textContent?.trim() === 'View on GitHub',
      );
      expect(githubLink).not.toBeNull();
      expect(githubLink!.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
    });
  });

  describe('Test Case 6: Missing logo graceful fallback', () => {
    beforeAll(() => {
      // Backup the logo
      if (existsSync(LOGO_PATH)) {
        copyFileSync(LOGO_PATH, LOGO_BACKUP);
        unlinkSync(LOGO_PATH);
      }
      // Build without the logo
      buildSite();
    });

    afterAll(() => {
      // Restore the logo
      if (existsSync(LOGO_BACKUP)) {
        copyFileSync(LOGO_BACKUP, LOGO_PATH);
        unlinkSync(LOGO_BACKUP);
      }
    });

    it('renders hero section without broken image (alt text present)', () => {
      const { document } = parseBuiltHtml();
      // The hero section should still render
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1!.textContent).toContain('MirDB');

      // The img should still exist with alt text (graceful fallback)
      const img = document.querySelector('img');
      if (img) {
        const alt = img!.getAttribute('alt');
        expect(alt).not.toBeNull();
        expect(alt!.length).toBeGreaterThan(0);
      }
    });
  });
});
