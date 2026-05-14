/**
 * Unit tests for Footer component.
 * Tests GitHub link, external link security attributes, copyright, license,
 * documentation link, footer element presence, and link navigation.
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync, existsSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { execSync } from 'node:child_process';
import { parseHTML } from 'linkedom';

const HOMEPAGE_DIR = join(__dirname, '..', '..', '..');
const DIST_DIR = join(HOMEPAGE_DIR, 'dist');
const INDEX_HTML = join(DIST_DIR, 'index.html');

// Ensure esbuild binaries are executable (needed in sandboxed environments)
function fixEsbuildPermissions() {
  try {
    execSync(
      'find node_modules -name "esbuild" -path "*/bin/*" -exec chmod +x {} \\; 2>/dev/null',
      { cwd: HOMEPAGE_DIR, stdio: 'pipe' },
    );
  } catch {
    // Best-effort, ignore failures
  }
}

function buildSite() {
  fixEsbuildPermissions();
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

describe('Footer & External Links', () => {
  beforeAll(() => {
    buildSite();
  });

  // Test Case 1: GitHub link
  describe('Test Case 1: GitHub link', () => {
    it('has a link with href pointing to the MirDB GitHub repository', () => {
      const { document } = parseBuiltHtml();
      const githubLink = document.querySelector('[data-footer-link="github"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink!.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
    });

    it('has descriptive link text for the GitHub link', () => {
      const { document } = parseBuiltHtml();
      const githubLink = document.querySelector('[data-footer-link="github"]');
      expect(githubLink).not.toBeNull();
      const text = githubLink!.textContent?.trim() || '';
      expect(text.length).toBeGreaterThan(0);
      expect(text).toBe('GitHub');
    });
  });

  // Test Case 2: External link security attributes
  describe('Test Case 2: External link security attributes', () => {
    it('every external link in the footer has target="_blank"', () => {
      const { document } = parseBuiltHtml();
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const externalLinks = Array.from(
        footer!.querySelectorAll('a[href^="http"]'),
      );
      expect(externalLinks.length).toBeGreaterThan(0);

      for (const link of externalLinks) {
        expect(link.getAttribute('target')).toBe('_blank');
      }
    });

    it('every external link in the footer has rel="noopener noreferrer"', () => {
      const { document } = parseBuiltHtml();
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const externalLinks = Array.from(
        footer!.querySelectorAll('a[href^="http"]'),
      );
      expect(externalLinks.length).toBeGreaterThan(0);

      for (const link of externalLinks) {
        expect(link.getAttribute('rel')).toBe('noopener noreferrer');
      }
    });
  });

  // Test Case 3: Copyright notice and license
  describe('Test Case 3: Copyright notice', () => {
    it('displays a copyright notice with the copyright symbol and a year', () => {
      const { document } = parseBuiltHtml();
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const copyrightEl = footer!.querySelector('.footerCopyright');
      expect(copyrightEl).not.toBeNull();
      const text = copyrightEl!.textContent?.trim() || '';
      expect(text).toMatch(/©\s*\d{4}/);
    });

    it('displays the project license type', () => {
      const { document } = parseBuiltHtml();
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      const licenseEl = footer!.querySelector('.footerLicense');
      expect(licenseEl).not.toBeNull();
      const text = licenseEl!.textContent?.trim() || '';
      expect(text.length).toBeGreaterThan(0);
      expect(text).toMatch(/MIT|Apache|BSD|GPL|License/i);
    });
  });

  // Test Case 4: Documentation link
  describe('Test Case 4: Documentation link', () => {
    it('has a link to documentation', () => {
      const { document } = parseBuiltHtml();
      const docsLink = document.querySelector('[data-footer-link="docs"]');
      expect(docsLink).not.toBeNull();
      expect(docsLink!.getAttribute('href')).toBeTruthy();
    });

    it('the documentation link is a valid URL', () => {
      const { document } = parseBuiltHtml();
      const docsLink = document.querySelector('[data-footer-link="docs"]');
      expect(docsLink).not.toBeNull();
      const href = docsLink!.getAttribute('href');
      expect(href).toMatch(/^https?:\/\//);
    });
  });

  // Test Case 5: Footer element presence with non-trivial content
  describe('Test Case 5: Footer element presence', () => {
    it('has a footer element within the page body', () => {
      const { document } = parseBuiltHtml();
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    it('contains non-trivial content beyond just a copyright line', () => {
      const { document } = parseBuiltHtml();
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      // Footer should have multiple link elements
      const links = footer!.querySelectorAll('a');
      expect(links.length).toBeGreaterThanOrEqual(3);

      // Footer text should mention MirDB and license
      const footerText = footer!.textContent?.trim() || '';
      expect(footerText).toContain('MirDB');
      expect(footerText).toContain('License');
    });
  });

  // Test Case 6: GitHub link navigation (integration)
  describe('Test Case 6: GitHub link navigation', () => {
    it('the GitHub link points to the correct repository URL', () => {
      const { document } = parseBuiltHtml();
      const githubLink = document.querySelector(
        '[data-footer-link="github"]',
      ) as HTMLAnchorElement;
      expect(githubLink).not.toBeNull();

      const href = githubLink!.getAttribute('href');
      expect(href).toBe('https://github.com/yetone/mirdb');

      // Verify it opens in a new tab
      expect(githubLink!.getAttribute('target')).toBe('_blank');
    });
  });
});
