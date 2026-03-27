/**
 * Badge Unit Tests
 * Owner: Scenario 6 - Project Status Badges
 *
 * Tests:
 * - Badge image has descriptive alt text
 * - Badge link has proper accessibility attributes
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { JSDOM } from 'jsdom';
import { resolve } from 'path';

describe('Project Status Badges', () => {
  let document;

  beforeAll(() => {
    const htmlPath = resolve(process.cwd(), 'src/index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  it('TC3: Badge image has descriptive alt text like Build Status', () => {
    const statusSection = document.querySelector('#status');
    expect(statusSection).not.toBeNull();

    // Find badge images
    const badgeImages = statusSection.querySelectorAll('img');
    expect(badgeImages.length).toBeGreaterThan(0);

    // Each badge should have descriptive alt text
    badgeImages.forEach((img) => {
      const altText = img.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText.length).toBeGreaterThan(0);
      // Should contain descriptive keywords like 'Status', 'Build', 'Badge', etc.
      const hasDescriptiveAlt = /status|build|badge|ci|test/i.test(altText);
      expect(hasDescriptiveAlt).toBe(true);
    });
  });

  it('CircleCI badge specifically has alt text Build Status', () => {
    const statusSection = document.querySelector('#status');
    const circleCIBadge = statusSection.querySelector('img[src*="circleci"]');
    expect(circleCIBadge).not.toBeNull();
    expect(circleCIBadge.getAttribute('alt')).toBe('Build Status');
  });

  it('Status section exists with proper heading', () => {
    const statusSection = document.querySelector('#status');
    expect(statusSection).not.toBeNull();

    const heading = statusSection.querySelector('h2');
    expect(heading).not.toBeNull();
    expect(heading.textContent).toContain('Project Status');
  });

  it('Badge link has accessibility attributes', () => {
    const statusSection = document.querySelector('#status');
    const badgeLink = statusSection.querySelector('a[href*="circleci"]');
    expect(badgeLink).not.toBeNull();

    // Should have aria-label or other accessibility attribute
    const ariaLabel = badgeLink.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
  });

  it('Badge link has proper security attributes', () => {
    const statusSection = document.querySelector('#status');
    const badgeLink = statusSection.querySelector('a[href*="circleci"]');
    expect(badgeLink).not.toBeNull();

    // Should have rel="noopener noreferrer" for security
    const rel = badgeLink.getAttribute('rel');
    expect(rel).toContain('noopener');

    // Should open in new tab
    expect(badgeLink.getAttribute('target')).toBe('_blank');
  });
});
