/**
 * Unit tests for Accessibility Compliance.
 * Owner: Scenario 11 - Accessibility Compliance
 *
 * Tests:
 * - Semantic HTML elements (header, nav, main, sections, footer)
 * - Heading hierarchy (single h1, proper h2/h3 ordering)
 * - ARIA labels on interactive components
 */

import { describe, test, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';
import { JSDOM } from 'jsdom';

const htmlPath = resolve(process.cwd(), 'index.html');
const html = readFileSync(htmlPath, 'utf-8');

describe('Accessibility Compliance - Semantic HTML', () => {
  let document;

  beforeAll(() => {
    const dom = new JSDOM(html, { url: 'http://localhost' });
    document = dom.window.document;
  });

  test('page contains header element', () => {
    const header = document.querySelector('header');
    expect(header).not.toBeNull();
  });

  test('page contains nav element', () => {
    const nav = document.querySelector('nav');
    expect(nav).not.toBeNull();
  });

  test('page contains main element', () => {
    const main = document.querySelector('main');
    expect(main).not.toBeNull();
  });

  test('page contains at least 5 section elements', () => {
    const sections = document.querySelectorAll('section');
    expect(sections.length).toBeGreaterThanOrEqual(5);
  });

  test('page contains footer element', () => {
    const footer = document.querySelector('footer');
    expect(footer).not.toBeNull();
  });

  test('main element has id="main-content"', () => {
    const main = document.querySelector('main#main-content');
    expect(main).not.toBeNull();
  });

  test('html element has lang="en"', () => {
    const htmlEl = document.querySelector('html');
    expect(htmlEl.getAttribute('lang')).toBe('en');
  });

  test('page has a title', () => {
    const title = document.querySelector('title');
    expect(title).not.toBeNull();
    expect(title.textContent.trim().length).toBeGreaterThan(0);
  });

  test('page has viewport meta tag', () => {
    const viewport = document.querySelector('meta[name="viewport"]');
    expect(viewport).not.toBeNull();
  });

  test('page has charset meta tag', () => {
    const charset = document.querySelector('meta[charset]');
    expect(charset).not.toBeNull();
  });
});

describe('Accessibility Compliance - Heading Hierarchy', () => {
  let document;

  beforeAll(() => {
    const dom = new JSDOM(html, { url: 'http://localhost' });
    document = dom.window.document;
  });

  test('page has exactly one h1 element', () => {
    const h1s = document.querySelectorAll('h1');
    expect(h1s.length).toBe(1);
  });

  test('all headings follow sequential order with no skipped levels', () => {
    const headings = Array.from(document.querySelectorAll('h1, h2, h3, h4, h5, h6'));
    expect(headings.length).toBeGreaterThan(0);

    const levels = headings.map(h => parseInt(h.tagName[1], 10));

    // First heading must be h1
    expect(levels[0]).toBe(1);

    // No level should skip more than 1 from the previous
    for (let i = 1; i < levels.length; i++) {
      const prev = levels[i - 1];
      const curr = levels[i];
      expect(curr).toBeLessThanOrEqual(prev + 1);
    }
  });

  test('h1 is followed by h2 (no h3 before h2)', () => {
    const headings = Array.from(document.querySelectorAll('h1, h2, h3'));
    const h1Index = headings.findIndex(h => h.tagName === 'H1');
    const firstH2Index = headings.findIndex(h => h.tagName === 'H2');
    const firstH3Index = headings.findIndex(h => h.tagName === 'H3');

    // If there are h3s, the first h3 should come after at least one h2
    if (firstH3Index !== -1 && firstH2Index !== -1) {
      expect(firstH3Index).toBeGreaterThan(firstH2Index);
    }
  });
});

describe('Accessibility Compliance - ARIA Labels', () => {
  let document;

  beforeAll(() => {
    const dom = new JSDOM(html, { url: 'http://localhost' });
    document = dom.window.document;
  });

  test('mobile menu button has aria-label', () => {
    const mobileMenuToggle = document.querySelector('.mobile-menu-toggle');
    expect(mobileMenuToggle).not.toBeNull();
    const ariaLabel = mobileMenuToggle.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.length).toBeGreaterThan(0);
  });

  test('mobile menu button has aria-expanded', () => {
    const mobileMenuToggle = document.querySelector('.mobile-menu-toggle');
    expect(mobileMenuToggle).not.toBeNull();
    const ariaExpanded = mobileMenuToggle.getAttribute('aria-expanded');
    expect(ariaExpanded).toBeTruthy();
  });

  test('mobile menu button has aria-controls', () => {
    const mobileMenuToggle = document.querySelector('.mobile-menu-toggle');
    expect(mobileMenuToggle).not.toBeNull();
    const ariaControls = mobileMenuToggle.getAttribute('aria-controls');
    expect(ariaControls).toBeTruthy();
    expect(ariaControls).toBe('nav-menu');
  });

  test('theme toggle button has aria-label', () => {
    const themeToggle = document.querySelector('.theme-toggle');
    expect(themeToggle).not.toBeNull();
    const ariaLabel = themeToggle.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.length).toBeGreaterThan(0);
  });

  test('theme toggle button has aria-pressed', () => {
    const themeToggle = document.querySelector('.theme-toggle');
    expect(themeToggle).not.toBeNull();
    const ariaPressed = themeToggle.getAttribute('aria-pressed');
    expect(ariaPressed).toBeTruthy();
  });

  test('all copy buttons have aria-label', () => {
    const copyButtons = document.querySelectorAll('.copy-button');
    expect(copyButtons.length).toBeGreaterThan(0);

    copyButtons.forEach(btn => {
      const ariaLabel = btn.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.length).toBeGreaterThan(0);
    });
  });

  test('nav element has aria-label', () => {
    const nav = document.querySelector('nav');
    expect(nav).not.toBeNull();
    const ariaLabel = nav.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.length).toBeGreaterThan(0);
  });

  test('status section has aria-labelledby', () => {
    const statusSection = document.querySelector('#status');
    expect(statusSection).not.toBeNull();
    const ariaLabelledBy = statusSection.getAttribute('aria-labelledby');
    expect(ariaLabelledBy).toBeTruthy();
  });

  test('status list has aria-label', () => {
    const statusList = document.querySelector('.status-list');
    expect(statusList).not.toBeNull();
    const ariaLabel = statusList.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.length).toBeGreaterThan(0);
  });

  test('CircleCI badge link has aria-label', () => {
    const badgeLink = document.querySelector('.footer-badges a');
    expect(badgeLink).not.toBeNull();
    const ariaLabel = badgeLink.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.length).toBeGreaterThan(0);
  });
});
