/**
 * Accessibility Unit Tests
 * Owner: Scenario 10 - Accessibility Compliance
 *
 * Unit tests for semantic HTML structure and accessibility attributes
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('Semantic HTML Structure', () => {
  let document: Document;

  beforeEach(() => {
    const htmlPath = resolve(__dirname, '../../src/index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  it('should have a header element', () => {
    const header = document.querySelector('header');
    expect(header).not.toBeNull();
    expect(header?.classList.contains('header')).toBe(true);
  });

  it('should have a nav element with aria-label', () => {
    const nav = document.querySelector('nav');
    expect(nav).not.toBeNull();

    // Main navigation should have aria-label
    const mainNav = document.querySelector('nav[aria-label="Main navigation"]');
    expect(mainNav).not.toBeNull();
  });

  it('should have a main element with id for skip link', () => {
    const main = document.querySelector('main');
    expect(main).not.toBeNull();

    // Main should have id for skip link target
    expect(main?.id).toBe('main-content');
  });

  it('should have multiple section elements for content areas', () => {
    const sections = document.querySelectorAll('section');
    expect(sections.length).toBeGreaterThanOrEqual(6);

    // Check for specific section IDs
    const expectedSections = ['hero', 'features', 'quick-start', 'architecture', 'protocol', 'status'];
    expectedSections.forEach(sectionId => {
      const section = document.querySelector(`section#${sectionId}`);
      expect(section).not.toBeNull();
    });
  });

  it('should have article elements for feature cards', () => {
    const articles = document.querySelectorAll('article');
    expect(articles.length).toBeGreaterThan(0);

    // Feature cards should be articles
    const featureCards = document.querySelectorAll('article.feature-card');
    expect(featureCards.length).toBeGreaterThanOrEqual(6);
  });

  it('should have a footer element', () => {
    const footer = document.querySelector('footer');
    expect(footer).not.toBeNull();
    expect(footer?.classList.contains('footer')).toBe(true);
  });

  it('should have skip link for keyboard navigation', () => {
    const skipLink = document.querySelector('.skip-link');
    expect(skipLink).not.toBeNull();
    expect(skipLink?.getAttribute('href')).toBe('#main-content');
    expect(skipLink?.textContent).toContain('Skip to main content');
  });

  it('should have exactly one h1 heading', () => {
    const h1Elements = document.querySelectorAll('h1');
    expect(h1Elements.length).toBe(1);
    expect(h1Elements[0].textContent).toContain('MirDB');
  });

  it('should have proper heading hierarchy', () => {
    const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
    const levels = Array.from(headings).map(h => parseInt(h.tagName.charAt(1)));

    // First heading should be h1
    expect(levels[0]).toBe(1);

    // Check no level is skipped when going deeper
    let previousLevel = 0;
    for (const level of levels) {
      if (previousLevel > 0) {
        // Should not skip more than one level when going deeper
        const levelJump = level - previousLevel;
        expect(levelJump).toBeLessThanOrEqual(1);
      }
      previousLevel = level;
    }
  });

  it('should have aria-label on hero logo', () => {
    const heroLogo = document.querySelector('.hero-logo');
    expect(heroLogo).not.toBeNull();
    expect(heroLogo?.getAttribute('aria-label')).toBeTruthy();
    expect(heroLogo?.getAttribute('aria-label')?.toLowerCase()).toContain('logo');
  });

  it('should have aria-hidden on decorative elements', () => {
    // ASCII logo should be hidden from screen readers
    const asciiLogo = document.querySelector('.ascii-logo');
    expect(asciiLogo?.getAttribute('aria-hidden')).toBe('true');

    // Feature icons should be hidden
    const featureIcons = document.querySelectorAll('.feature-icon');
    featureIcons.forEach(icon => {
      expect(icon.getAttribute('aria-hidden')).toBe('true');
    });
  });

  it('should have footer navigation with aria-label', () => {
    const footerNav = document.querySelector('footer nav');
    expect(footerNav).not.toBeNull();
    expect(footerNav?.getAttribute('aria-label')).toBe('Footer navigation');
  });

  it('should have accessible button labels', () => {
    // Mobile menu toggle should have aria-label
    const mobileToggle = document.querySelector('.mobile-menu-toggle');
    expect(mobileToggle?.getAttribute('aria-label')).toBeTruthy();
    expect(mobileToggle?.getAttribute('aria-expanded')).toBe('false');
    expect(mobileToggle?.getAttribute('aria-controls')).toBeTruthy();

    // Copy buttons should have aria-label
    const copyButtons = document.querySelectorAll('.copy-button');
    copyButtons.forEach(button => {
      expect(button.getAttribute('aria-label')).toBeTruthy();
    });
  });

  it('should have proper link attributes for external links', () => {
    const externalLinks = document.querySelectorAll('a[target="_blank"]');
    externalLinks.forEach(link => {
      // External links should have rel="noopener noreferrer" for security
      const rel = link.getAttribute('rel');
      expect(rel).toContain('noopener');
    });
  });

  it('should have lang attribute on html element', () => {
    const html = document.querySelector('html');
    expect(html?.getAttribute('lang')).toBe('en');
  });

  it('should have meta viewport for responsive design', () => {
    const viewport = document.querySelector('meta[name="viewport"]');
    expect(viewport).not.toBeNull();
    expect(viewport?.getAttribute('content')).toContain('width=device-width');
  });
});
