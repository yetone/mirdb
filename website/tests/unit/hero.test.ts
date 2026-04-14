/**
 * Hero Section Unit Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests for:
 * - Hero component rendering
 * - Required elements presence
 */
import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('Hero Component Unit Tests', () => {
  let document: Document;

  beforeEach(() => {
    // Load the HTML file
    const htmlPath = resolve(__dirname, '../../src/index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  it('TC5: Hero component renders without errors and contains required elements', () => {
    // Get hero section
    const heroSection = document.querySelector('#hero');
    expect(heroSection).not.toBeNull();

    // Check logo area
    const heroLogo = document.querySelector('.hero-logo');
    expect(heroLogo).not.toBeNull();

    // Check title
    const heroTitle = document.querySelector('.hero-title');
    expect(heroTitle).not.toBeNull();
    expect(heroTitle?.textContent).toContain('MirDB');

    // Check tagline
    const heroTagline = document.querySelector('.hero-tagline');
    expect(heroTagline).not.toBeNull();
    expect(heroTagline?.textContent).toContain('Persistent Key-Value Store');

    // Check CTA buttons
    const ctaPrimary = document.querySelector('.cta-primary');
    expect(ctaPrimary).not.toBeNull();
    expect(ctaPrimary?.textContent).toContain('Get Started');
    expect(ctaPrimary?.getAttribute('href')).toBe('#quick-start');

    const ctaSecondary = document.querySelector('.cta-secondary');
    expect(ctaSecondary).not.toBeNull();
    expect(ctaSecondary?.textContent).toContain('View on GitHub');
    expect(ctaSecondary?.getAttribute('target')).toBe('_blank');
  });

  it('should have proper semantic structure', () => {
    // Check hero is a section element
    const heroSection = document.querySelector('#hero');
    expect(heroSection?.tagName.toLowerCase()).toBe('section');

    // Check h1 exists for main heading
    const h1 = document.querySelector('h1');
    expect(h1).not.toBeNull();
    expect(h1?.textContent).toContain('MirDB');
  });

  it('should have accessible logo with aria-label', () => {
    const heroLogo = document.querySelector('.hero-logo');
    expect(heroLogo?.getAttribute('aria-label')).toBe('MirDB Logo');

    // ASCII art should be hidden from screen readers
    const asciiLogo = document.querySelector('.ascii-logo');
    expect(asciiLogo?.getAttribute('aria-hidden')).toBe('true');
  });
});
