/**
 * Unit tests for Hero Section (Scenario 1).
 *
 * Tests:
 * - Hero uses semantic section element with id='hero'
 * - h1 for product name
 * - Appropriate heading hierarchy
 * - Tagline is present
 * - CTA button exists with correct attributes
 */

import { describe, test, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('Hero Section HTML Structure', () => {
  let html;
  let parser;
  let doc;

  beforeAll(() => {
    const htmlPath = resolve(process.cwd(), 'index.html');
    html = readFileSync(htmlPath, 'utf-8');
    parser = new DOMParser();
    doc = parser.parseFromString(html, 'text/html');
  });

  test('hero uses semantic section element with id="hero"', () => {
    const heroSection = doc.querySelector('section#hero');
    expect(heroSection).not.toBeNull();
    expect(heroSection.tagName.toLowerCase()).toBe('section');
    expect(heroSection.id).toBe('hero');
  });

  test('product name is in h1 element', () => {
    const heroSection = doc.querySelector('section#hero');
    const h1 = heroSection.querySelector('h1');
    expect(h1).not.toBeNull();
    expect(h1.textContent.trim()).toBe('MirDB');
  });

  test('tagline is present as subheading', () => {
    const heroSection = doc.querySelector('section#hero');
    const tagline = heroSection.querySelector('.tagline');
    expect(tagline).not.toBeNull();
    expect(tagline.textContent.trim()).toBe('A Persistent Key-Value Store with Memcached Protocol');
  });

  test('primary CTA button exists with correct text and link', () => {
    const heroSection = doc.querySelector('section#hero');
    const cta = heroSection.querySelector('.cta-button');
    expect(cta).not.toBeNull();
    expect(cta.textContent.trim()).toBe('Get Started');
    expect(cta.getAttribute('href')).toBe('#quick-start');
  });

  test('heading hierarchy is appropriate (h1 inside hero, no skipped levels)', () => {
    const heroSection = doc.querySelector('section#hero');
    const headings = heroSection.querySelectorAll('h1, h2, h3, h4, h5, h6');
    // Hero should only have h1 (no lower-level headings inside hero)
    expect(headings.length).toBe(1);
    expect(headings[0].tagName.toLowerCase()).toBe('h1');
  });

  test('hero content is wrapped in a container div', () => {
    const heroSection = doc.querySelector('section#hero');
    const contentDiv = heroSection.querySelector('.hero-content');
    expect(contentDiv).not.toBeNull();
    // h1 should be inside .hero-content
    const h1InsideContent = contentDiv.querySelector('h1');
    expect(h1InsideContent).not.toBeNull();
  });
});
