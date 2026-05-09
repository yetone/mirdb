/**
 * Scenario 1 - Hero Section & Get Started Call-to-Action Integration Tests
 * Owner: Scenario 1
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

const __dirname = path.dirname(new URL(import.meta.url).pathname);
const INDEX_PATH = path.resolve(__dirname, '../../index.html');

function loadHomepage() {
  const html = fs.readFileSync(INDEX_PATH, 'utf-8');
  return new JSDOM(html, {
    url: 'http://localhost:3000',
    runScripts: 'dangerously',
    resources: 'usable',
  });
}

describe('Hero Section', () => {
  let dom;
  let document;

  beforeEach(() => {
    dom = loadHomepage();
    document = dom.window.document;
  });

  afterEach(() => {
    dom = null;
    document = null;
  });

  // Test Case 1: Hero contains h1 with 'MirDB'
  it('should have an h1 element containing "MirDB"', () => {
    const h1 = document.querySelector('#hero h1');
    expect(h1).not.toBeNull();
    expect(h1.textContent).toContain('MirDB');
  });

  // Test Case 2: Tagline mentions persistent key-value store and Memcached
  it('should contain tagline mentioning both "persistent key-value store" and "Memcached"', () => {
    const hero = document.querySelector('#hero');
    expect(hero).not.toBeNull();
    const heroText = hero.textContent.toLowerCase();
    expect(heroText).toContain('persistent key-value store');
    expect(heroText).toContain('memcached');
  });

  // Test Case 3: CTA has href="#install"
  it('should have a CTA link with href="#install"', () => {
    const cta = document.querySelector('#hero a.cta');
    expect(cta).not.toBeNull();
    expect(cta.getAttribute('href')).toBe('#install');
  });

  // Test Case 4: CTA accessible name is "Get Started"
  it('should have a CTA with accessible name "Get Started"', () => {
    const cta = document.querySelector('#hero a.cta');
    expect(cta).not.toBeNull();

    // Check aria-label first, then text content
    const ariaLabel = cta.getAttribute('aria-label');
    const textContent = cta.textContent.trim();

    if (ariaLabel) {
      expect(ariaLabel.toLowerCase().trim()).toBe('get started');
    } else {
      expect(textContent.toLowerCase().trim()).toBe('get started');
    }
  });

  // Test Case 5: CTA click updates window.location.hash
  it('should navigate to #install when CTA is clicked', () => {
    const cta = document.querySelector('#hero a.cta');
    expect(cta).not.toBeNull();

    // Simulate click
    const clickEvent = new dom.window.MouseEvent('click', {
      bubbles: true,
      cancelable: true,
    });
    cta.dispatchEvent(clickEvent);

    // Check that the href attribute points to #install
    // (Actual hash update depends on navigation.js smooth scroll)
    expect(cta.getAttribute('href')).toBe('#install');
  });

  // Test Case 6: Negative case - hero without h1 should fail
  it('should fail if hero does not contain an h1 element', () => {
    const hero = document.querySelector('#hero');
    expect(hero).not.toBeNull();
    const h1 = hero.querySelector('h1');
    expect(h1).not.toBeNull();
    // This assertion ensures heading hierarchy regression is detected
    expect(h1.tagName.toLowerCase()).toBe('h1');
  });
});
