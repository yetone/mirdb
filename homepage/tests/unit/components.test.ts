/**
 * Component Unit Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests for:
 * - Hero component rendering
 * - Component structure validation
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';
import { PRODUCT_NAME, HERO_KEYWORDS, CTA_TEXT } from '../fixtures/test-data';

// Load the Hero HTML component from file system
const heroHtmlPath = resolve(__dirname, '../../src/components/Hero/Hero.html');
const heroHtml = readFileSync(heroHtmlPath, 'utf-8');

describe('Hero Component', () => {
  beforeEach(() => {
    // Set up the DOM with Hero HTML
    document.body.innerHTML = heroHtml;
  });

  it('renders hero section without errors', () => {
    const heroSection = document.querySelector('#hero');
    expect(heroSection).not.toBeNull();
    expect(heroSection?.classList.contains('hero')).toBe(true);
  });

  it('contains product name MirDB in h1 element', () => {
    const h1 = document.querySelector('h1');
    expect(h1).not.toBeNull();
    expect(h1?.textContent).toContain(PRODUCT_NAME);
  });

  it('contains headline with required keywords', () => {
    const headline = document.querySelector('.hero__headline');
    expect(headline).not.toBeNull();

    const headlineText = headline?.textContent?.toLowerCase() || '';
    for (const keyword of HERO_KEYWORDS) {
      expect(headlineText).toContain(keyword.toLowerCase());
    }
  });

  it('has CTA button with Get Started text', () => {
    const cta = document.querySelector('.hero__cta');
    expect(cta).not.toBeNull();
    expect(cta?.textContent?.trim()).toContain(CTA_TEXT);
  });

  it('CTA button has correct href attribute', () => {
    const cta = document.querySelector('.hero__cta');
    expect(cta).not.toBeNull();
    expect(cta?.getAttribute('href')).toBe('#quickstart');
  });

  it('has proper accessibility structure', () => {
    const heroSection = document.querySelector('#hero');
    expect(heroSection?.getAttribute('aria-labelledby')).toBe('hero-title');

    const title = document.querySelector('#hero-title');
    expect(title).not.toBeNull();
  });
});

describe('Hero Component Structure', () => {
  beforeEach(() => {
    document.body.innerHTML = heroHtml;
  });

  it('has all required elements', () => {
    expect(document.querySelector('.hero')).not.toBeNull();
    expect(document.querySelector('.hero__content')).not.toBeNull();
    expect(document.querySelector('.hero__title')).not.toBeNull();
    expect(document.querySelector('.hero__headline')).not.toBeNull();
    expect(document.querySelector('.hero__description')).not.toBeNull();
    expect(document.querySelector('.hero__cta')).not.toBeNull();
  });

  it('title has accent span for styling', () => {
    const accentSpan = document.querySelector('.hero__title-accent');
    expect(accentSpan).not.toBeNull();
    expect(accentSpan?.textContent).toContain(PRODUCT_NAME);
  });
});
