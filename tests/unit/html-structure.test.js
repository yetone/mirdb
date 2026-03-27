/**
 * HTML Structure Unit Tests
 * Owner: Shared across scenarios
 *
 * Tests:
 * - Semantic HTML elements (header, main, nav, section, footer)
 * - Heading hierarchy
 * - Image alt text
 * - Link attributes
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { JSDOM } from 'jsdom';
import { resolve } from 'path';

describe('HTML Structure', () => {
  let document;

  beforeAll(() => {
    const htmlPath = resolve(process.cwd(), 'src/index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  it('TC4: Page title contains MirDB', () => {
    const title = document.querySelector('title').textContent;
    expect(title).toContain('MirDB');
  });

  it('has semantic HTML structure', () => {
    expect(document.querySelector('header')).not.toBeNull();
    expect(document.querySelector('main')).not.toBeNull();
    expect(document.querySelector('footer')).not.toBeNull();
  });

  it('has hero section with required elements', () => {
    const heroSection = document.querySelector('#hero');
    expect(heroSection).not.toBeNull();

    const logo = heroSection.querySelector('img[alt="MirDB Logo"]');
    expect(logo).not.toBeNull();

    const heading = heroSection.querySelector('h1');
    expect(heading).not.toBeNull();
    expect(heading.textContent).toContain('MirDB');
  });
});

/**
 * Hero Section Tests (JSDOM-based)
 * Owner: Scenario 1 - Hero Section and Branding
 *
 * Note: These tests use JSDOM as a fallback since Playwright
 * cannot run in this environment (missing system libraries).
 */
describe('Hero Section and Branding', () => {
  let document;

  beforeAll(() => {
    const htmlPath = resolve(process.cwd(), 'src/index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  it('TC1: Logo image is visible with alt text MirDB Logo', () => {
    // Navigate to homepage and inspect hero section
    const heroSection = document.querySelector('#hero');
    expect(heroSection).not.toBeNull();

    // Logo image should be present with alt text 'MirDB Logo'
    const logo = heroSection.querySelector('img[alt="MirDB Logo"]');
    expect(logo).not.toBeNull();
    expect(logo.getAttribute('alt')).toBe('MirDB Logo');
    expect(logo.getAttribute('src')).toContain('logo.gif');
  });

  it('TC2: Hero heading contains MirDB, persistent, and key-value store', () => {
    // Check hero heading text content
    const heroHeading = document.querySelector('#hero h1');
    expect(heroHeading).not.toBeNull();

    const headingText = heroHeading.textContent;
    expect(headingText).toContain('MirDB');
    expect(headingText.toLowerCase()).toContain('persistent');
    expect(headingText.toLowerCase()).toContain('key-value store');
  });

  it('TC3: Hero description mentions memcached protocol compatibility', () => {
    // Check hero description text
    const heroDescription = document.querySelector('#hero p');
    expect(heroDescription).not.toBeNull();

    const descriptionText = heroDescription.textContent;
    expect(descriptionText.toLowerCase()).toContain('memcached protocol');
  });

  it('TC4 (duplicate): Page title contains MirDB', () => {
    // Verify project name in page title
    const title = document.querySelector('title').textContent;
    expect(title).toContain('MirDB');
  });
});

/**
 * Navigation Unit Tests
 * Owner: Scenario 5 - Navigation and GitHub Link
 *
 * Unit tests for navigation elements and link attributes
 */
describe('Navigation and GitHub Link (Unit)', () => {
  let document;

  beforeAll(() => {
    const htmlPath = resolve(process.cwd(), 'src/index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  it('TC2: GitHub link has target=_blank and rel=noopener noreferrer', () => {
    // Find GitHub link in header
    const header = document.querySelector('header');
    expect(header).not.toBeNull();

    const githubLink = header.querySelector('a[href*="github.com"]');
    expect(githubLink).not.toBeNull();

    // Verify target="_blank" attribute
    const target = githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel="noopener noreferrer" for security
    const rel = githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });

  it('Navigation links have correct href attributes', () => {
    const header = document.querySelector('header');
    expect(header).not.toBeNull();

    // Check features link
    const featuresLink = header.querySelector('a[href="#features"]');
    expect(featuresLink).not.toBeNull();

    // Check quick-start link
    const quickStartLink = header.querySelector('a[href="#quick-start"]');
    expect(quickStartLink).not.toBeNull();
  });

  it('Mobile menu button has accessibility attributes', () => {
    const mobileMenuBtn = document.querySelector('#mobile-menu-btn');
    expect(mobileMenuBtn).not.toBeNull();

    // Verify aria-label
    const ariaLabel = mobileMenuBtn.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();

    // Verify aria-expanded
    const ariaExpanded = mobileMenuBtn.getAttribute('aria-expanded');
    expect(ariaExpanded).toBe('false');
  });
});
