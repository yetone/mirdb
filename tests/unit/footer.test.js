/**
 * Unit tests for Footer Section.
 * Owner: Scenario 13 - Footer Section
 *
 * Tests:
 * - Footer uses semantic footer element
 * - GitHub link exists with correct URL
 * - Footer contains "MirDB" text
 * - Footer contains copyright/author attribution
 * - External links use target='_blank' and rel='noopener noreferrer'
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'fs';
import path from 'path';

const htmlPath = path.resolve(__dirname, '../../index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

describe('Footer Unit Tests', () => {
  beforeEach(() => {
    document.body.innerHTML = htmlContent;
  });

  afterEach(() => {
    document.body.innerHTML = '';
  });

  // Test Case 1: footer element exists as semantic footer
  it('footer element uses semantic footer tag', () => {
    const footer = document.querySelector('footer');
    expect(footer).not.toBeNull();
    expect(footer.tagName.toLowerCase()).toBe('footer');
  });

  // Test Case 2: GitHub link in footer
  it('footer contains GitHub link pointing to https://github.com/yetone/mirdb', () => {
    const footer = document.querySelector('footer');
    const githubLink = footer.querySelector('[data-testid="github-link-footer"]');
    expect(githubLink).not.toBeNull();
    expect(githubLink.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
  });

  // Test Case 3: Text 'MirDB' in footer
  it('footer contains text "MirDB"', () => {
    const footer = document.querySelector('footer');
    const text = footer.textContent;
    expect(text).toContain('MirDB');
  });

  // Test Case 4: Copyright or author attribution
  it('footer contains copyright text with author attribution', () => {
    const footer = document.querySelector('footer');
    const copyright = footer.querySelector('.footer-copyright');
    expect(copyright).not.toBeNull();
    expect(copyright.textContent.toLowerCase()).toContain('yetone');
  });

  // Test Case 5: External links use target='_blank' and rel='noopener noreferrer'
  it('all external links in footer use target="_blank" and rel="noopener noreferrer"', () => {
    const footer = document.querySelector('footer');
    const externalLinks = footer.querySelectorAll('a[target="_blank"]');
    expect(externalLinks.length).toBeGreaterThanOrEqual(1);

    externalLinks.forEach(link => {
      expect(link.getAttribute('target')).toBe('_blank');
      expect(link.getAttribute('rel')).toBe('noopener noreferrer');
    });
  });

  // Additional: Footer contains navigation
  it('footer contains navigation links', () => {
    const footer = document.querySelector('footer');
    const links = footer.querySelectorAll('a');
    expect(links.length).toBeGreaterThanOrEqual(1);
  });

  // Additional: Footer contains CircleCI badge
  it('footer contains CircleCI build status badge', () => {
    const footer = document.querySelector('footer');
    const badge = footer.querySelector('[data-testid="circleci-badge"]');
    expect(badge).not.toBeNull();
    expect(badge.getAttribute('alt')).toBe('CircleCI build status');
  });

  // Additional: Footer contains project tagline
  it('footer contains project tagline', () => {
    const footer = document.querySelector('footer');
    const tagline = footer.querySelector('.footer-tagline');
    expect(tagline).not.toBeNull();
    expect(tagline.textContent.toLowerCase()).toContain('persistent key-value store');
  });
});
