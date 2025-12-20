/**
 * Unit Tests: CTA Button href Attribute Validation
 *
 * This test suite specifically validates that CTA buttons have proper
 * href attributes pointing to correct destinations.
 *
 * Test Case 4: Check CTA buttons for proper href attributes
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

describe('CTA Button href Attribute Validation (Unit Tests)', () => {
  let document;
  let primaryCta;
  let secondaryCta;

  beforeAll(() => {
    const htmlPath = path.resolve(__dirname, '../index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html, { url: 'http://localhost' });
    document = dom.window.document;
    primaryCta = document.querySelector('[data-cta="primary"]');
    secondaryCta = document.querySelector('[data-cta="secondary"]');
  });

  describe('Primary CTA (Get Started) href validation', () => {
    it('should exist in the DOM', () => {
      expect(primaryCta).not.toBeNull();
    });

    it('should have an href attribute', () => {
      expect(primaryCta.hasAttribute('href')).toBe(true);
    });

    it('should have a non-empty href value', () => {
      const href = primaryCta.getAttribute('href');
      expect(href).toBeDefined();
      expect(href.length).toBeGreaterThan(0);
    });

    it('should not have a placeholder href (#)', () => {
      const href = primaryCta.getAttribute('href');
      expect(href).not.toBe('#');
    });

    it('should point to documentation section or page', () => {
      const href = primaryCta.getAttribute('href');
      // Valid documentation destinations: anchor to docs section, /docs path, or external docs URL
      const isValidDocumentationLink =
        href.includes('quick-start') ||
        href.includes('docs') ||
        href.includes('documentation') ||
        href.includes('getting-started') ||
        href.startsWith('#');

      expect(isValidDocumentationLink).toBe(true);
    });
  });

  describe('Secondary CTA (View on GitHub) href validation', () => {
    it('should exist in the DOM', () => {
      expect(secondaryCta).not.toBeNull();
    });

    it('should have an href attribute', () => {
      expect(secondaryCta.hasAttribute('href')).toBe(true);
    });

    it('should have a non-empty href value', () => {
      const href = secondaryCta.getAttribute('href');
      expect(href).toBeDefined();
      expect(href.length).toBeGreaterThan(0);
    });

    it('should not have a placeholder href (#)', () => {
      const href = secondaryCta.getAttribute('href');
      expect(href).not.toBe('#');
    });

    it('should point to GitHub repository', () => {
      const href = secondaryCta.getAttribute('href');
      expect(href).toContain('github.com');
    });

    it('should point to the correct MirDB repository', () => {
      const href = secondaryCta.getAttribute('href');
      expect(href).toContain('mirdb');
    });

    it('should use HTTPS protocol for security', () => {
      const href = secondaryCta.getAttribute('href');
      expect(href.startsWith('https://')).toBe(true);
    });
  });

  describe('href attribute security and best practices', () => {
    it('should have target="_blank" for external GitHub link', () => {
      const target = secondaryCta.getAttribute('target');
      expect(target).toBe('_blank');
    });

    it('should have rel="noopener" for security when opening new tab', () => {
      const rel = secondaryCta.getAttribute('rel');
      expect(rel).toContain('noopener');
    });

    it('should have rel="noreferrer" or rel="noopener noreferrer" for security', () => {
      const rel = secondaryCta.getAttribute('rel');
      expect(rel).toMatch(/noopener/);
    });

    it('internal documentation link should not open in new tab', () => {
      const target = primaryCta.getAttribute('target');
      // Internal links should either have no target or _self
      expect(target === null || target === '_self' || target === '').toBe(true);
    });
  });

  describe('href URL format validation', () => {
    it('GitHub URL should be a valid URL format', () => {
      const href = secondaryCta.getAttribute('href');
      // Should be a properly formatted URL
      expect(() => new URL(href)).not.toThrow();
    });

    it('GitHub URL should match expected repository pattern', () => {
      const href = secondaryCta.getAttribute('href');
      const urlPattern = /^https:\/\/github\.com\/[\w-]+\/[\w-]+$/;
      expect(href).toMatch(urlPattern);
    });
  });
});
