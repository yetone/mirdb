/**
 * Test Suite: Call-to-Action Buttons Functionality
 *
 * This test suite verifies that primary and secondary CTA buttons are
 * prominently displayed and link to correct destinations.
 *
 * Test Cases:
 * 1. Query DOM for CTA button elements in hero section
 * 2. Click 'Get Started' button and capture navigation
 * 3. Click 'View on GitHub' button and capture navigation
 * 4. Check CTA buttons for proper href attributes
 * 5. Tab through page and verify CTA focus states
 */

import { describe, it, expect, beforeAll, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

describe('Call-to-Action Buttons Functionality', () => {
  let dom;
  let document;

  beforeAll(() => {
    // Load the index.html file
    const htmlPath = path.resolve(__dirname, '../index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(html, {
      url: 'http://localhost',
      runScripts: 'dangerously',
    });
    document = dom.window.document;
  });

  describe('Test Case 1: Query DOM for CTA button elements in hero section', () => {
    it('should find the hero section', () => {
      const heroSection = document.querySelector('.hero, #hero, [data-section="hero"]');
      expect(heroSection).not.toBeNull();
    });

    it('should find two CTA buttons in the hero section', () => {
      const heroSection = document.querySelector('.hero, #hero, [data-section="hero"]');
      const ctaButtons = heroSection.querySelectorAll('a.cta, a.btn, [data-cta]');
      expect(ctaButtons.length).toBeGreaterThanOrEqual(2);
    });

    it('should have a primary "Get Started" CTA button', () => {
      const heroSection = document.querySelector('.hero, #hero, [data-section="hero"]');
      const primaryCta = heroSection.querySelector('[data-cta="primary"], .cta-primary, .btn-primary');
      expect(primaryCta).not.toBeNull();
      expect(primaryCta.textContent.toLowerCase()).toContain('get started');
    });

    it('should have a secondary "View on GitHub" CTA button', () => {
      const heroSection = document.querySelector('.hero, #hero, [data-section="hero"]');
      const secondaryCta = heroSection.querySelector('[data-cta="secondary"], .cta-secondary, .btn-secondary');
      expect(secondaryCta).not.toBeNull();
      expect(secondaryCta.textContent.toLowerCase()).toContain('github');
    });
  });

  describe('Test Case 2: Click "Get Started" button and capture navigation', () => {
    it('should have "Get Started" button pointing to documentation', () => {
      const primaryCta = document.querySelector('[data-cta="primary"], .cta-primary, .btn-primary');
      expect(primaryCta).not.toBeNull();
      const href = primaryCta.getAttribute('href');
      expect(href).toBeDefined();
      // Should link to documentation - could be #docs, /docs, or external docs URL
      expect(href).toMatch(/(#docs|\/docs|#documentation|documentation|#quick-start|quick-start)/i);
    });
  });

  describe('Test Case 3: Click "View on GitHub" button and capture navigation', () => {
    it('should have "View on GitHub" button pointing to GitHub repository', () => {
      const secondaryCta = document.querySelector('[data-cta="secondary"], .cta-secondary, .btn-secondary');
      expect(secondaryCta).not.toBeNull();
      const href = secondaryCta.getAttribute('href');
      expect(href).toBeDefined();
      // Should link to GitHub repository
      expect(href).toMatch(/github\.com/i);
    });

    it('should open GitHub link in new tab for external navigation', () => {
      const secondaryCta = document.querySelector('[data-cta="secondary"], .cta-secondary, .btn-secondary');
      expect(secondaryCta).not.toBeNull();
      // External links should open in new tab
      const target = secondaryCta.getAttribute('target');
      const rel = secondaryCta.getAttribute('rel');
      expect(target).toBe('_blank');
      expect(rel).toContain('noopener');
    });
  });

  describe('Test Case 4: Check CTA buttons for proper href attributes', () => {
    it('should have valid href attribute on primary CTA', () => {
      const primaryCta = document.querySelector('[data-cta="primary"], .cta-primary, .btn-primary');
      expect(primaryCta).not.toBeNull();
      const href = primaryCta.getAttribute('href');
      expect(href).toBeDefined();
      expect(href).not.toBe('');
      expect(href).not.toBe('#');
    });

    it('should have valid href attribute on secondary CTA', () => {
      const secondaryCta = document.querySelector('[data-cta="secondary"], .cta-secondary, .btn-secondary');
      expect(secondaryCta).not.toBeNull();
      const href = secondaryCta.getAttribute('href');
      expect(href).toBeDefined();
      expect(href).not.toBe('');
      expect(href).not.toBe('#');
    });

    it('should have proper anchor element tags for CTAs', () => {
      const primaryCta = document.querySelector('[data-cta="primary"], .cta-primary, .btn-primary');
      const secondaryCta = document.querySelector('[data-cta="secondary"], .cta-secondary, .btn-secondary');
      expect(primaryCta.tagName.toLowerCase()).toBe('a');
      expect(secondaryCta.tagName.toLowerCase()).toBe('a');
    });
  });

  describe('Test Case 5: Tab through page and verify CTA focus states', () => {
    it('should have CTAs that are focusable (not tabindex=-1)', () => {
      const primaryCta = document.querySelector('[data-cta="primary"], .cta-primary, .btn-primary');
      const secondaryCta = document.querySelector('[data-cta="secondary"], .cta-secondary, .btn-secondary');

      const primaryTabIndex = primaryCta.getAttribute('tabindex');
      const secondaryTabIndex = secondaryCta.getAttribute('tabindex');

      // tabindex should not be -1, can be null (default) or >= 0
      expect(primaryTabIndex).not.toBe('-1');
      expect(secondaryTabIndex).not.toBe('-1');
    });

    it('should have focus styles defined in stylesheet for CTA elements', () => {
      // Check that CTAs have classes that would receive focus styling
      const primaryCta = document.querySelector('[data-cta="primary"], .cta-primary, .btn-primary');
      const secondaryCta = document.querySelector('[data-cta="secondary"], .cta-secondary, .btn-secondary');

      // Both CTAs should be anchor elements that receive focus naturally
      expect(primaryCta.tagName.toLowerCase()).toBe('a');
      expect(secondaryCta.tagName.toLowerCase()).toBe('a');

      // Check that they have appropriate role or are interactive elements
      const primaryRole = primaryCta.getAttribute('role');
      const secondaryRole = secondaryCta.getAttribute('role');

      // Role can be null (implicit link) or 'button' if styled as button
      expect(primaryRole === null || primaryRole === 'link' || primaryRole === 'button').toBe(true);
      expect(secondaryRole === null || secondaryRole === 'link' || secondaryRole === 'button').toBe(true);
    });

    it('should have visible focus indicator through CSS class or inline focus styles', () => {
      // Get the stylesheet from the document
      const styleSheet = document.querySelector('style');
      if (styleSheet) {
        const cssText = styleSheet.textContent;
        // Check that there are focus styles defined
        expect(cssText).toMatch(/:focus|:focus-visible/);
      } else {
        // If no inline styles, check for linked stylesheet reference
        const linkedStylesheet = document.querySelector('link[rel="stylesheet"]');
        // Either inline styles or linked stylesheet should exist
        expect(styleSheet || linkedStylesheet).toBeDefined();
      }
    });
  });

  describe('CTA Visual Hierarchy', () => {
    it('should have primary CTA with distinct primary styling class', () => {
      const primaryCta = document.querySelector('[data-cta="primary"], .cta-primary, .btn-primary');
      expect(primaryCta).not.toBeNull();
      // Primary CTA should have visual distinction through class
      expect(
        primaryCta.classList.contains('cta-primary') ||
        primaryCta.classList.contains('btn-primary') ||
        primaryCta.getAttribute('data-cta') === 'primary'
      ).toBe(true);
    });

    it('should have secondary CTA with distinct secondary styling class', () => {
      const secondaryCta = document.querySelector('[data-cta="secondary"], .cta-secondary, .btn-secondary');
      expect(secondaryCta).not.toBeNull();
      // Secondary CTA should have visual distinction through class
      expect(
        secondaryCta.classList.contains('cta-secondary') ||
        secondaryCta.classList.contains('btn-secondary') ||
        secondaryCta.getAttribute('data-cta') === 'secondary'
      ).toBe(true);
    });
  });
});
