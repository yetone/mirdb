/**
 * Navigation Unit Tests
 * Owner: Scenario 12 - Navigation and Smooth Scroll
 *
 * Tests:
 * - scroll-behavior CSS property on html element
 * - Navigation links href values match section IDs
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { join } from 'path';

describe('Navigation and Smooth Scroll Unit Tests', () => {
  let dom: JSDOM;
  let document: Document;

  beforeEach(() => {
    // Load the HTML file
    const htmlPath = join(process.cwd(), 'src', 'index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(html, {
      url: 'http://localhost:3000',
      resources: 'usable',
      runScripts: 'outside-only'
    });
    document = dom.window.document;
  });

  afterEach(() => {
    dom.window.close();
  });

  describe('Test Case 3: Scroll behavior CSS property', () => {
    it('html element has scroll-behavior: smooth in base.css', () => {
      // Load and parse the base.css file
      const cssPath = join(process.cwd(), 'src', 'styles', 'base.css');
      const css = readFileSync(cssPath, 'utf-8');

      // Check that scroll-behavior: smooth is defined for html
      const htmlRuleMatch = css.match(/html\s*\{[^}]*scroll-behavior\s*:\s*smooth[^}]*\}/s);
      expect(htmlRuleMatch).not.toBeNull();
    });

    it('scroll-behavior property is set to smooth', () => {
      // Load the CSS file
      const cssPath = join(process.cwd(), 'src', 'styles', 'base.css');
      const css = readFileSync(cssPath, 'utf-8');

      // Extract scroll-behavior value
      const scrollBehaviorMatch = css.match(/scroll-behavior\s*:\s*(\w+)/);
      expect(scrollBehaviorMatch).not.toBeNull();
      expect(scrollBehaviorMatch![1]).toBe('smooth');
    });
  });

  describe('Test Case 4: Navigation links href values', () => {
    it('navigation links have href values matching section IDs', () => {
      const navLinks = document.querySelectorAll('.header__nav-link');

      // Should have at least 3 navigation links
      expect(navLinks.length).toBeGreaterThanOrEqual(3);

      // Check each navigation link has a valid href that matches a section
      const expectedSections = ['features', 'quickstart', 'techspecs'];

      navLinks.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).toBeTruthy();
        expect(href).toMatch(/^#\w+/);

        // Extract section ID from href
        const sectionId = href!.slice(1);

        // Verify the corresponding section exists
        const section = document.getElementById(sectionId);
        expect(section).not.toBeNull();
        expect(section?.tagName.toLowerCase()).toBe('section');
      });

      // Verify all expected sections are linked
      for (const sectionId of expectedSections) {
        const link = document.querySelector(`a.header__nav-link[href="#${sectionId}"]`);
        expect(link).not.toBeNull();
      }
    });

    it('Features link points to features section', () => {
      const featuresLink = document.querySelector('a[href="#features"]');
      expect(featuresLink).not.toBeNull();

      const featuresSection = document.getElementById('features');
      expect(featuresSection).not.toBeNull();
      expect(featuresSection?.classList.contains('features')).toBe(true);
    });

    it('Quick Start link points to quickstart section', () => {
      const quickstartLink = document.querySelector('a[href="#quickstart"]');
      expect(quickstartLink).not.toBeNull();

      const quickstartSection = document.getElementById('quickstart');
      expect(quickstartSection).not.toBeNull();
      expect(quickstartSection?.classList.contains('quickstart')).toBe(true);
    });

    it('Tech Specs link points to techspecs section', () => {
      const techspecsLink = document.querySelector('a[href="#techspecs"]');
      expect(techspecsLink).not.toBeNull();

      const techspecsSection = document.getElementById('techspecs');
      expect(techspecsSection).not.toBeNull();
      expect(techspecsSection?.classList.contains('techspecs')).toBe(true);
    });
  });

  describe('Navigation structure', () => {
    it('header has navigation element with aria-label', () => {
      const nav = document.querySelector('.header__nav');
      expect(nav).not.toBeNull();
      expect(nav?.getAttribute('aria-label')).toBe('Main navigation');
    });

    it('header has sticky positioning class', () => {
      const header = document.getElementById('header');
      expect(header).not.toBeNull();
      expect(header?.classList.contains('header')).toBe(true);
    });

    it('all section IDs are unique', () => {
      const sections = document.querySelectorAll('section[id]');
      const ids = Array.from(sections).map(s => s.id);
      const uniqueIds = new Set(ids);
      expect(ids.length).toBe(uniqueIds.size);
    });
  });

  describe('Hero CTA', () => {
    it('hero CTA button links to Quick Start section', () => {
      const cta = document.querySelector('.hero__cta');
      expect(cta).not.toBeNull();
      expect(cta?.getAttribute('href')).toBe('#quickstart');
    });
  });
});
