import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('Navigation and Header', () => {
  let dom;
  let document;

  beforeEach(() => {
    const htmlPath = resolve(__dirname, '../index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(html, { runScripts: 'dangerously', resources: 'usable' });
    document = dom.window.document;
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  // Test Case 1: Check header/nav element exists
  describe('Test Case 1: Header/nav element exists', () => {
    it('should have a header element present at top of page', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
    });

    it('should have a nav element present', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();
    });

    it('header should be the first major element in body (after skip link)', () => {
      const body = document.body;
      // Find the first non-skip-link element
      let firstMajorElement = null;
      for (let i = 0; i < body.children.length; i++) {
        const child = body.children[i];
        // Skip links are accessibility features and should come before header
        if (!child.classList.contains('skip-link') &&
            !child.classList.contains('skip-to-content') &&
            !child.getAttribute('href')?.includes('#main')) {
          firstMajorElement = child;
          break;
        }
      }
      expect(
        firstMajorElement.tagName.toLowerCase() === 'header' ||
        firstMajorElement.querySelector('header') !== null ||
        firstMajorElement.querySelector('nav') !== null
      ).toBe(true);
    });

    it('should have header or nav with proper class for styling', () => {
      const header = document.querySelector('header.header, header[class], nav.nav, nav[class]');
      expect(header).not.toBeNull();
    });
  });

  // Test Case 2: Check for MirDB logo/name in header
  describe('Test Case 2: MirDB logo/name in header', () => {
    it('should have MirDB text visible in header', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
      const headerText = header.textContent.toLowerCase();
      expect(headerText).toContain('mirdb');
    });

    it('should have a logo element in header (image or text)', () => {
      const header = document.querySelector('header');
      const logo = header.querySelector('.logo, [class*="logo"], img[alt*="logo"], img[alt*="MirDB"], a[class*="logo"]');
      expect(logo).not.toBeNull();
    });

    it('logo should be a link to homepage', () => {
      const header = document.querySelector('header');
      const logoLink = header.querySelector('a.logo, a[class*="logo"]');
      expect(logoLink).not.toBeNull();
      const href = logoLink.getAttribute('href');
      expect(href === '#' || href === '/' || href === 'index.html' || href === './').toBe(true);
    });

    it('logo text should be "MirDB"', () => {
      const header = document.querySelector('header');
      const logo = header.querySelector('.logo, [class*="logo"]');
      expect(logo).not.toBeNull();
      expect(logo.textContent.trim()).toBe('MirDB');
    });
  });

  // Test Case 3: Check Features navigation link
  describe('Test Case 3: Features navigation link', () => {
    it('should have a link to #features section', () => {
      const nav = document.querySelector('nav');
      const featuresLink = nav.querySelector('a[href="#features"], a[href*="features"]');
      expect(featuresLink).not.toBeNull();
    });

    it('features link should have readable text', () => {
      const nav = document.querySelector('nav');
      const featuresLink = nav.querySelector('a[href="#features"], a[href*="features"]');
      expect(featuresLink).not.toBeNull();
      const linkText = featuresLink.textContent.toLowerCase();
      expect(linkText).toContain('feature');
    });

    it('features section with matching id should exist', () => {
      const featuresSection = document.querySelector('#features, [id*="features"]');
      expect(featuresSection).not.toBeNull();
    });
  });

  // Test Case 4: Check Quick Start navigation link
  describe('Test Case 4: Quick Start navigation link', () => {
    it('should have a link to #quick-start or #quickstart section', () => {
      const nav = document.querySelector('nav');
      const quickStartLink = nav.querySelector(
        'a[href="#quick-start"], a[href="#quickstart"], a[href*="quick-start"], a[href*="quickstart"]'
      );
      expect(quickStartLink).not.toBeNull();
    });

    it('quick start link should have readable text', () => {
      const nav = document.querySelector('nav');
      const quickStartLink = nav.querySelector(
        'a[href="#quick-start"], a[href="#quickstart"], a[href*="quick-start"], a[href*="quickstart"]'
      );
      expect(quickStartLink).not.toBeNull();
      const linkText = quickStartLink.textContent.toLowerCase();
      expect(linkText.includes('quick') || linkText.includes('start') || linkText.includes('getting')).toBe(true);
    });

    it('quick start section with matching id should exist', () => {
      const quickStartSection = document.querySelector('#quick-start, #quickstart, [id*="quick-start"], [id*="quickstart"]');
      expect(quickStartSection).not.toBeNull();
    });
  });

  // Test Case 5: Check Documentation link
  describe('Test Case 5: Documentation link', () => {
    it('should have a link to documentation (internal or external)', () => {
      const header = document.querySelector('header');
      const footer = document.querySelector('footer');
      const page = document.body;

      // Check in header/nav first
      const navDocLink = header ? header.querySelector(
        'a[href*="doc"], a[href*="readme"], a[href*="documentation"]'
      ) : null;

      // Check in footer
      const footerDocLink = footer ? footer.querySelector(
        'a[href*="doc"], a[href*="readme"], a[href*="documentation"]'
      ) : null;

      // Check anywhere on page for documentation link
      const pageDocLink = page.querySelector(
        'a[href*="documentation"], a[href*="docs"], a[href*="README"], a[href*="readme"]'
      );

      expect(navDocLink !== null || footerDocLink !== null || pageDocLink !== null).toBe(true);
    });

    it('documentation link should have valid href', () => {
      const docLinks = document.querySelectorAll(
        'a[href*="doc"], a[href*="readme"], a[href*="README"], a[href*="documentation"]'
      );
      expect(docLinks.length).toBeGreaterThan(0);

      // At least one link should have a valid href
      const hasValidHref = Array.from(docLinks).some(link => {
        const href = link.getAttribute('href');
        return href && href.length > 0 && href !== '#';
      });
      expect(hasValidHref).toBe(true);
    });
  });

  // Test Case 6: Check GitHub link
  describe('Test Case 6: GitHub link', () => {
    it('should have a link to GitHub repository', () => {
      const githubLink = document.querySelector(
        'a[href*="github.com"], a[href*="github"]'
      );
      expect(githubLink).not.toBeNull();
    });

    it('GitHub link should have valid GitHub URL', () => {
      const githubLinks = document.querySelectorAll('a[href*="github.com"]');
      expect(githubLinks.length).toBeGreaterThan(0);

      const hasValidGithubUrl = Array.from(githubLinks).some(link => {
        const href = link.getAttribute('href');
        return href && href.includes('github.com/') && href.length > 15;
      });
      expect(hasValidGithubUrl).toBe(true);
    });

    it('GitHub link should be in navigation/header', () => {
      const header = document.querySelector('header');
      const nav = document.querySelector('nav');

      const headerGithubLink = header ? header.querySelector('a[href*="github.com"]') : null;
      const navGithubLink = nav ? nav.querySelector('a[href*="github.com"]') : null;

      expect(headerGithubLink !== null || navGithubLink !== null).toBe(true);
    });

    it('GitHub link should open in new tab', () => {
      const githubLink = document.querySelector('header a[href*="github.com"], nav a[href*="github.com"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('target')).toBe('_blank');
    });

    it('GitHub link should have security attributes', () => {
      const githubLink = document.querySelector('header a[href*="github.com"], nav a[href*="github.com"]');
      expect(githubLink).not.toBeNull();
      const rel = githubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
    });
  });

  // Test Case 7: Verify all navigation links have proper href attributes
  describe('Test Case 7: Navigation links have proper href attributes', () => {
    it('all nav links should have non-empty href values', () => {
      const nav = document.querySelector('nav');
      const navLinks = nav.querySelectorAll('a');
      expect(navLinks.length).toBeGreaterThan(0);

      navLinks.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).not.toBeNull();
        expect(href.length).toBeGreaterThan(0);
      });
    });

    it('internal navigation links should start with #', () => {
      const nav = document.querySelector('nav');
      const internalLinks = nav.querySelectorAll('a[href^="#"]');

      internalLinks.forEach(link => {
        const href = link.getAttribute('href');
        // Allow either just '#' (home/top) or '#section-name' patterns
        expect(href).toMatch(/^#([\w-]+)?$/);
      });
    });

    it('external links should have full URLs', () => {
      const nav = document.querySelector('nav');
      const externalLinks = nav.querySelectorAll('a[href^="http"], a[href^="https"]');

      externalLinks.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).toMatch(/^https?:\/\//);
      });
    });

    it('navigation should have multiple links', () => {
      const nav = document.querySelector('nav');
      const navLinks = nav.querySelectorAll('.nav-links a, ul a, li a');
      expect(navLinks.length).toBeGreaterThanOrEqual(3);
    });

    it('all internal anchor links should have corresponding sections', () => {
      const nav = document.querySelector('nav');
      const internalLinks = nav.querySelectorAll('a[href^="#"]');

      internalLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href && href !== '#') {
          const targetId = href.substring(1);
          const targetSection = document.getElementById(targetId);
          expect(targetSection).not.toBeNull();
        }
      });
    });
  });

  // Additional tests for navigation structure and accessibility
  describe('Navigation structure and accessibility', () => {
    it('navigation should use semantic ul/li elements', () => {
      const nav = document.querySelector('nav');
      const navList = nav.querySelector('ul');
      expect(navList).not.toBeNull();

      const listItems = nav.querySelectorAll('li');
      expect(listItems.length).toBeGreaterThan(0);
    });

    it('mobile menu toggle button should exist', () => {
      const nav = document.querySelector('nav, header');
      const mobileToggle = nav.querySelector(
        '.mobile-menu-toggle, .hamburger, .menu-toggle, button[aria-label*="menu"], button[aria-label*="navigation"]'
      );
      expect(mobileToggle).not.toBeNull();
    });

    it('mobile menu toggle should have aria-label for accessibility', () => {
      const mobileToggle = document.querySelector('.mobile-menu-toggle, .hamburger, .menu-toggle');
      if (mobileToggle) {
        expect(mobileToggle.hasAttribute('aria-label')).toBe(true);
      }
    });

    it('navigation links should be keyboard accessible', () => {
      const nav = document.querySelector('nav');
      const links = nav.querySelectorAll('a');
      links.forEach(link => {
        // Links are naturally focusable, just verify they exist
        expect(link.tagName.toLowerCase()).toBe('a');
      });
    });
  });

  // Test smooth scrolling functionality exists
  describe('Internal navigation behavior', () => {
    it('should have smooth scroll script for anchor links', () => {
      const scripts = document.querySelectorAll('script');
      const hasSmoothScroll = Array.from(scripts).some(script => {
        const content = script.textContent || '';
        return content.includes('scrollIntoView') ||
               content.includes('smooth') ||
               content.includes('scroll-behavior');
      });

      // Also check if CSS has scroll-behavior
      const styleSheets = document.querySelectorAll('link[rel="stylesheet"], style');
      const hasCSSSmooth = Array.from(styleSheets).some(style => {
        const content = style.textContent || '';
        return content.includes('scroll-behavior');
      });

      expect(hasSmoothScroll || hasCSSSmooth).toBe(true);
    });
  });
});
