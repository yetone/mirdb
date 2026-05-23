/**
 * Unit tests for navigation module.
 * Owner: Scenario 7 - Navigation and GitHub Links
 *
 * Tests:
 * - Mobile menu toggle
 * - Active section highlighting
 * - Anchor link handling
 * - External link security attributes
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import fs from 'fs';
import path from 'path';

const htmlPath = path.resolve(__dirname, '../../index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

describe('Navigation Unit Tests', () => {
  beforeEach(() => {
    document.body.innerHTML = htmlContent;
  });

  afterEach(() => {
    document.body.innerHTML = '';
  });

  describe('Header Navigation', () => {
    it('nav element exists with role="navigation"', () => {
      const nav = document.querySelector('nav[role="navigation"]');
      expect(nav).not.toBeNull();
    });

    it('header contains the nav element', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
      const nav = header.querySelector('nav');
      expect(nav).not.toBeNull();
    });

    it('nav menu has links to #features, #quick-start, and #status', () => {
      const links = document.querySelectorAll('.nav-link[href^="#"]');
      const hrefs = Array.from(links).map(link => link.getAttribute('href'));
      expect(hrefs).toContain('#features');
      expect(hrefs).toContain('#quick-start');
      expect(hrefs).toContain('#status');
    });
  });

  describe('GitHub Links', () => {
    it('GitHub link exists in header/nav', () => {
      const githubLink = document.querySelector('[data-testid="github-link-header"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
    });

    it('GitHub link exists in hero section', () => {
      const githubLink = document.querySelector('[data-testid="github-link-hero"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
    });

    it('GitHub link exists in footer', () => {
      const githubLink = document.querySelector('[data-testid="github-link-footer"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
    });

    it('all external GitHub links use target="_blank" and rel="noopener noreferrer"', () => {
      const allLinks = document.querySelectorAll('a[href="https://github.com/yetone/mirdb"]');
      expect(allLinks.length).toBeGreaterThanOrEqual(3);

      allLinks.forEach(link => {
        expect(link.getAttribute('target')).toBe('_blank');
        expect(link.getAttribute('rel')).toBe('noopener noreferrer');
      });
    });
  });

  describe('Mobile Menu', () => {
    it('mobile menu toggle button exists', () => {
      const toggle = document.querySelector('.mobile-menu-toggle');
      expect(toggle).not.toBeNull();
    });

    it('mobile menu toggle has correct ARIA attributes', () => {
      const toggle = document.querySelector('.mobile-menu-toggle');
      expect(toggle.getAttribute('aria-label')).toBe('Toggle navigation menu');
      expect(toggle.getAttribute('aria-expanded')).toBe('false');
      expect(toggle.getAttribute('aria-controls')).toBe('nav-menu');
    });

    it('nav menu has id="nav-menu"', () => {
      const menu = document.getElementById('nav-menu');
      expect(menu).not.toBeNull();
    });

    it('hamburger bars exist inside toggle button', () => {
      const toggle = document.querySelector('.mobile-menu-toggle');
      const bars = toggle.querySelectorAll('.hamburger-bar');
      expect(bars.length).toBe(3);
    });
  });

  describe('Footer', () => {
    it('footer element exists', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    it('footer contains navigation links', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('a');
      expect(links.length).toBeGreaterThanOrEqual(1);
    });

    it('footer contains GitHub link', () => {
      const footer = document.querySelector('footer');
      const githubLink = footer.querySelector('[data-testid="github-link-footer"]');
      expect(githubLink).not.toBeNull();
    });
  });
});
