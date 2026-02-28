/**
 * Navigation Component Unit Tests
 * Owner: Scenario 2 - Navigation Menu Desktop
 *
 * Tests:
 * - Logo link present
 * - Navigation items render
 * - Links have correct href values
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('Navigation Component', () => {
  let document;

  beforeEach(() => {
    const htmlPath = resolve(process.cwd(), 'src/index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html, { url: 'http://localhost:3000' });
    document = dom.window.document;
  });

  describe('Logo/Product Name', () => {
    it('should have a logo link in navigation', () => {
      const logo = document.querySelector('.nav__logo');
      expect(logo).not.toBeNull();
    });

    it('should display product name "MirDB"', () => {
      const logo = document.querySelector('.nav__logo');
      expect(logo.textContent.trim()).toBe('MirDB');
    });

    it('should link logo back to homepage', () => {
      const logo = document.querySelector('.nav__logo');
      expect(logo.getAttribute('href')).toBe('/');
    });

    it('should have accessible label on logo', () => {
      const logo = document.querySelector('.nav__logo');
      const ariaLabel = logo.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.toLowerCase()).toContain('mirdb');
    });
  });

  describe('Navigation Menu Items', () => {
    it('should have a navigation menu', () => {
      const menu = document.querySelector('.nav__menu');
      expect(menu).not.toBeNull();
    });

    it('should contain Home link', () => {
      const homeLink = document.querySelector('.nav__link[href="#home"]');
      expect(homeLink).not.toBeNull();
      expect(homeLink.textContent.trim()).toBe('Home');
    });

    it('should contain Features link', () => {
      const featuresLink = document.querySelector('.nav__link[href="#features"]');
      expect(featuresLink).not.toBeNull();
      expect(featuresLink.textContent.trim()).toBe('Features');
    });

    it('should contain About link', () => {
      const aboutLink = document.querySelector('.nav__link[href="#about"]');
      expect(aboutLink).not.toBeNull();
      expect(aboutLink.textContent.trim()).toBe('About');
    });

    it('should contain Contact link', () => {
      const contactLink = document.querySelector('.nav__link[href="#contact"]');
      expect(contactLink).not.toBeNull();
      expect(contactLink.textContent.trim()).toBe('Contact');
    });

    it('should have exactly 4 navigation links', () => {
      const links = document.querySelectorAll('.nav__link');
      expect(links.length).toBe(4);
    });
  });

  describe('Navigation Structure', () => {
    it('should be inside a header element', () => {
      const header = document.querySelector('header');
      const nav = header?.querySelector('nav');
      expect(nav).not.toBeNull();
    });

    it('should have proper nav aria-label', () => {
      const nav = document.querySelector('nav');
      const ariaLabel = nav.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.toLowerCase()).toContain('navigation');
    });

    it('should have fixed header class', () => {
      const header = document.querySelector('.header');
      expect(header).not.toBeNull();
    });
  });

  describe('Mobile Toggle Button', () => {
    it('should have a hamburger menu toggle button', () => {
      const toggle = document.querySelector('.nav__toggle');
      expect(toggle).not.toBeNull();
    });

    it('should have aria-expanded attribute on toggle', () => {
      const toggle = document.querySelector('.nav__toggle');
      const ariaExpanded = toggle.getAttribute('aria-expanded');
      expect(ariaExpanded).toBe('false');
    });

    it('should have aria-controls pointing to nav-menu', () => {
      const toggle = document.querySelector('.nav__toggle');
      const ariaControls = toggle.getAttribute('aria-controls');
      expect(ariaControls).toBe('nav-menu');
    });

    it('should have accessible label for toggle', () => {
      const toggle = document.querySelector('.nav__toggle');
      const ariaLabel = toggle.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.toLowerCase()).toContain('menu');
    });
  });
});
