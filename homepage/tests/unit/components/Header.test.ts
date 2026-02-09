/**
 * Header Component Unit Tests.
 * Owner: Scenario 7 - Navigation and Header
 *
 * Test cases:
 * 1. Header contains logo
 * 2. Header contains navigation links to all sections
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { renderHeader } from '../../../src/components/Header';

describe('Header Component', () => {
  beforeEach(() => {
    document.body.innerHTML = '';
  });

  describe('Logo', () => {
    it('should contain a logo image', () => {
      const header = renderHeader();

      const logoImg = header.querySelector('.header__logo-img');
      expect(logoImg).not.toBeNull();
      expect(logoImg?.getAttribute('alt')).toBe('MirDB Logo');
    });

    it('should contain the MirDB logo text', () => {
      const header = renderHeader();

      const logoText = header.querySelector('.header__logo-text');
      expect(logoText).not.toBeNull();
      expect(logoText?.textContent).toBe('MirDB');
    });

    it('should have a logo link to home', () => {
      const header = renderHeader();

      const logoLink = header.querySelector('.header__logo') as HTMLAnchorElement;
      expect(logoLink).not.toBeNull();
      expect(logoLink.href).toContain('#');
    });
  });

  describe('Navigation Links', () => {
    it('should contain navigation element with correct role', () => {
      const header = renderHeader();

      const nav = header.querySelector('nav');
      expect(nav).not.toBeNull();
      expect(nav?.getAttribute('role')).toBe('navigation');
      expect(nav?.getAttribute('aria-label')).toBe('Main navigation');
    });

    it('should contain navigation links to all main sections', () => {
      const header = renderHeader();

      const expectedSections = ['features', 'quickstart', 'protocol', 'configuration', 'architecture'];
      const navLinks = header.querySelectorAll('.header__nav-link');

      expect(navLinks.length).toBe(expectedSections.length);

      expectedSections.forEach((section) => {
        const link = header.querySelector(`a[href="#${section}"]`);
        expect(link).not.toBeNull();
        expect(link?.getAttribute('data-section')).toBe(section);
      });
    });

    it('should have Features link pointing to #features', () => {
      const header = renderHeader();

      const featuresLink = header.querySelector('a[href="#features"]');
      expect(featuresLink).not.toBeNull();
      expect(featuresLink?.textContent).toBe('Features');
    });

    it('should have Quick Start link pointing to #quickstart', () => {
      const header = renderHeader();

      const quickStartLink = header.querySelector('a[href="#quickstart"]');
      expect(quickStartLink).not.toBeNull();
      expect(quickStartLink?.textContent).toBe('Quick Start');
    });

    it('should have Protocol link pointing to #protocol', () => {
      const header = renderHeader();

      const protocolLink = header.querySelector('a[href="#protocol"]');
      expect(protocolLink).not.toBeNull();
      expect(protocolLink?.textContent).toBe('Protocol');
    });

    it('should have Configuration link pointing to #configuration', () => {
      const header = renderHeader();

      const configLink = header.querySelector('a[href="#configuration"]');
      expect(configLink).not.toBeNull();
      expect(configLink?.textContent).toBe('Configuration');
    });

    it('should have Architecture link pointing to #architecture', () => {
      const header = renderHeader();

      const archLink = header.querySelector('a[href="#architecture"]');
      expect(archLink).not.toBeNull();
      expect(archLink?.textContent).toBe('Architecture');
    });
  });

  describe('Hamburger Menu', () => {
    it('should contain a hamburger menu button', () => {
      const header = renderHeader();

      const hamburger = header.querySelector('.header__hamburger');
      expect(hamburger).not.toBeNull();
      expect(hamburger?.getAttribute('type')).toBe('button');
      expect(hamburger?.getAttribute('aria-label')).toBe('Toggle navigation menu');
    });

    it('should have hamburger menu with aria-expanded=false initially', () => {
      const header = renderHeader();

      const hamburger = header.querySelector('.header__hamburger');
      expect(hamburger?.getAttribute('aria-expanded')).toBe('false');
    });

    it('should have three hamburger lines', () => {
      const header = renderHeader();

      const lines = header.querySelectorAll('.header__hamburger-line');
      expect(lines.length).toBe(3);
    });

    it('should toggle navigation menu when hamburger is clicked', () => {
      const header = renderHeader();
      document.body.appendChild(header);

      const hamburger = header.querySelector('.header__hamburger') as HTMLButtonElement;
      const nav = header.querySelector('.header__nav');

      expect(nav?.classList.contains('header__nav--open')).toBe(false);
      expect(hamburger.getAttribute('aria-expanded')).toBe('false');

      hamburger.click();

      expect(nav?.classList.contains('header__nav--open')).toBe(true);
      expect(hamburger.getAttribute('aria-expanded')).toBe('true');

      hamburger.click();

      expect(nav?.classList.contains('header__nav--open')).toBe(false);
      expect(hamburger.getAttribute('aria-expanded')).toBe('false');
    });

    it('should close mobile menu when a nav link is clicked', () => {
      const header = renderHeader();
      document.body.appendChild(header);

      const hamburger = header.querySelector('.header__hamburger') as HTMLButtonElement;
      const nav = header.querySelector('.header__nav');

      // Open menu
      hamburger.click();
      expect(nav?.classList.contains('header__nav--open')).toBe(true);

      // Click a nav link
      const navLink = header.querySelector('.header__nav-link') as HTMLAnchorElement;
      navLink.click();

      expect(nav?.classList.contains('header__nav--open')).toBe(false);
      expect(hamburger.getAttribute('aria-expanded')).toBe('false');
    });
  });

  describe('Accessibility', () => {
    it('should have correct role on header element', () => {
      const header = renderHeader();

      expect(header.getAttribute('role')).toBe('banner');
    });

    it('should have aria-controls on hamburger pointing to nav-list', () => {
      const header = renderHeader();

      const hamburger = header.querySelector('.header__hamburger');
      expect(hamburger?.getAttribute('aria-controls')).toBe('nav-list');
    });

    it('should have id on nav-list matching aria-controls', () => {
      const header = renderHeader();

      const navList = header.querySelector('.header__nav-list');
      expect(navList?.id).toBe('nav-list');
    });

    it('should have aria-label on logo link', () => {
      const header = renderHeader();

      const logo = header.querySelector('.header__logo');
      expect(logo?.getAttribute('aria-label')).toBe('MirDB Home');
    });
  });

  describe('Structure', () => {
    it('should return an HTMLElement with header class', () => {
      const header = renderHeader();

      expect(header).toBeInstanceOf(HTMLElement);
      expect(header.tagName.toLowerCase()).toBe('header');
      expect(header.classList.contains('header')).toBe(true);
    });

    it('should have a container element', () => {
      const header = renderHeader();

      const container = header.querySelector('.header__container');
      expect(container).not.toBeNull();
    });
  });
});
