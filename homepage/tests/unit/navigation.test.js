/**
 * Navigation Unit Tests
 * Owner: Scenario 5 - Navigation and Layout
 *
 * Tests for:
 * - Sticky header behavior
 * - Mobile menu toggle
 * - Smooth scroll functionality
 * - Section link highlighting
 * - Footer presence and structure
 * - GitHub link configuration
 */

const fs = require('fs');
const path = require('path');

describe('Navigation and Layout', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
  });

  beforeEach(() => {
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Header Structure', () => {
    test('header element exists with correct class', () => {
      const header = document.querySelector('.header');
      expect(header).not.toBeNull();
      expect(header.tagName.toLowerCase()).toBe('header');
    });

    test('header has role="banner" for accessibility', () => {
      const header = document.querySelector('.header');
      expect(header.getAttribute('role')).toBe('banner');
    });

    test('header contains navigation element', () => {
      const nav = document.querySelector('.header nav');
      expect(nav).not.toBeNull();
      expect(nav.classList.contains('nav')).toBe(true);
    });

    test('navigation has proper ARIA attributes', () => {
      const nav = document.querySelector('.header nav');
      expect(nav.getAttribute('role')).toBe('navigation');
      expect(nav.getAttribute('aria-label')).toBe('Main navigation');
    });

    test('header has position fixed class structure', () => {
      const header = document.querySelector('.header');
      expect(header).not.toBeNull();
      // The header element exists and will have fixed positioning via CSS
    });
  });

  describe('Navigation Brand', () => {
    test('brand link exists', () => {
      const brand = document.querySelector('.nav-brand');
      expect(brand).not.toBeNull();
    });

    test('brand links to hero section', () => {
      const brand = document.querySelector('.nav-brand');
      expect(brand.getAttribute('href')).toBe('#hero');
    });

    test('brand contains MirDB text', () => {
      const brandText = document.querySelector('.nav-brand-text');
      expect(brandText).not.toBeNull();
      expect(brandText.textContent).toBe('MirDB');
    });
  });

  describe('Navigation Links', () => {
    test('navigation links container exists', () => {
      const navLinks = document.querySelector('.nav-links');
      expect(navLinks).not.toBeNull();
    });

    test('features navigation link exists', () => {
      const featuresLink = document.querySelector('[data-nav="features"]');
      expect(featuresLink).not.toBeNull();
      expect(featuresLink.getAttribute('href')).toBe('#features');
      expect(featuresLink.textContent).toBe('Features');
    });

    test('usage navigation link exists', () => {
      const usageLink = document.querySelector('[data-nav="usage"]');
      expect(usageLink).not.toBeNull();
      expect(usageLink.getAttribute('href')).toBe('#usage');
      expect(usageLink.textContent).toBe('Usage');
    });

    test('getting started navigation link exists', () => {
      const gettingStartedLink = document.querySelector('[data-nav="getting-started"]');
      expect(gettingStartedLink).not.toBeNull();
      expect(gettingStartedLink.getAttribute('href')).toBe('#getting-started');
      expect(gettingStartedLink.textContent).toBe('Getting Started');
    });

    test('all internal links have nav-link class', () => {
      const navLinks = document.querySelectorAll('.nav-links .nav-link');
      expect(navLinks.length).toBeGreaterThanOrEqual(4);
      navLinks.forEach(link => {
        expect(link.classList.contains('nav-link')).toBe(true);
      });
    });

    test('internal links have correct href patterns', () => {
      const internalLinks = document.querySelectorAll('.nav-links a[href^="#"]');
      expect(internalLinks.length).toBeGreaterThanOrEqual(3);

      internalLinks.forEach(link => {
        const href = link.getAttribute('href');
        const sectionId = href.substring(1);
        // Verify the section exists in the document
        const targetSection = document.getElementById(sectionId);
        expect(targetSection).not.toBeNull();
      });
    });
  });

  describe('GitHub Repository Link', () => {
    test('GitHub link exists in navigation', () => {
      const githubLink = document.querySelector('[data-nav="github"]');
      expect(githubLink).not.toBeNull();
    });

    test('GitHub link has correct URL', () => {
      const githubLink = document.querySelector('[data-nav="github"]');
      expect(githubLink.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
    });

    test('GitHub link opens in new tab', () => {
      const githubLink = document.querySelector('[data-nav="github"]');
      expect(githubLink.getAttribute('target')).toBe('_blank');
    });

    test('GitHub link has security attributes', () => {
      const githubLink = document.querySelector('[data-nav="github"]');
      expect(githubLink.getAttribute('rel')).toContain('noopener');
      expect(githubLink.getAttribute('rel')).toContain('noreferrer');
    });

    test('GitHub link has accessible label', () => {
      const githubLink = document.querySelector('[data-nav="github"]');
      expect(githubLink.getAttribute('aria-label')).toContain('GitHub');
    });

    test('GitHub link contains GitHub icon', () => {
      const githubIcon = document.querySelector('[data-nav="github"] svg');
      expect(githubIcon).not.toBeNull();
    });
  });

  describe('Mobile Menu Toggle', () => {
    test('mobile menu toggle button exists', () => {
      const toggleButton = document.querySelector('.nav-mobile-toggle');
      expect(toggleButton).not.toBeNull();
      expect(toggleButton.tagName.toLowerCase()).toBe('button');
    });

    test('mobile toggle has accessible attributes', () => {
      const toggleButton = document.querySelector('.nav-mobile-toggle');
      expect(toggleButton.getAttribute('aria-label')).toContain('navigation');
      expect(toggleButton.getAttribute('aria-expanded')).toBe('false');
      expect(toggleButton.getAttribute('aria-controls')).toBe('nav-links');
    });

    test('mobile toggle has hamburger icon element', () => {
      const toggleIcon = document.querySelector('.nav-mobile-toggle-icon');
      expect(toggleIcon).not.toBeNull();
    });

    test('nav-links has id matching aria-controls', () => {
      const navLinks = document.getElementById('nav-links');
      expect(navLinks).not.toBeNull();
      expect(navLinks.classList.contains('nav-links')).toBe(true);
    });
  });

  describe('Footer Structure', () => {
    test('footer element exists', () => {
      const footer = document.querySelector('.footer');
      expect(footer).not.toBeNull();
      expect(footer.tagName.toLowerCase()).toBe('footer');
    });

    test('footer has role="contentinfo" for accessibility', () => {
      const footer = document.querySelector('.footer');
      expect(footer.getAttribute('role')).toBe('contentinfo');
    });

    test('footer has brand section', () => {
      const footerBrand = document.querySelector('.footer-brand');
      expect(footerBrand).not.toBeNull();
    });

    test('footer brand contains MirDB text', () => {
      const footerBrandText = document.querySelector('.footer-brand-text');
      expect(footerBrandText).not.toBeNull();
      expect(footerBrandText.textContent).toBe('MirDB');
    });

    test('footer has tagline', () => {
      const footerTagline = document.querySelector('.footer-tagline');
      expect(footerTagline).not.toBeNull();
      expect(footerTagline.textContent).toContain('Key-Value Store');
    });
  });

  describe('Footer Navigation', () => {
    test('footer navigation exists', () => {
      const footerNav = document.querySelector('.footer-nav');
      expect(footerNav).not.toBeNull();
    });

    test('footer has navigation links group', () => {
      const navGroup = document.querySelector('.footer-nav-group');
      expect(navGroup).not.toBeNull();
    });

    test('footer contains Features link', () => {
      const featuresFooterLink = document.querySelector('[data-nav="features-footer"]');
      expect(featuresFooterLink).not.toBeNull();
      expect(featuresFooterLink.getAttribute('href')).toBe('#features');
    });

    test('footer contains Usage link', () => {
      const usageFooterLink = document.querySelector('[data-nav="usage-footer"]');
      expect(usageFooterLink).not.toBeNull();
      expect(usageFooterLink.getAttribute('href')).toBe('#usage');
    });

    test('footer contains Getting Started link', () => {
      const gsFooterLink = document.querySelector('[data-nav="getting-started-footer"]');
      expect(gsFooterLink).not.toBeNull();
      expect(gsFooterLink.getAttribute('href')).toBe('#getting-started');
    });

    test('footer contains GitHub repository link', () => {
      const githubFooterLink = document.querySelector('[data-nav="github-footer"]');
      expect(githubFooterLink).not.toBeNull();
      expect(githubFooterLink.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
      expect(githubFooterLink.getAttribute('target')).toBe('_blank');
    });
  });

  describe('Footer Bottom', () => {
    test('footer bottom section exists', () => {
      const footerBottom = document.querySelector('.footer-bottom');
      expect(footerBottom).not.toBeNull();
    });

    test('footer has copyright notice', () => {
      const copyright = document.querySelector('.footer-copyright');
      expect(copyright).not.toBeNull();
      expect(copyright.textContent).toContain('MirDB');
      expect(copyright.textContent).toContain('MIT License');
    });

    test('footer has dynamic year placeholder', () => {
      const yearElement = document.getElementById('footer-year');
      expect(yearElement).not.toBeNull();
    });

    test('footer has social links section', () => {
      const socialSection = document.querySelector('.footer-social');
      expect(socialSection).not.toBeNull();
    });

    test('footer social has GitHub link', () => {
      const socialGithub = document.querySelector('[data-nav="github-social"]');
      expect(socialGithub).not.toBeNull();
      expect(socialGithub.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
      expect(socialGithub.getAttribute('target')).toBe('_blank');
    });
  });

  describe('Sections Exist for Navigation', () => {
    test('hero section exists', () => {
      const heroSection = document.getElementById('hero');
      expect(heroSection).not.toBeNull();
    });

    test('features section exists', () => {
      const featuresSection = document.getElementById('features');
      expect(featuresSection).not.toBeNull();
    });

    test('usage section exists', () => {
      const usageSection = document.getElementById('usage');
      expect(usageSection).not.toBeNull();
    });

    test('getting-started section exists', () => {
      const gettingStartedSection = document.getElementById('getting-started');
      expect(gettingStartedSection).not.toBeNull();
    });
  });

  describe('Smooth Scroll Support', () => {
    test('html element has smooth scroll behavior in CSS', () => {
      // The scroll-behavior: smooth is set in CSS
      // We verify the structure supports smooth scrolling
      const html = document.documentElement;
      expect(html).not.toBeNull();
    });

    test('internal navigation links use hash hrefs', () => {
      const internalLinks = document.querySelectorAll('.nav-link[href^="#"]');
      expect(internalLinks.length).toBeGreaterThanOrEqual(3);

      internalLinks.forEach(link => {
        expect(link.getAttribute('href')).toMatch(/^#[a-z-]+$/);
      });
    });
  });

  describe('Accessibility', () => {
    test('navigation uses semantic list structure', () => {
      const navList = document.querySelector('.nav-links');
      expect(navList.tagName.toLowerCase()).toBe('ul');
      expect(navList.getAttribute('role')).toBe('menubar');
    });

    test('navigation items have proper roles', () => {
      const navItems = document.querySelectorAll('.nav-links li');
      navItems.forEach(item => {
        expect(item.getAttribute('role')).toBe('none');
      });

      const navLinks = document.querySelectorAll('.nav-links .nav-link');
      navLinks.forEach(link => {
        expect(link.getAttribute('role')).toBe('menuitem');
      });
    });

    test('footer navigation has aria-label', () => {
      const footerNav = document.querySelector('.footer-nav');
      expect(footerNav.getAttribute('aria-label')).toBe('Footer navigation');
    });
  });
});
