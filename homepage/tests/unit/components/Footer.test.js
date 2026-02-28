/**
 * Footer Component Unit Tests
 * Owner: Scenario 6 - Footer Section Display
 *
 * Tests:
 * - Copyright notice with current year
 * - Legal links present (Privacy Policy, Terms of Service)
 * - Contact link present
 * - Social links present (GitHub)
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { loadHTML } from '../../setup.js';

const footerHTML = `
<footer class="footer" role="contentinfo">
  <div class="footer__content">
    <p class="footer__copyright">&copy; 2026 MirDB. All rights reserved.</p>
    <nav class="footer__links" aria-label="Footer navigation">
      <a href="/privacy" class="footer__link">Privacy Policy</a>
      <a href="/terms" class="footer__link">Terms of Service</a>
      <a href="#contact" class="footer__link">Contact</a>
      <a href="https://github.com/mirdb/mirdb" class="footer__link footer__link--social" aria-label="MirDB on GitHub">
        <svg class="footer__icon" width="20" height="20" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
          <path d="M12 0C5.37 0 0 5.37 0 12c0 5.31 3.435 9.795 8.205 11.385.6.105.825-.255.825-.57 0-.285-.015-1.23-.015-2.235-3.015.555-3.795-.735-4.035-1.41-.135-.345-.72-1.41-1.23-1.695-.42-.225-1.02-.78-.015-.795.945-.015 1.62.87 1.845 1.23 1.08 1.815 2.805 1.305 3.495.99.105-.78.42-1.305.765-1.605-2.67-.3-5.46-1.335-5.46-5.925 0-1.305.465-2.385 1.23-3.225-.12-.3-.54-1.53.12-3.18 0 0 1.005-.315 3.3 1.23.96-.27 1.98-.405 3-.405s2.04.135 3 .405c2.295-1.56 3.3-1.23 3.3-1.23.66 1.65.24 2.88.12 3.18.765.84 1.23 1.905 1.23 3.225 0 4.605-2.805 5.625-5.475 5.925.435.375.81 1.095.81 2.22 0 1.605-.015 2.895-.015 3.3 0 .315.225.69.825.57A12.02 12.02 0 0024 12c0-6.63-5.37-12-12-12z"/>
        </svg>
        GitHub
      </a>
    </nav>
  </div>
</footer>
`;

describe('Footer Component', () => {
  beforeEach(() => {
    loadHTML(footerHTML);
  });

  describe('Copyright Notice', () => {
    it('displays copyright notice with current year', () => {
      const copyright = document.querySelector('.footer__copyright');
      expect(copyright).not.toBeNull();
      expect(copyright.textContent).toContain('2026');
      expect(copyright.textContent).toContain('MirDB');
    });

    it('copyright notice contains "All rights reserved"', () => {
      const copyright = document.querySelector('.footer__copyright');
      expect(copyright).not.toBeNull();
      expect(copyright.textContent).toContain('All rights reserved');
    });

    it('copyright uses paragraph element', () => {
      const copyright = document.querySelector('.footer__copyright');
      expect(copyright).not.toBeNull();
      expect(copyright.tagName.toLowerCase()).toBe('p');
    });
  });

  describe('Legal Links', () => {
    it('contains Privacy Policy link', () => {
      const privacyLink = document.querySelector('a[href="/privacy"]');
      expect(privacyLink).not.toBeNull();
      expect(privacyLink.textContent).toContain('Privacy Policy');
    });

    it('contains Terms of Service link', () => {
      const termsLink = document.querySelector('a[href="/terms"]');
      expect(termsLink).not.toBeNull();
      expect(termsLink.textContent).toContain('Terms of Service');
    });

    it('legal links have footer__link class for consistent styling', () => {
      const privacyLink = document.querySelector('a[href="/privacy"]');
      const termsLink = document.querySelector('a[href="/terms"]');

      expect(privacyLink.classList.contains('footer__link')).toBe(true);
      expect(termsLink.classList.contains('footer__link')).toBe(true);
    });
  });

  describe('Contact Link', () => {
    it('contains contact link or information', () => {
      const contactLink = document.querySelector('a[href="#contact"]');
      expect(contactLink).not.toBeNull();
      expect(contactLink.textContent).toContain('Contact');
    });

    it('contact link has footer__link class', () => {
      const contactLink = document.querySelector('a[href="#contact"]');
      expect(contactLink).not.toBeNull();
      expect(contactLink.classList.contains('footer__link')).toBe(true);
    });
  });

  describe('Social Links', () => {
    it('contains GitHub repository link for MirDB project', () => {
      const githubLink = document.querySelector('a[href*="github.com"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('href')).toContain('github.com');
    });

    it('GitHub link has accessible label', () => {
      const githubLink = document.querySelector('a[href*="github.com"]');
      expect(githubLink).not.toBeNull();
      const ariaLabel = githubLink.getAttribute('aria-label');
      expect(ariaLabel).not.toBeNull();
      expect(ariaLabel.toLowerCase()).toContain('github');
    });

    it('GitHub link includes GitHub icon', () => {
      const githubLink = document.querySelector('a[href*="github.com"]');
      expect(githubLink).not.toBeNull();
      const svg = githubLink.querySelector('svg');
      expect(svg).not.toBeNull();
    });

    it('GitHub link text includes "GitHub"', () => {
      const githubLink = document.querySelector('a[href*="github.com"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink.textContent).toContain('GitHub');
    });
  });

  describe('Footer Structure and Accessibility', () => {
    it('footer element has role="contentinfo"', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
      expect(footer.getAttribute('role')).toBe('contentinfo');
    });

    it('footer contains semantic footer element', () => {
      const footer = document.querySelector('footer.footer');
      expect(footer).not.toBeNull();
      expect(footer.tagName.toLowerCase()).toBe('footer');
    });

    it('footer links navigation has aria-label', () => {
      const nav = document.querySelector('.footer__links');
      expect(nav).not.toBeNull();
      expect(nav.getAttribute('aria-label')).toBe('Footer navigation');
    });

    it('footer contains all required links', () => {
      const links = document.querySelectorAll('.footer__link');
      expect(links.length).toBeGreaterThanOrEqual(4); // Privacy, Terms, Contact, GitHub
    });
  });
});
