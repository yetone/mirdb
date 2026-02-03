/**
 * Unit tests for Navigation component.
 * Owner: Scenario 16 - Navigation and Anchor Links
 *
 * Tests:
 * - Navigation structure and elements
 * - Anchor links presence and correctness
 * - Accessibility attributes
 * - Mobile menu structure
 */

import { describe, it, expect, beforeAll } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';

describe('Navigation Component', () => {
  let navigationContent: string;

  beforeAll(() => {
    const filePath = path.join(process.cwd(), 'src/components/Navigation.astro');
    navigationContent = fs.readFileSync(filePath, 'utf-8');
  });

  describe('Structure and Elements', () => {
    it('should have a sticky header element', () => {
      expect(navigationContent).toContain('sticky');
      expect(navigationContent).toContain('top-0');
      expect(navigationContent).toContain('<header');
    });

    it('should have a nav element with proper aria-label', () => {
      expect(navigationContent).toContain('<nav');
      expect(navigationContent).toContain('aria-label="Main navigation"');
    });

    it('should have a logo/home link pointing to hero', () => {
      expect(navigationContent).toContain('href="#hero"');
      expect(navigationContent).toContain('data-testid="nav-logo"');
    });

    it('should have data-testid for navigation header', () => {
      expect(navigationContent).toContain('data-testid="navigation-header"');
    });
  });

  describe('Navigation Links', () => {
    it('should have link to Features section', () => {
      // Links are defined in navLinks array
      expect(navigationContent).toContain("href: '#features'");
      expect(navigationContent).toContain("label: 'Features'");
    });

    it('should have link to Examples section', () => {
      expect(navigationContent).toContain("href: '#examples'");
      expect(navigationContent).toContain("label: 'Examples'");
    });

    it('should have link to Architecture section', () => {
      expect(navigationContent).toContain("href: '#architecture'");
      expect(navigationContent).toContain("label: 'Architecture'");
    });

    it('should have link to Installation section', () => {
      expect(navigationContent).toContain("href: '#installation'");
      expect(navigationContent).toContain("label: 'Installation'");
    });

    it('should have GitHub link with proper attributes', () => {
      expect(navigationContent).toContain('GITHUB_URL');
      expect(navigationContent).toContain('target="_blank"');
      expect(navigationContent).toContain('rel="noopener noreferrer"');
      expect(navigationContent).toContain('data-testid="nav-github-link"');
    });

    it('should have nav-link class for styling and JavaScript targeting', () => {
      expect(navigationContent).toContain('nav-link');
    });
  });

  describe('Desktop Navigation', () => {
    it('should have desktop navigation container', () => {
      expect(navigationContent).toContain('data-testid="desktop-nav"');
    });

    it('should hide desktop nav on mobile', () => {
      expect(navigationContent).toContain('hidden md:flex');
    });

    it('should have dynamic data-testid pattern for nav links', () => {
      // data-testid is generated dynamically using template literal
      expect(navigationContent).toContain('data-testid={`nav-link-${link.label.toLowerCase()}`}');
    });
  });

  describe('Mobile Navigation', () => {
    it('should have mobile menu button', () => {
      expect(navigationContent).toContain('data-testid="mobile-menu-button"');
      expect(navigationContent).toContain('id="mobile-menu-button"');
    });

    it('should have mobile menu container', () => {
      expect(navigationContent).toContain('id="mobile-nav-menu"');
      expect(navigationContent).toContain('data-testid="mobile-nav-menu"');
    });

    it('should have proper ARIA attributes for mobile menu', () => {
      expect(navigationContent).toContain('aria-label="Toggle navigation menu"');
      expect(navigationContent).toContain('aria-expanded="false"');
      expect(navigationContent).toContain('aria-controls="mobile-nav-menu"');
    });

    it('should have dynamic data-testid pattern for mobile nav links', () => {
      // Mobile nav links also use the same dynamic pattern
      expect(navigationContent).toContain('data-testid={`mobile-nav-link-${link.label.toLowerCase()}`}');
    });

    it('should have mobile GitHub link', () => {
      expect(navigationContent).toContain('data-testid="mobile-nav-github-link"');
    });

    it('should hide mobile menu button on desktop', () => {
      expect(navigationContent).toContain('md:hidden');
    });
  });

  describe('Accessibility', () => {
    it('should have aria-label on logo link', () => {
      expect(navigationContent).toContain('aria-label="Go to top of page"');
    });

    it('should have aria-label on GitHub link', () => {
      expect(navigationContent).toContain('aria-label="View on GitHub"');
    });

    it('should have aria-hidden on decorative SVG icons', () => {
      expect(navigationContent).toContain('aria-hidden="true"');
    });

    it('should have focus-visible styles', () => {
      expect(navigationContent).toContain('focus-visible');
    });
  });

  describe('Smooth Scrolling and URL Hash', () => {
    it('should have JavaScript for URL hash update', () => {
      expect(navigationContent).toContain("history.pushState");
    });

    it('should have JavaScript for scroll to hash on page load', () => {
      expect(navigationContent).toContain('scrollToHashOnLoad');
      expect(navigationContent).toContain('window.location.hash');
    });

    it('should have scrollIntoView for smooth scrolling', () => {
      expect(navigationContent).toContain('scrollIntoView');
      expect(navigationContent).toContain("behavior: 'smooth'");
    });
  });

  describe('Keyboard Navigation', () => {
    it('should close mobile menu on Escape key', () => {
      expect(navigationContent).toContain("e.key === 'Escape'");
    });

    it('should return focus to menu button when closing with Escape', () => {
      expect(navigationContent).toContain('btn.focus()');
    });
  });

  describe('Responsive Behavior', () => {
    it('should close mobile menu on window resize to desktop', () => {
      expect(navigationContent).toContain('window.addEventListener');
      expect(navigationContent).toContain('resize');
      expect(navigationContent).toContain('window.innerWidth >= 768');
    });
  });

  describe('Styling', () => {
    it('should have backdrop blur for modern look', () => {
      expect(navigationContent).toContain('backdrop-blur');
    });

    it('should have transition colors for hover effects', () => {
      expect(navigationContent).toContain('transition-colors');
    });

    it('should use CSS custom properties for theming', () => {
      expect(navigationContent).toContain('var(--color-bg)');
      expect(navigationContent).toContain('var(--color-text)');
      expect(navigationContent).toContain('var(--color-text-secondary)');
      expect(navigationContent).toContain('var(--color-primary)');
    });
  });
});
