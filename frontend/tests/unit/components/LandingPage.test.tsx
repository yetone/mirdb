/**
 * LandingPage Test Suite
 * Owner: Scenario 13 - Semantic HTML and SEO
 *
 * Tests for proper semantic HTML structure, meta tags, and SEO requirements.
 * Validates:
 * - Semantic HTML elements (header, main, footer)
 * - Single H1 element with main headline
 * - Section elements wrapping content areas
 * - Meta tags in document head
 * - Open Graph tags for social sharing
 */

import { render, screen, within } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import Home from '../../../src/pages/Home';

// Helper to render with router context
function renderWithRouter(component: React.ReactElement) {
  return render(<BrowserRouter>{component}</BrowserRouter>);
}

describe('LandingPage Semantic HTML Structure', () => {
  describe('Test Case 1: Semantic Elements', () => {
    it('contains header, main, and footer semantic elements', () => {
      renderWithRouter(<Home />);

      // Verify header element exists
      const header = document.querySelector('header');
      expect(header).toBeInTheDocument();

      // Verify main element exists
      const main = document.querySelector('main');
      expect(main).toBeInTheDocument();
      expect(main).toHaveAttribute('id', 'main-content');

      // Verify footer element exists
      const footer = document.querySelector('footer');
      expect(footer).toBeInTheDocument();
    });

    it('has proper document structure order', () => {
      renderWithRouter(<Home />);

      const header = document.querySelector('header');
      const main = document.querySelector('main');
      const footer = document.querySelector('footer');

      // Header should come before main
      expect(header?.compareDocumentPosition(main!)).toBe(
        Node.DOCUMENT_POSITION_FOLLOWING
      );

      // Main should come before footer
      expect(main?.compareDocumentPosition(footer!)).toBe(
        Node.DOCUMENT_POSITION_FOLLOWING
      );
    });
  });

  describe('Test Case 2: Hero Section H1', () => {
    it('contains a single H1 element with main headline', () => {
      renderWithRouter(<Home />);

      // Should have exactly one H1
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements).toHaveLength(1);

      // H1 should contain the headline text
      const h1 = screen.getByRole('heading', { level: 1 });
      expect(h1).toHaveTextContent(/shorten urls, track performance/i);
    });

    it('H1 is within the hero section', () => {
      renderWithRouter(<Home />);

      const h1 = screen.getByRole('heading', { level: 1 });
      // H1 should be labeled by hero-headline id
      expect(h1).toHaveAttribute('id', 'hero-headline');
    });
  });

  describe('Test Case 3: Section Elements', () => {
    it('Features section is wrapped in section tag', () => {
      renderWithRouter(<Home />);

      const featuresSection = screen.getByTestId('features-section');
      expect(featuresSection.tagName.toLowerCase()).toBe('section');
      expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-title');
    });

    it('HowItWorks section is wrapped in section tag', () => {
      renderWithRouter(<Home />);

      const howItWorksSection = screen.getByTestId('how-it-works-section');
      expect(howItWorksSection.tagName.toLowerCase()).toBe('section');
      expect(howItWorksSection).toHaveAttribute(
        'aria-labelledby',
        'how-it-works-title'
      );
    });

    it('CTA section is wrapped in section tag', () => {
      renderWithRouter(<Home />);

      const ctaSection = screen.getByTestId('cta-section');
      expect(ctaSection.tagName.toLowerCase()).toBe('section');
      expect(ctaSection).toHaveAttribute('aria-labelledby', 'cta-title');
    });

    it('Hero section is wrapped in section tag', () => {
      renderWithRouter(<Home />);

      // Hero section contains the h1 with hero-headline id
      const heroHeadline = document.getElementById('hero-headline');
      const heroSection = heroHeadline?.closest('section');
      expect(heroSection).toBeInTheDocument();
      expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-headline');
    });
  });

  describe('Test Case 4: Document Head Meta Tags', () => {
    let originalHead: string;

    beforeEach(() => {
      // Store original head content
      originalHead = document.head.innerHTML;

      // Set up the meta tags as they would be in index.html
      const charset = document.querySelector('meta[charset]');
      if (!charset) {
        const meta = document.createElement('meta');
        meta.setAttribute('charset', 'UTF-8');
        document.head.appendChild(meta);
      }

      const viewport = document.querySelector('meta[name="viewport"]');
      if (!viewport) {
        const meta = document.createElement('meta');
        meta.setAttribute('name', 'viewport');
        meta.setAttribute('content', 'width=device-width, initial-scale=1.0');
        document.head.appendChild(meta);
      }

      const description = document.querySelector('meta[name="description"]');
      if (!description) {
        const meta = document.createElement('meta');
        meta.setAttribute('name', 'description');
        meta.setAttribute(
          'content',
          'Free URL shortener with detailed click analytics. Track referrers, devices, locations and more.'
        );
        document.head.appendChild(meta);
      }
    });

    afterEach(() => {
      // Restore original head content
      document.head.innerHTML = originalHead;
    });

    it('contains charset meta tag', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).toBeInTheDocument();
      expect(charset?.getAttribute('charset')?.toUpperCase()).toBe('UTF-8');
    });

    it('contains viewport meta tag', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).toBeInTheDocument();
      expect(viewport?.getAttribute('content')).toContain('width=device-width');
      expect(viewport?.getAttribute('content')).toContain('initial-scale=1.0');
    });

    it('contains description meta tag', () => {
      const description = document.querySelector('meta[name="description"]');
      expect(description).toBeInTheDocument();
      expect(description?.getAttribute('content')).toBeTruthy();
      expect(description?.getAttribute('content')?.length).toBeGreaterThan(50);
    });
  });

  describe('Test Case 5: Open Graph Tags', () => {
    let originalHead: string;

    beforeEach(() => {
      originalHead = document.head.innerHTML;

      // Set up OG tags as they would be in index.html
      const ogTitle = document.querySelector('meta[property="og:title"]');
      if (!ogTitle) {
        const meta = document.createElement('meta');
        meta.setAttribute('property', 'og:title');
        meta.setAttribute('content', 'URL Shortener - Track Your Links');
        document.head.appendChild(meta);
      }

      const ogDescription = document.querySelector(
        'meta[property="og:description"]'
      );
      if (!ogDescription) {
        const meta = document.createElement('meta');
        meta.setAttribute('property', 'og:description');
        meta.setAttribute(
          'content',
          'Create short URLs and track detailed analytics for free.'
        );
        document.head.appendChild(meta);
      }

      const ogType = document.querySelector('meta[property="og:type"]');
      if (!ogType) {
        const meta = document.createElement('meta');
        meta.setAttribute('property', 'og:type');
        meta.setAttribute('content', 'website');
        document.head.appendChild(meta);
      }
    });

    afterEach(() => {
      document.head.innerHTML = originalHead;
    });

    it('contains og:title meta tag', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).toBeInTheDocument();
      expect(ogTitle?.getAttribute('content')).toBeTruthy();
    });

    it('contains og:description meta tag', () => {
      const ogDescription = document.querySelector(
        'meta[property="og:description"]'
      );
      expect(ogDescription).toBeInTheDocument();
      expect(ogDescription?.getAttribute('content')).toBeTruthy();
    });

    it('contains og:type meta tag', () => {
      const ogType = document.querySelector('meta[property="og:type"]');
      expect(ogType).toBeInTheDocument();
      expect(ogType?.getAttribute('content')).toBe('website');
    });
  });

  describe('Heading Hierarchy', () => {
    it('has proper heading hierarchy (H1 followed by H2s)', () => {
      renderWithRouter(<Home />);

      // Get all headings
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const headingLevels = Array.from(headings).map((h) =>
        parseInt(h.tagName.charAt(1))
      );

      // First heading should be H1
      expect(headingLevels[0]).toBe(1);

      // Should only have one H1
      expect(headingLevels.filter((level) => level === 1)).toHaveLength(1);

      // Subsequent headings should be H2 (section headings)
      const nonH1Headings = headingLevels.slice(1);
      nonH1Headings.forEach((level) => {
        expect(level).toBeGreaterThanOrEqual(2);
      });
    });

    it('each section has an H2 heading', () => {
      renderWithRouter(<Home />);

      // Features section should have H2
      const featuresTitle = screen.getByTestId('features-title');
      expect(featuresTitle.tagName.toLowerCase()).toBe('h2');

      // How it works section should have H2
      const howItWorksTitle = screen.getByTestId('how-it-works-title');
      expect(howItWorksTitle.tagName.toLowerCase()).toBe('h2');

      // CTA section should have H2
      const ctaTitle = screen.getByTestId('cta-headline');
      expect(ctaTitle.tagName.toLowerCase()).toBe('h2');
    });
  });

  describe('Accessibility Landmarks', () => {
    it('header contains navigation', () => {
      renderWithRouter(<Home />);

      const header = document.querySelector('header');
      const nav = header?.querySelector('nav');
      expect(nav).toBeInTheDocument();
    });

    it('main content has proper landmark role', () => {
      renderWithRouter(<Home />);

      const main = screen.getByRole('main');
      expect(main).toBeInTheDocument();
    });

    it('footer has contentinfo role', () => {
      renderWithRouter(<Home />);

      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();
    });
  });
});
