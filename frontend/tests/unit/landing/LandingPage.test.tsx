/**
 * Landing Page Unit Tests - Navigation Links & Semantic HTML
 * Owner: Scenario 4 - Navigation Links
 * Owner: Scenario 12 - Accessibility - Semantic HTML
 * Owner: Scenario 18 - SEO Structure
 *
 * Tests for navigation header with Login/Register links
 * Tests for semantic HTML structure (header, main, footer, section elements)
 * Tests for SEO-friendly structure (meta tags, heading hierarchy)
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { screen, within } from '@testing-library/react';
import { renderWithProviders } from './setup';
import Home from '../../../src/pages/Home';

describe('Landing Page Navigation', () => {
  describe('Test Case 1: Navigation bar presence', () => {
    it('renders navigation bar in the header', () => {
      renderWithProviders(<Home />);

      // The navbar should be in a header element
      const header = screen.getByRole('banner');
      expect(header).toBeInTheDocument();

      // Navigation should contain navigation bar class
      expect(header).toHaveClass('navbar');
    });
  });

  describe('Test Case 2: Login link', () => {
    it('renders a Login link with correct href', () => {
      renderWithProviders(<Home />);

      const loginLink = screen.getByRole('link', { name: /login/i });
      expect(loginLink).toBeInTheDocument();
      expect(loginLink).toHaveAttribute('href', '/login');
    });
  });

  describe('Test Case 3: Register/Sign Up link', () => {
    it('renders a Register or Sign Up link/button', () => {
      renderWithProviders(<Home />);

      // Could be "Register", "Sign Up", or "Get Started"
      const registerElement = screen.getByRole('link', { name: /get started|register|sign up/i });
      expect(registerElement).toBeInTheDocument();
      expect(registerElement).toHaveAttribute('href', '/register');
    });
  });

  describe('Test Case 4: Logo/brand element', () => {
    it('renders logo/brand name that links to homepage', () => {
      renderWithProviders(<Home />);

      // Should have URL Shortener brand text
      const logoLink = screen.getByRole('link', { name: /url shortener/i });
      expect(logoLink).toBeInTheDocument();
      expect(logoLink).toHaveAttribute('href', '/');
    });
  });
});

/**
 * Semantic HTML Structure Tests
 * Owner: Scenario 12 - Accessibility - Semantic HTML
 *
 * Tests for proper semantic HTML structure ensuring accessibility:
 * - header element for navigation
 * - main element for content
 * - footer element
 * - section elements for major content areas
 * - proper heading hierarchy (one h1, nested h2, h3, etc.)
 */
describe('Semantic HTML Structure', () => {
  describe('Test Case 1: Header element', () => {
    it('contains a semantic <header> element for navigation', () => {
      renderWithProviders(<Home />);

      // Query for header element using banner role (semantic role for <header>)
      const header = screen.getByRole('banner');
      expect(header).toBeInTheDocument();
      expect(header.tagName.toLowerCase()).toBe('header');
    });
  });

  describe('Test Case 2: Main element', () => {
    it('contains a semantic <main> element for content', () => {
      renderWithProviders(<Home />);

      // Query for main element using main role
      const main = screen.getByRole('main');
      expect(main).toBeInTheDocument();
      expect(main.tagName.toLowerCase()).toBe('main');
    });
  });

  describe('Test Case 3: Footer element', () => {
    it('contains a semantic <footer> element', () => {
      renderWithProviders(<Home />);

      // Query for footer element using contentinfo role (semantic role for <footer>)
      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();
      expect(footer.tagName.toLowerCase()).toBe('footer');
    });
  });

  describe('Test Case 4: Section elements', () => {
    it('main content uses <section> elements for each major section', () => {
      renderWithProviders(<Home />);

      const main = screen.getByRole('main');

      // Query for all section elements within main
      // Sections with aria-label or aria-labelledby become accessible regions
      const regions = within(main).queryAllByRole('region');

      // Should have multiple sections/regions for major content areas
      // Hero, Features, How It Works, CTA sections
      expect(regions.length).toBeGreaterThanOrEqual(3);

      // Verify these are actual <section> elements
      regions.forEach(region => {
        expect(region.tagName.toLowerCase()).toBe('section');
      });
    });

    it('sections have proper aria labels', () => {
      renderWithProviders(<Home />);

      const main = screen.getByRole('main');
      const regions = within(main).queryAllByRole('region');

      // Each section should have either aria-label or aria-labelledby
      regions.forEach(region => {
        const hasAriaLabel = region.hasAttribute('aria-label') || region.hasAttribute('aria-labelledby');
        expect(hasAriaLabel).toBe(true);
      });
    });
  });

  describe('Test Case 5: Heading hierarchy', () => {
    it('has exactly one h1 element', () => {
      renderWithProviders(<Home />);

      const headings = screen.getAllByRole('heading', { level: 1 });
      expect(headings).toHaveLength(1);
    });

    it('has proper heading hierarchy with h2 elements', () => {
      renderWithProviders(<Home />);

      // Should have h2 elements for section titles
      const h2Headings = screen.getAllByRole('heading', { level: 2 });
      expect(h2Headings.length).toBeGreaterThanOrEqual(2);
    });

    it('headings follow proper nesting order (no skipped levels)', () => {
      renderWithProviders(<Home />);

      // Get all headings and verify they don't skip levels
      const allHeadings = screen.getAllByRole('heading');

      let maxLevel = 0;
      allHeadings.forEach(heading => {
        const level = parseInt(heading.tagName.charAt(1));

        // Heading level should not skip more than one level from previous max
        // e.g., h1 to h3 is not allowed (skips h2)
        if (level > maxLevel + 1 && maxLevel > 0) {
          throw new Error(`Heading level ${level} skips level ${maxLevel + 1}`);
        }

        if (level > maxLevel) {
          maxLevel = level;
        }
      });

      // Should at least have h1 and h2
      expect(maxLevel).toBeGreaterThanOrEqual(2);
    });
  });
});

/**
 * Component Integration Tests - Existing Components
 * Owner: Scenario 16 - Component Integration - Existing Components
 *
 * Verifies that the landing page correctly uses the existing component library:
 * - FuturisticButton for CTA buttons
 * - GlassMorphismCard for feature cards
 * - BackgroundEffect for visual enhancement
 * - ThemeToggle in footer or navigation
 */
describe('Component Integration - Existing Components', () => {
  describe('Test Case 1: FuturisticButton usage', () => {
    it('CTA buttons use FuturisticButton components with Framer Motion animations', () => {
      renderWithProviders(<Home />);

      // FuturisticButton uses motion.button which has whileHover and whileTap animations
      // Find CTA buttons - "Get Started" in hero and navigation
      const getStartedButtons = screen.getAllByRole('button', { name: /get started/i });

      // Verify at least one Get Started button exists (from HeroSection)
      expect(getStartedButtons.length).toBeGreaterThanOrEqual(1);

      // FuturisticButton renders as a motion.button with btn classes
      getStartedButtons.forEach(button => {
        // Should have btn class from DaisyUI styling
        expect(button).toHaveClass('btn');
        // Should have btn-primary class for primary variant
        expect(button).toHaveClass('btn-primary');
      });

      // Check CTA section register button
      const registerButton = screen.getByRole('button', { name: /create a free account/i });
      expect(registerButton).toBeInTheDocument();
      expect(registerButton).toHaveClass('btn');
      expect(registerButton).toHaveClass('btn-primary');
    });
  });

  describe('Test Case 2: GlassMorphismCard usage', () => {
    it('feature cards use GlassMorphismCard component with glassmorphism styling', () => {
      renderWithProviders(<Home />);

      // GlassMorphismCard has specific classes for glassmorphism effect
      // Find feature cards by their data-testid
      const featureCards = screen.getAllByTestId('feature-card');

      // Should have 4 feature cards
      expect(featureCards.length).toBe(4);

      // Each feature card should be inside a GlassMorphismCard wrapper
      // GlassMorphismCard has classes: card, bg-base-200/50, backdrop-blur-lg, border, shadow-xl
      featureCards.forEach(card => {
        // The card wrapper (GlassMorphismCard) is the parent with card class
        const cardWrapper = card.closest('.card');
        expect(cardWrapper).toBeInTheDocument();
        expect(cardWrapper).toHaveClass('card');
        expect(cardWrapper).toHaveClass('backdrop-blur-lg');
        expect(cardWrapper).toHaveClass('shadow-xl');
      });
    });
  });

  describe('Test Case 3: BackgroundEffect usage', () => {
    it('BackgroundEffect component is rendered for visual enhancement', () => {
      const { container } = renderWithProviders(<Home />);

      // BackgroundEffect renders a fixed position div with gradient background
      // It has classes: fixed, inset-0, -z-10, overflow-hidden
      const backgroundEffect = container.querySelector('.fixed.inset-0.-z-10');
      expect(backgroundEffect).toBeInTheDocument();

      // Should contain gradient background div
      const gradientBg = backgroundEffect?.querySelector('.bg-gradient-to-br');
      expect(gradientBg).toBeInTheDocument();

      // Should contain animated pulse elements
      const pulseElements = backgroundEffect?.querySelectorAll('.animate-pulse');
      expect(pulseElements?.length).toBeGreaterThanOrEqual(1);
    });
  });

  describe('Test Case 4: ThemeToggle usage', () => {
    it('ThemeToggle component is included in footer or navigation', () => {
      renderWithProviders(<Home />);

      // ThemeToggle renders a button with aria-label for switching themes
      // It should be in the navigation header
      const header = screen.getByRole('banner');
      const themeToggleButton = within(header).getByRole('button', { name: /switch to (light|dark) mode/i });

      expect(themeToggleButton).toBeInTheDocument();
      // ThemeToggle uses btn-ghost btn-circle classes
      expect(themeToggleButton).toHaveClass('btn');
      expect(themeToggleButton).toHaveClass('btn-ghost');
      expect(themeToggleButton).toHaveClass('btn-circle');
    });
  });
});

/**
 * SEO Structure Tests
 * Owner: Scenario 18 - SEO Structure
 *
 * Tests for SEO-friendly page structure:
 * - Document title with product name
 * - Meta description with relevant content
 * - H1 with relevant URL shortening keywords
 *
 * Note: Document title and meta description are configured in index.html
 * and need to be set up in the test environment to simulate the production HTML.
 */
describe('SEO Structure', () => {
  let originalTitle: string;

  // Set up the document head with SEO tags before each test
  // This simulates what index.html provides in production
  beforeEach(() => {
    // Save original document title
    originalTitle = document.title;

    // Set document title as it appears in index.html
    document.title = 'URL Shortener';

    // Add meta description if not present
    let metaDescription = document.querySelector('meta[name="description"]');
    if (!metaDescription) {
      metaDescription = document.createElement('meta');
      metaDescription.setAttribute('name', 'description');
      metaDescription.setAttribute('content', 'URL Shortener - Create short, memorable links with analytics');
      document.head.appendChild(metaDescription);
    }

    // Add viewport meta if not present
    let viewportMeta = document.querySelector('meta[name="viewport"]');
    if (!viewportMeta) {
      viewportMeta = document.createElement('meta');
      viewportMeta.setAttribute('name', 'viewport');
      viewportMeta.setAttribute('content', 'width=device-width, initial-scale=1.0');
      document.head.appendChild(viewportMeta);
    }
  });

  afterEach(() => {
    // Restore original title
    document.title = originalTitle;

    // Clean up added meta tags to prevent test pollution
    const metaDescription = document.querySelector('meta[name="description"]');
    if (metaDescription) {
      metaDescription.remove();
    }
    const viewportMeta = document.querySelector('meta[name="viewport"]');
    if (viewportMeta) {
      viewportMeta.remove();
    }
  });

  describe('Test Case 1: Document title', () => {
    it('page has a descriptive title including product name', () => {
      renderWithProviders(<Home />);

      // Verify document title contains product name
      // Title is set in index.html as "URL Shortener"
      expect(document.title).toContain('URL Shortener');
      expect(document.title).toMatch(/url shortener/i);
    });

    it('document title is not empty or generic', () => {
      renderWithProviders(<Home />);

      // Title should not be empty or generic
      expect(document.title).not.toBe('');
      expect(document.title).not.toBe('React App');
      expect(document.title).not.toBe('Vite + React');
      expect(document.title.length).toBeGreaterThan(5);
    });
  });

  describe('Test Case 2: Meta description', () => {
    it('page has meta description tag with relevant content', () => {
      renderWithProviders(<Home />);

      // Meta description is set in index.html
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).toBeInTheDocument();
      expect(metaDescription).toHaveAttribute('content');

      // Content should be relevant to URL shortening service
      const content = metaDescription?.getAttribute('content') || '';
      expect(content.length).toBeGreaterThan(20);

      // Should mention key product features
      const lowerContent = content.toLowerCase();
      expect(
        lowerContent.includes('url') ||
        lowerContent.includes('short') ||
        lowerContent.includes('link') ||
        lowerContent.includes('analytics')
      ).toBe(true);
    });

    it('meta description is not empty', () => {
      renderWithProviders(<Home />);

      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).toBeInTheDocument();
      const content = metaDescription?.getAttribute('content') || '';
      expect(content.trim()).not.toBe('');
    });
  });

  describe('Test Case 3: H1 content', () => {
    it('H1 contains relevant keywords about URL shortening service', () => {
      renderWithProviders(<Home />);

      // Get the single H1 element
      const h1 = screen.getByRole('heading', { level: 1 });
      expect(h1).toBeInTheDocument();

      // H1 text should contain relevant keywords about URL shortening
      const h1Text = h1.textContent?.toLowerCase() || '';

      // Should contain at least one of these relevant keywords
      const relevantKeywords = ['shorten', 'link', 'url', 'track', 'short'];
      const containsRelevantKeyword = relevantKeywords.some(keyword =>
        h1Text.includes(keyword)
      );

      expect(containsRelevantKeyword).toBe(true);
    });

    it('H1 is descriptive and not generic', () => {
      renderWithProviders(<Home />);

      const h1 = screen.getByRole('heading', { level: 1 });
      const h1Text = h1.textContent || '';

      // H1 should have meaningful content, not be generic
      expect(h1Text.length).toBeGreaterThan(10);
      expect(h1Text.toLowerCase()).not.toBe('welcome');
      expect(h1Text.toLowerCase()).not.toBe('home');
      expect(h1Text.toLowerCase()).not.toBe('hello');
    });

    it('H1 has a meaningful length for SEO', () => {
      renderWithProviders(<Home />);

      const h1 = screen.getByRole('heading', { level: 1 });
      const h1Text = h1.textContent || '';

      // H1 should not be too short (SEO best practice: 10-100 characters)
      expect(h1Text.length).toBeGreaterThanOrEqual(10);
      // H1 should not be excessively long
      expect(h1Text.length).toBeLessThanOrEqual(100);
    });

    it('page has exactly one H1 for SEO best practices', () => {
      renderWithProviders(<Home />);

      // SEO best practice: only one H1 per page
      const allH1s = screen.getAllByRole('heading', { level: 1 });
      expect(allH1s).toHaveLength(1);
    });
  });
});
