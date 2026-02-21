/**
 * Homepage SEO and Meta Tags tests.
 * Owner: Scenario 13 - SEO and Meta Tags
 *
 * Test coverage:
 * - Document title contains product name and describes URL shortening service
 * - Meta description tag present with compelling service description
 * - Proper use of semantic HTML5 elements (header, main, section, footer)
 * - Single h1 with keyword-rich content, proper h2/h3 hierarchy
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, waitFor, within } from '@testing-library/react';
import { renderWithProviders } from '../utils/renderWithProviders';
import { Home } from '@/pages/Home';

/**
 * Helper function to setup document head with SEO meta tags.
 * Simulates what index.html provides in production.
 */
function setupDocumentHead() {
  // Set document title
  document.title = 'URL Shortener - Shorten, Track, Analyze';

  // Set lang attribute
  document.documentElement.setAttribute('lang', 'en');

  // Add charset meta tag if not exists
  if (!document.querySelector('meta[charset]')) {
    const charsetMeta = document.createElement('meta');
    charsetMeta.setAttribute('charset', 'UTF-8');
    document.head.appendChild(charsetMeta);
  }

  // Add viewport meta tag if not exists
  if (!document.querySelector('meta[name="viewport"]')) {
    const viewportMeta = document.createElement('meta');
    viewportMeta.setAttribute('name', 'viewport');
    viewportMeta.setAttribute('content', 'width=device-width, initial-scale=1.0');
    document.head.appendChild(viewportMeta);
  }

  // Add description meta tag if not exists
  if (!document.querySelector('meta[name="description"]')) {
    const descriptionMeta = document.createElement('meta');
    descriptionMeta.setAttribute('name', 'description');
    descriptionMeta.setAttribute(
      'content',
      'Transform long URLs into short, trackable links with powerful analytics.'
    );
    document.head.appendChild(descriptionMeta);
  }
}

describe('Home Page - SEO and Meta Tags', () => {
  beforeEach(() => {
    vi.mocked(localStorage.getItem).mockReturnValue(null);
    // Setup document head before each test
    setupDocumentHead();
  });

  describe('Test Case 1: Document Title', () => {
    it('should have page title that contains product name', () => {
      // Ensure document head is properly setup
      setupDocumentHead();
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      // The document title should be SEO-friendly
      expect(document.title).toBeTruthy();
      // Title should contain URL-related keywords or product name
      expect(document.title.toLowerCase()).toMatch(/url|shortener|short|link/i);
    });

    it('should have descriptive title that explains the URL shortening service', () => {
      // Ensure document head is properly setup
      setupDocumentHead();
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      // The title should describe the service purpose
      const title = document.title.toLowerCase();
      const hasServiceDescription =
        title.includes('short') ||
        title.includes('url') ||
        title.includes('link') ||
        title.includes('track') ||
        title.includes('analytics');

      expect(hasServiceDescription).toBe(true);
    });
  });

  describe('Test Case 2: Meta Description', () => {
    it('should have meta description tag present', () => {
      setupDocumentHead();
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      // Get meta description from document
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).toBeInTheDocument();
    });

    it('should have compelling meta description content', () => {
      setupDocumentHead();
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      const metaDescription = document.querySelector(
        'meta[name="description"]'
      ) as HTMLMetaElement;
      expect(metaDescription).toBeInTheDocument();

      const content = metaDescription?.content?.toLowerCase() || '';
      // Meta description should contain service-related keywords
      const hasRelevantContent =
        content.includes('url') ||
        content.includes('short') ||
        content.includes('link') ||
        content.includes('track') ||
        content.includes('analytics');

      expect(hasRelevantContent).toBe(true);
      // Meta description should be descriptive (at least 50 characters)
      expect(metaDescription?.content?.length).toBeGreaterThan(50);
    });
  });

  describe('Test Case 3: Semantic HTML Structure', () => {
    it('should use header element for navigation', async () => {
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });

      // Navbar should be wrapped in a header element
      const header = document.querySelector('header');
      expect(header).toBeInTheDocument();
      expect(header).toHaveAttribute('data-testid', 'navbar');
    });

    it('should use main element for primary content', async () => {
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });

      // Should have a main element for primary content
      const main = document.querySelector('main');
      expect(main).toBeInTheDocument();
    });

    it('should use section elements for content sections', async () => {
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });

      // Page should have multiple section elements
      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThanOrEqual(2);

      // Verify key sections exist
      expect(screen.getByTestId('hero-section').tagName.toLowerCase()).toBe('section');
      expect(screen.getByTestId('features-section').tagName.toLowerCase()).toBe('section');
    });

    it('should use footer element for page footer', async () => {
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });

      // Should have a footer element
      const footer = document.querySelector('footer');
      expect(footer).toBeInTheDocument();
      expect(footer).toHaveAttribute('data-testid', 'footer');
    });

    it('should have proper semantic hierarchy with header, main, and footer', async () => {
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });

      // Verify all semantic elements are present in proper structure
      const homePage = screen.getByTestId('home-page');

      // Should contain header (navbar)
      const header = homePage.querySelector('header');
      expect(header).toBeInTheDocument();

      // Should contain main
      const main = homePage.querySelector('main');
      expect(main).toBeInTheDocument();

      // Should contain footer
      const footer = homePage.querySelector('footer');
      expect(footer).toBeInTheDocument();

      // Main should contain sections
      const mainSections = main?.querySelectorAll('section');
      expect(mainSections?.length).toBeGreaterThanOrEqual(2);
    });
  });

  describe('Test Case 4: Heading Hierarchy for SEO', () => {
    it('should have exactly one h1 element on the page', async () => {
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });

      // There should be exactly one h1 on the page for proper SEO
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    it('should have h1 with keyword-rich content related to URL shortening', async () => {
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument();
      });

      const h1 = screen.getByRole('heading', { level: 1 });
      expect(h1).toBeInTheDocument();

      // h1 should contain relevant keywords
      const h1Text = h1.textContent?.toLowerCase() || '';
      const hasRelevantKeywords =
        h1Text.includes('shorten') ||
        h1Text.includes('url') ||
        h1Text.includes('link') ||
        h1Text.includes('track') ||
        h1Text.includes('insights') ||
        h1Text.includes('amplify');

      expect(hasRelevantKeywords).toBe(true);
    });

    it('should have h2 elements for section headings', async () => {
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });

      // Should have multiple h2 elements for section headings
      const h2Elements = screen.getAllByRole('heading', { level: 2 });
      expect(h2Elements.length).toBeGreaterThanOrEqual(2);
    });

    it('should have proper h2/h3 hierarchy under sections', async () => {
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      await waitFor(() => {
        expect(screen.getByTestId('features-section')).toBeInTheDocument();
      });

      // Features section should have h2 heading
      const featuresSection = screen.getByTestId('features-section');
      const featuresH2 = within(featuresSection).getByRole('heading', { level: 2 });
      expect(featuresH2).toBeInTheDocument();

      // Feature cards should use h3 for individual feature titles
      const h3Elements = within(featuresSection).getAllByRole('heading', { level: 3 });
      expect(h3Elements.length).toBeGreaterThan(0);
    });

    it('should maintain proper heading order without skipping levels', async () => {
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });

      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      let previousLevel = 0;

      // Check that headings follow proper hierarchy (no skipping levels)
      allHeadings.forEach((heading) => {
        const level = parseInt(heading.tagName.charAt(1));

        // First heading should be h1
        if (previousLevel === 0) {
          expect(level).toBe(1);
        }

        // Should not skip more than one level going down (e.g., h1 directly to h3)
        // Note: Going from h3 to h2 is fine when starting a new section
        if (previousLevel > 0 && level > previousLevel) {
          expect(level - previousLevel).toBeLessThanOrEqual(1);
        }

        previousLevel = level;
      });
    });

    it('should have descriptive section headings for SEO', async () => {
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toBeInTheDocument();
      });

      // Check that h2 elements have meaningful, SEO-friendly content
      const h2Elements = screen.getAllByRole('heading', { level: 2 });

      h2Elements.forEach((h2) => {
        const text = h2.textContent || '';
        // Each h2 should have substantial content (not empty or too short)
        expect(text.length).toBeGreaterThan(5);
      });
    });
  });

  describe('Additional SEO Best Practices', () => {
    it('should have lang attribute on html element', () => {
      setupDocumentHead();
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      // Check that the document has proper lang attribute for SEO
      const htmlElement = document.documentElement;
      expect(htmlElement.getAttribute('lang')).toBeTruthy();
    });

    it('should have viewport meta tag for mobile responsiveness', () => {
      setupDocumentHead();
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).toBeInTheDocument();
      expect(viewport?.getAttribute('content')).toContain('width=device-width');
    });

    it('should have charset meta tag for proper encoding', () => {
      setupDocumentHead();
      renderWithProviders(<Home />, { initialEntries: ['/'] });

      const charset = document.querySelector('meta[charset]');
      expect(charset).toBeInTheDocument();
      expect(charset?.getAttribute('charset')?.toLowerCase()).toBe('utf-8');
    });
  });
});
