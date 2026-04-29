import { describe, it, expect, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { HelmetProvider } from 'react-helmet-async';
import { BrowserRouter } from 'react-router-dom';
import Home, { SEO_CONFIG, STRUCTURED_DATA } from '../../src/pages/Home';

const renderWithProviders = (component: React.ReactNode) => {
  const helmetContext = {};
  return render(
    <HelmetProvider context={helmetContext}>
      <BrowserRouter>
        {component}
      </BrowserRouter>
    </HelmetProvider>
  );
};

describe('Home Page SEO', () => {
  beforeEach(() => {
    document.head.innerHTML = '';
  });

  describe('Test Case 1: Page Title Tag', () => {
    it('should have a title tag present', async () => {
      renderWithProviders(<Home />);

      await waitFor(() => {
        const title = document.querySelector('title');
        expect(title).not.toBeNull();
      });
    });

    it('should have a descriptive title', async () => {
      renderWithProviders(<Home />);

      await waitFor(() => {
        const title = document.querySelector('title');
        expect(title?.textContent).toContain('URL Shortener');
      });
    });

    it('should have a title under 60 characters', async () => {
      renderWithProviders(<Home />);

      expect(SEO_CONFIG.title.length).toBeLessThanOrEqual(60);
    });

    it('should include primary keywords in title', async () => {
      expect(SEO_CONFIG.title.toLowerCase()).toContain('shorten');
      expect(SEO_CONFIG.title.toLowerCase()).toContain('link');
    });
  });

  describe('Test Case 2: Meta Description', () => {
    it('should have a meta description tag present', async () => {
      renderWithProviders(<Home />);

      await waitFor(() => {
        const metaDesc = document.querySelector('meta[name="description"]');
        expect(metaDesc).not.toBeNull();
      });
    });

    it('should have a meta description under 160 characters', async () => {
      expect(SEO_CONFIG.description.length).toBeLessThanOrEqual(160);
    });

    it('should have a descriptive meta description content', async () => {
      expect(SEO_CONFIG.description.toLowerCase()).toContain('url shortener');
    });
  });

  describe('Test Case 3: Open Graph Tags', () => {
    it('should have og:title tag present', async () => {
      renderWithProviders(<Home />);

      await waitFor(() => {
        const ogTitle = document.querySelector('meta[property="og:title"]');
        expect(ogTitle).not.toBeNull();
      });
    });

    it('should have og:description tag present', async () => {
      renderWithProviders(<Home />);

      await waitFor(() => {
        const ogDesc = document.querySelector('meta[property="og:description"]');
        expect(ogDesc).not.toBeNull();
      });
    });

    it('should have og:image tag present', async () => {
      renderWithProviders(<Home />);

      await waitFor(() => {
        const ogImage = document.querySelector('meta[property="og:image"]');
        expect(ogImage).not.toBeNull();
      });
    });

    it('should have og:url tag present', async () => {
      renderWithProviders(<Home />);

      await waitFor(() => {
        const ogUrl = document.querySelector('meta[property="og:url"]');
        expect(ogUrl).not.toBeNull();
      });
    });

    it('should have og:type tag present', async () => {
      renderWithProviders(<Home />);

      await waitFor(() => {
        const ogType = document.querySelector('meta[property="og:type"]');
        expect(ogType).not.toBeNull();
        expect(ogType?.getAttribute('content')).toBe('website');
      });
    });
  });

  describe('Test Case 4: Semantic HTML Structure', () => {
    it('should have a header element', () => {
      renderWithProviders(<Home />);

      const header = document.querySelector('header');
      expect(header).not.toBeNull();
      expect(header?.getAttribute('role')).toBe('banner');
    });

    it('should have a nav element within header', () => {
      renderWithProviders(<Home />);

      const nav = document.querySelector('header nav');
      expect(nav).not.toBeNull();
      expect(nav?.getAttribute('role')).toBe('navigation');
    });

    it('should have a main element', () => {
      renderWithProviders(<Home />);

      const main = document.querySelector('main');
      expect(main).not.toBeNull();
      expect(main?.getAttribute('role')).toBe('main');
      expect(main?.getAttribute('id')).toBe('main-content');
    });

    it('should have section elements with proper labels', () => {
      renderWithProviders(<Home />);

      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThanOrEqual(2);

      // Check that sections have aria-labelledby
      sections.forEach(section => {
        const labelledBy = section.getAttribute('aria-labelledby');
        expect(labelledBy).not.toBeNull();
      });
    });

    it('should have a footer element', () => {
      renderWithProviders(<Home />);

      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
      expect(footer?.getAttribute('role')).toBe('contentinfo');
    });

    it('should have proper heading hierarchy with h1', () => {
      renderWithProviders(<Home />);

      const h1 = screen.getByRole('heading', { level: 1 });
      expect(h1).toBeInTheDocument();
    });

    it('should have h2 headings for sections', () => {
      renderWithProviders(<Home />);

      const h2s = screen.getAllByRole('heading', { level: 2 });
      expect(h2s.length).toBeGreaterThanOrEqual(2);
    });
  });

  describe('Test Case 5: Canonical URL', () => {
    it('should have a canonical link tag', async () => {
      renderWithProviders(<Home />);

      await waitFor(() => {
        const canonical = document.querySelector('link[rel="canonical"]');
        expect(canonical).not.toBeNull();
      });
    });

    it('should point canonical to the homepage URL', async () => {
      renderWithProviders(<Home />);

      await waitFor(() => {
        const canonical = document.querySelector('link[rel="canonical"]');
        expect(canonical?.getAttribute('href')).toBe(SEO_CONFIG.canonicalUrl);
      });
    });

    it('should have a valid canonical URL format', () => {
      const urlPattern = /^https?:\/\/.+/;
      expect(SEO_CONFIG.canonicalUrl).toMatch(urlPattern);
    });
  });

  describe('Structured Data (JSON-LD)', () => {
    it('should have JSON-LD structured data', async () => {
      renderWithProviders(<Home />);

      await waitFor(() => {
        const jsonLd = document.querySelector('script[type="application/ld+json"]');
        expect(jsonLd).not.toBeNull();
      });
    });

    it('should have valid Schema.org context', () => {
      expect(STRUCTURED_DATA['@context']).toBe('https://schema.org');
    });

    it('should have WebApplication type', () => {
      expect(STRUCTURED_DATA['@type']).toBe('WebApplication');
    });

    it('should include application name', () => {
      expect(STRUCTURED_DATA.name).toBe('URL Shortener');
    });
  });

  describe('Twitter Card Meta Tags', () => {
    it('should have twitter:card meta tag', async () => {
      renderWithProviders(<Home />);

      await waitFor(() => {
        const twitterCard = document.querySelector('meta[name="twitter:card"]');
        expect(twitterCard).not.toBeNull();
        expect(twitterCard?.getAttribute('content')).toBe('summary_large_image');
      });
    });

    it('should have twitter:title meta tag', async () => {
      renderWithProviders(<Home />);

      await waitFor(() => {
        const twitterTitle = document.querySelector('meta[name="twitter:title"]');
        expect(twitterTitle).not.toBeNull();
      });
    });
  });
});
