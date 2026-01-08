import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import Home from './Home';
import { ThemeProvider } from '../contexts/ThemeContext';
import fs from 'fs';
import path from 'path';

// Mock useNavigate
const mockNavigate = vi.fn();
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom');
  return {
    ...actual,
    useNavigate: () => mockNavigate,
  };
});

const renderHome = () => {
  return render(
    <ThemeProvider>
      <BrowserRouter>
        <Home />
      </BrowserRouter>
    </ThemeProvider>
  );
};

// Read index.html content for meta tag tests
const getIndexHtmlContent = (): string => {
  const indexPath = path.resolve(__dirname, '../../index.html');
  return fs.readFileSync(indexPath, 'utf-8');
};

// SEO Optimization Tests (NFR-4)
describe('Home Page - SEO Optimization (NFR-4)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  // Test Case 1: Page has a descriptive <title> tag
  describe('Test Case 1: Descriptive title tag', () => {
    it('has a descriptive <title> tag in index.html', () => {
      const htmlContent = getIndexHtmlContent();

      // Check for title tag existence
      const titleMatch = htmlContent.match(/<title>([^<]+)<\/title>/);
      expect(titleMatch).not.toBeNull();

      // Verify title is descriptive (not empty or generic)
      const titleText = titleMatch![1];
      expect(titleText.length).toBeGreaterThan(10);
      expect(titleText).toMatch(/URL Shortener|Shorten|Track/i);
    });

    it('title tag contains relevant keywords', () => {
      const htmlContent = getIndexHtmlContent();
      const titleMatch = htmlContent.match(/<title>([^<]+)<\/title>/);
      const titleText = titleMatch![1];

      // Title should contain at least one relevant keyword
      const hasRelevantKeywords =
        titleText.toLowerCase().includes('shorten') ||
        titleText.toLowerCase().includes('url') ||
        titleText.toLowerCase().includes('share') ||
        titleText.toLowerCase().includes('track');

      expect(hasRelevantKeywords).toBe(true);
    });
  });

  // Test Case 2: Meta description tag is present with relevant content
  describe('Test Case 2: Meta description tag', () => {
    it('has a meta description tag in index.html', () => {
      const htmlContent = getIndexHtmlContent();

      // Check for meta description tag
      const descriptionMatch = htmlContent.match(/<meta\s+name=["']description["']\s+content=["']([^"']+)["']/);
      expect(descriptionMatch).not.toBeNull();
    });

    it('meta description has relevant content', () => {
      const htmlContent = getIndexHtmlContent();
      const descriptionMatch = htmlContent.match(/<meta\s+name=["']description["']\s+content=["']([^"']+)["']/);
      const description = descriptionMatch![1];

      // Description should be between 50-160 characters (SEO best practice)
      expect(description.length).toBeGreaterThanOrEqual(50);
      expect(description.length).toBeLessThanOrEqual(200);

      // Should contain relevant keywords
      const hasRelevantContent =
        description.toLowerCase().includes('short') ||
        description.toLowerCase().includes('link') ||
        description.toLowerCase().includes('url') ||
        description.toLowerCase().includes('track') ||
        description.toLowerCase().includes('analytics');

      expect(hasRelevantContent).toBe(true);
    });
  });

  // Test Case 3: Page uses semantic HTML elements (header, main, footer, section)
  describe('Test Case 3: Semantic HTML elements', () => {
    it('renders with a header element', () => {
      renderHome();

      const header = document.querySelector('header');
      expect(header).toBeInTheDocument();
    });

    it('renders with a main element', () => {
      renderHome();

      const main = document.querySelector('main');
      expect(main).toBeInTheDocument();
    });

    it('renders with a footer element', () => {
      renderHome();

      const footer = document.querySelector('footer');
      expect(footer).toBeInTheDocument();
    });

    it('renders with section elements', () => {
      renderHome();

      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThanOrEqual(1);
    });

    it('has proper heading hierarchy (h1 followed by h2)', () => {
      renderHome();

      // Should have exactly one h1
      const h1Elements = screen.getAllByRole('heading', { level: 1 });
      expect(h1Elements.length).toBe(1);

      // Should have multiple h2 elements for sections
      const h2Elements = screen.getAllByRole('heading', { level: 2 });
      expect(h2Elements.length).toBeGreaterThanOrEqual(1);
    });
  });

  // Test Case 4: OG tags present for social media sharing
  describe('Test Case 4: Open Graph tags', () => {
    it('has og:title meta tag', () => {
      const htmlContent = getIndexHtmlContent();

      const ogTitleMatch = htmlContent.match(/<meta\s+property=["']og:title["']\s+content=["']([^"']+)["']/);
      expect(ogTitleMatch).not.toBeNull();
      expect(ogTitleMatch![1].length).toBeGreaterThan(0);
    });

    it('has og:description meta tag', () => {
      const htmlContent = getIndexHtmlContent();

      const ogDescMatch = htmlContent.match(/<meta\s+property=["']og:description["']\s+content=["']([^"']+)["']/);
      expect(ogDescMatch).not.toBeNull();
      expect(ogDescMatch![1].length).toBeGreaterThan(0);
    });

    it('has og:type meta tag', () => {
      const htmlContent = getIndexHtmlContent();

      const ogTypeMatch = htmlContent.match(/<meta\s+property=["']og:type["']\s+content=["']([^"']+)["']/);
      expect(ogTypeMatch).not.toBeNull();
    });

    it('has og:url meta tag', () => {
      const htmlContent = getIndexHtmlContent();

      const ogUrlMatch = htmlContent.match(/<meta\s+property=["']og:url["']\s+content=["']([^"']+)["']/);
      expect(ogUrlMatch).not.toBeNull();
    });
  });

  // Additional SEO tests
  describe('Additional SEO requirements', () => {
    it('has lang attribute on html element', () => {
      const htmlContent = getIndexHtmlContent();

      const langMatch = htmlContent.match(/<html[^>]+lang=["']([^"']+)["']/);
      expect(langMatch).not.toBeNull();
      expect(langMatch![1]).toBe('en');
    });

    it('has charset meta tag', () => {
      const htmlContent = getIndexHtmlContent();

      const charsetMatch = htmlContent.match(/<meta\s+charset=["']([^"']+)["']/);
      expect(charsetMatch).not.toBeNull();
      expect(charsetMatch![1].toLowerCase()).toBe('utf-8');
    });

    it('has viewport meta tag for responsive design', () => {
      const htmlContent = getIndexHtmlContent();

      const viewportMatch = htmlContent.match(/<meta\s+name=["']viewport["']/);
      expect(viewportMatch).not.toBeNull();
    });
  });
});
