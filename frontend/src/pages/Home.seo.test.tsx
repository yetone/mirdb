/**
 * SEO and Meta Tags Unit Tests
 * Owner: Scenario 10 - SEO and Meta Tags
 *
 * Tests for SEO optimization including meta tags, semantic HTML,
 * and proper heading structure (NFR-5).
 */
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import fs from 'fs'
import path from 'path'
import { Home } from './Home'
import { ThemeProvider } from '../contexts/ThemeContext'

// Mock localStorage
const localStorageMock = (() => {
  let store: Record<string, string> = {}
  return {
    getItem: vi.fn((key: string) => store[key] || null),
    setItem: vi.fn((key: string, value: string) => {
      store[key] = value
    }),
    removeItem: vi.fn((key: string) => {
      delete store[key]
    }),
    clear: vi.fn(() => {
      store = {}
    }),
  }
})()

Object.defineProperty(window, 'localStorage', {
  value: localStorageMock,
})

// Helper to wrap component with router and theme provider
const renderWithRouter = (ui: React.ReactElement) => {
  return render(
    <ThemeProvider defaultTheme="dark">
      <BrowserRouter>{ui}</BrowserRouter>
    </ThemeProvider>
  )
}

// Read and parse index.html content
const getIndexHtmlContent = (): string => {
  const indexPath = path.resolve(__dirname, '../../index.html')
  return fs.readFileSync(indexPath, 'utf-8')
}

describe('SEO and Meta Tags', () => {
  beforeEach(() => {
    localStorageMock.clear()
    vi.clearAllMocks()
  })

  describe('Test Case 1: Document Title', () => {
    it('should have a descriptive title containing "URL Shortener" or similar', () => {
      const htmlContent = getIndexHtmlContent()
      const titleMatch = htmlContent.match(/<title>([^<]+)<\/title>/)

      expect(titleMatch).not.toBeNull()
      const title = titleMatch![1]

      // Check that title contains URL Shortener or similar keywords
      const hasUrlShortenerKeyword =
        title.toLowerCase().includes('url shortener') ||
        title.toLowerCase().includes('shorten') ||
        title.toLowerCase().includes('short link')

      expect(hasUrlShortenerKeyword).toBe(true)
      expect(title.length).toBeGreaterThan(10)
      expect(title.length).toBeLessThanOrEqual(70)
    })
  })

  describe('Test Case 2: Meta Description', () => {
    it('should have a meta description between 50-160 characters describing the URL shortening service', () => {
      const htmlContent = getIndexHtmlContent()
      const descriptionMatch = htmlContent.match(
        /<meta\s+name="description"\s+content="([^"]+)"/i
      )

      expect(descriptionMatch).not.toBeNull()
      const description = descriptionMatch![1]

      // Check length (50-160 chars as per SEO best practices)
      expect(description.length).toBeGreaterThanOrEqual(50)
      expect(description.length).toBeLessThanOrEqual(160)

      // Check that it describes URL shortening service
      const descriptionLower = description.toLowerCase()
      const describesService =
        descriptionLower.includes('url') ||
        descriptionLower.includes('short') ||
        descriptionLower.includes('link') ||
        descriptionLower.includes('analytics')

      expect(describesService).toBe(true)
    })
  })

  describe('Test Case 3: Open Graph Title Tag', () => {
    it('should have og:title meta tag with appropriate content', () => {
      const htmlContent = getIndexHtmlContent()
      const ogTitleMatch = htmlContent.match(
        /<meta\s+property="og:title"\s+content="([^"]+)"/i
      )

      expect(ogTitleMatch).not.toBeNull()
      const ogTitle = ogTitleMatch![1]

      // Check that og:title has meaningful content
      expect(ogTitle.length).toBeGreaterThan(5)
      expect(ogTitle.toLowerCase()).toContain('url')
    })
  })

  describe('Test Case 4: Open Graph Description Tag', () => {
    it('should have og:description meta tag with appropriate content', () => {
      const htmlContent = getIndexHtmlContent()
      const ogDescriptionMatch = htmlContent.match(
        /<meta\s+property="og:description"\s+content="([^"]+)"/i
      )

      expect(ogDescriptionMatch).not.toBeNull()
      const ogDescription = ogDescriptionMatch![1]

      // Check that og:description has meaningful content
      expect(ogDescription.length).toBeGreaterThan(20)

      // Should describe the service
      const describesService =
        ogDescription.toLowerCase().includes('url') ||
        ogDescription.toLowerCase().includes('short') ||
        ogDescription.toLowerCase().includes('link')

      expect(describesService).toBe(true)
    })
  })

  describe('Test Case 5: Semantic HTML Structure', () => {
    it('should use <main> element appropriately', () => {
      renderWithRouter(<Home />)
      const mainElement = screen.getByRole('main')

      expect(mainElement).toBeInTheDocument()
      expect(mainElement).toHaveAttribute('aria-label', 'Homepage')
    })

    it('should use <section> elements appropriately', () => {
      renderWithRouter(<Home />)

      // Check for section elements using test IDs
      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')
      const faqSection = screen.getByTestId('faq-section')
      const ctaFooter = screen.getByTestId('cta-footer')

      expect(heroSection).toBeInTheDocument()
      expect(featuresSection).toBeInTheDocument()
      expect(faqSection).toBeInTheDocument()
      expect(ctaFooter).toBeInTheDocument()
    })

    it('should use <nav> element appropriately', () => {
      renderWithRouter(<Home />)
      const navElement = screen.getByRole('navigation')

      expect(navElement).toBeInTheDocument()
      expect(navElement).toHaveAttribute('aria-label', 'Main navigation')
    })

    it('should use <header> element appropriately', () => {
      renderWithRouter(<Home />)
      const headerElement = screen.getByTestId('site-header')

      expect(headerElement).toBeInTheDocument()
      expect(headerElement.tagName.toLowerCase()).toBe('header')
      expect(headerElement).toHaveAttribute('role', 'banner')
    })

    it('should use <footer> element appropriately', () => {
      renderWithRouter(<Home />)
      const footerElement = screen.getByTestId('site-footer')

      expect(footerElement).toBeInTheDocument()
      expect(footerElement.tagName.toLowerCase()).toBe('footer')
      expect(footerElement).toHaveAttribute('role', 'contentinfo')
    })
  })

  describe('Test Case 6: Canonical URL', () => {
    it('should have a canonical link tag pointing to homepage URL', () => {
      const htmlContent = getIndexHtmlContent()
      const canonicalMatch = htmlContent.match(/<link\s+rel="canonical"\s+href="([^"]+)"/i)

      expect(canonicalMatch).not.toBeNull()
      const canonicalUrl = canonicalMatch![1]

      // Should point to homepage (could be "/" or a full URL)
      expect(canonicalUrl).toBeDefined()
      expect(canonicalUrl.length).toBeGreaterThan(0)
    })
  })

  describe('Test Case 7: Robots Meta Tag', () => {
    it('should not have robots meta tag blocking indexing (or should be index,follow)', () => {
      const htmlContent = getIndexHtmlContent()
      const robotsMatch = htmlContent.match(/<meta\s+name="robots"\s+content="([^"]+)"/i)

      if (robotsMatch) {
        const robotsContent = robotsMatch[1].toLowerCase()
        // Should not block indexing
        expect(robotsContent).not.toContain('noindex')
        expect(robotsContent).not.toContain('nofollow')

        // Should allow indexing
        const allowsIndexing =
          robotsContent.includes('index') || robotsContent.includes('follow')
        expect(allowsIndexing).toBe(true)
      }
      // If no robots tag exists, search engines will index by default (which is fine)
    })
  })

  describe('Heading Structure', () => {
    it('should have a single H1 heading', () => {
      renderWithRouter(<Home />)
      const h1Elements = screen.getAllByRole('heading', { level: 1 })

      expect(h1Elements).toHaveLength(1)
    })

    it('should have proper heading hierarchy with H2 elements', () => {
      renderWithRouter(<Home />)
      const h2Elements = screen.getAllByRole('heading', { level: 2 })

      // Should have H2 elements for Features, FAQ, and CTA sections
      expect(h2Elements.length).toBeGreaterThanOrEqual(2)
    })

    it('should have H1 with meaningful content', () => {
      renderWithRouter(<Home />)
      const h1Element = screen.getByRole('heading', { level: 1 })

      expect(h1Element.textContent).toBeTruthy()
      expect(h1Element.textContent!.length).toBeGreaterThan(10)
    })
  })

  describe('Additional SEO Best Practices', () => {
    it('should have proper lang attribute on html element', () => {
      const htmlContent = getIndexHtmlContent()
      const langMatch = htmlContent.match(/<html[^>]+lang="([^"]+)"/i)

      expect(langMatch).not.toBeNull()
      expect(langMatch![1]).toBe('en')
    })

    it('should have viewport meta tag for mobile responsiveness', () => {
      const htmlContent = getIndexHtmlContent()
      const viewportMatch = htmlContent.match(/<meta\s+name="viewport"[^>]+>/i)

      expect(viewportMatch).not.toBeNull()
      expect(viewportMatch![0]).toContain('width=device-width')
    })

    it('should have og:type meta tag', () => {
      const htmlContent = getIndexHtmlContent()
      const ogTypeMatch = htmlContent.match(/<meta\s+property="og:type"\s+content="([^"]+)"/i)

      expect(ogTypeMatch).not.toBeNull()
      expect(ogTypeMatch![1]).toBe('website')
    })

    it('should have charset meta tag', () => {
      const htmlContent = getIndexHtmlContent()
      const charsetMatch = htmlContent.match(/<meta\s+charset="([^"]+)"/i)

      expect(charsetMatch).not.toBeNull()
      expect(charsetMatch![1].toLowerCase()).toBe('utf-8')
    })
  })
})
