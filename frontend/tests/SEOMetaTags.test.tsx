import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../src/pages/Home'

/**
 * SEO and Meta Tags Tests
 *
 * This test suite verifies:
 * - Proper page title with relevant keywords
 * - Meta description tag for search engine visibility
 * - Semantic HTML structure (main, section, footer)
 */

const renderHomepage = () => {
  return render(
    <BrowserRouter>
      <Home />
    </BrowserRouter>
  )
}

describe('SEO and Meta Tags', () => {
  // Test Case 1: Page title contains relevant keywords
  describe('Test Case 1: Page Title', () => {
    it('should have page title containing relevant keywords like URL Shortener or Link Shortening', () => {
      // The title is set in index.html, so we check document.title
      // Note: In a real browser environment, the title would be set
      // For testing purposes, we verify the title element exists and contains relevant keywords

      // Check that document.title contains relevant keywords
      const title = document.title.toLowerCase()
      const hasRelevantKeywords =
        title.includes('url shortener') ||
        title.includes('link shortening') ||
        title.includes('shorten') ||
        title.includes('url') ||
        title.includes('link')

      expect(hasRelevantKeywords).toBe(true)
    })

    it('should have a non-empty page title', () => {
      expect(document.title).toBeTruthy()
      expect(document.title.length).toBeGreaterThan(0)
    })

    it('should have a descriptive title under 60 characters for SEO best practices', () => {
      expect(document.title.length).toBeLessThanOrEqual(60)
    })
  })

  // Test Case 2: Semantic HTML elements
  describe('Test Case 2: Semantic HTML Elements', () => {
    it('should use semantic <main> element for primary content', () => {
      renderHomepage()

      const mainElement = screen.getByRole('main')
      expect(mainElement).toBeInTheDocument()
    })

    it('should use semantic <section> elements for content sections', () => {
      renderHomepage()

      // Check for section elements by their test IDs
      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')

      expect(heroSection.tagName.toLowerCase()).toBe('section')
      expect(featuresSection.tagName.toLowerCase()).toBe('section')
    })

    it('should use semantic <footer> element', () => {
      renderHomepage()

      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
      expect(footer.tagName.toLowerCase()).toBe('footer')
    })

    it('should use semantic <nav> element for navigation', () => {
      renderHomepage()

      // Check for navigation elements
      const navElements = screen.getAllByRole('navigation')
      expect(navElements.length).toBeGreaterThan(0)
    })

    it('should have main element containing hero and features sections', () => {
      renderHomepage()

      const mainElement = screen.getByRole('main')

      // Verify main contains the key sections
      const heroSection = within(mainElement).getByTestId('hero-section')
      const featuresSection = within(mainElement).getByTestId('features-section')

      expect(heroSection).toBeInTheDocument()
      expect(featuresSection).toBeInTheDocument()
    })
  })

  // Test Case 3: Meta description tag
  describe('Test Case 3: Meta Description', () => {
    let originalMetaDescription: HTMLMetaElement | null

    beforeEach(() => {
      // Store original meta description if it exists
      originalMetaDescription = document.querySelector('meta[name="description"]')
    })

    afterEach(() => {
      // Clean up: Remove any meta description we added during tests
      const metaDesc = document.querySelector('meta[name="description"]')
      if (metaDesc && metaDesc !== originalMetaDescription) {
        metaDesc.remove()
      }
    })

    it('should have a meta description tag', () => {
      const metaDescription = document.querySelector('meta[name="description"]')
      expect(metaDescription).not.toBeNull()
    })

    it('should have a non-empty meta description', () => {
      const metaDescription = document.querySelector('meta[name="description"]')
      expect(metaDescription).not.toBeNull()

      const content = metaDescription?.getAttribute('content')
      expect(content).toBeTruthy()
      expect(content!.length).toBeGreaterThan(0)
    })

    it('should have meta description with relevant service description', () => {
      const metaDescription = document.querySelector('meta[name="description"]')
      expect(metaDescription).not.toBeNull()

      const content = metaDescription?.getAttribute('content')?.toLowerCase() || ''

      // Check for relevant keywords in the description
      const hasRelevantContent =
        content.includes('url') ||
        content.includes('shorten') ||
        content.includes('link') ||
        content.includes('analytics') ||
        content.includes('track')

      expect(hasRelevantContent).toBe(true)
    })

    it('should have meta description between 50-160 characters for SEO best practices', () => {
      const metaDescription = document.querySelector('meta[name="description"]')
      expect(metaDescription).not.toBeNull()

      const content = metaDescription?.getAttribute('content') || ''
      expect(content.length).toBeGreaterThanOrEqual(50)
      expect(content.length).toBeLessThanOrEqual(160)
    })
  })

  // Additional SEO tests
  describe('Additional SEO Checks', () => {
    it('should have proper heading hierarchy with h1 as the first heading', () => {
      renderHomepage()

      const allHeadings = screen.getAllByRole('heading')
      expect(allHeadings.length).toBeGreaterThan(0)

      const firstHeading = allHeadings[0]
      expect(firstHeading.tagName.toLowerCase()).toBe('h1')
    })

    it('should have exactly one h1 element on the page', () => {
      renderHomepage()

      const h1Elements = screen.getAllByRole('heading', { level: 1 })
      expect(h1Elements).toHaveLength(1)
    })

    it('should have section headings (h2) for major content areas', () => {
      renderHomepage()

      const h2Elements = screen.getAllByRole('heading', { level: 2 })
      expect(h2Elements.length).toBeGreaterThanOrEqual(2)
    })

    it('should have document language set', () => {
      // Check that the HTML element has a lang attribute
      const htmlElement = document.documentElement
      expect(htmlElement.getAttribute('lang')).toBeTruthy()
    })
  })
})
