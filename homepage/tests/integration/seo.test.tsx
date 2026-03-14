/**
 * Integration tests for SEO and Semantic HTML
 * Owner: Scenario 10 - SEO and Semantic HTML
 *
 * Test cases:
 * - Check for semantic header element
 * - Check for semantic main element
 * - Check for semantic footer element
 * - Check for section/article elements
 * - Verify title tag
 * - Verify meta description
 * - Verify viewport meta tag
 * - Check Open Graph tags
 * - Run SEO audit
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { render, screen } from '@testing-library/react'
import { JSDOM } from 'jsdom'
import fs from 'fs'
import path from 'path'
import React from 'react'

// Import the Homepage component
import Homepage from '../../src/pages/Homepage'

describe('SEO and Semantic HTML', () => {
  // Test HTML document
  let htmlContent: string
  let dom: JSDOM

  beforeAll(() => {
    // Read the index.html file for meta tag tests
    const htmlPath = path.resolve(__dirname, '../../index.html')
    htmlContent = fs.readFileSync(htmlPath, 'utf-8')
    dom = new JSDOM(htmlContent)
  })

  describe('Semantic HTML Structure', () => {
    it('should contain semantic header element wrapping navigation', () => {
      const { container } = render(<Homepage />)
      const header = container.querySelector('header')
      expect(header).toBeInTheDocument()
      expect(header).toHaveAttribute('role', 'banner')
      // Verify header contains navigation
      const nav = header?.querySelector('nav')
      expect(nav).toBeInTheDocument()
    })

    it('should contain semantic main element wrapping primary content', () => {
      const { container } = render(<Homepage />)
      const main = container.querySelector('main')
      expect(main).toBeInTheDocument()
      // Main should contain sections
      const sections = main?.querySelectorAll('section')
      expect(sections?.length).toBeGreaterThan(0)
    })

    it('should contain semantic footer element at bottom', () => {
      const { container } = render(<Homepage />)
      const footer = container.querySelector('footer')
      expect(footer).toBeInTheDocument()
      // Footer element has implicit role="contentinfo" in HTML5
      // Check for explicit role if present, or verify it's a semantic footer
      if (footer?.hasAttribute('role')) {
        expect(footer).toHaveAttribute('role', 'contentinfo')
      }
      // Verify footer has content (navigation, links, etc.)
      expect(footer?.children.length).toBeGreaterThan(0)
    })

    it('should use section or article elements for content sections', () => {
      const { container } = render(<Homepage />)
      const sections = container.querySelectorAll('section, article')
      expect(sections.length).toBeGreaterThanOrEqual(3)
      // Verify sections have proper structure
      sections.forEach((section) => {
        // Each section should have at least some content
        expect(section.children.length).toBeGreaterThan(0)
      })
    })
  })

  describe('Meta Tags', () => {
    it('should have a unique, descriptive title tag', () => {
      const document = dom.window.document
      const title = document.querySelector('title')
      expect(title).not.toBeNull()
      expect(title?.textContent).toBeTruthy()
      expect(title?.textContent?.length).toBeGreaterThan(10)
      expect(title?.textContent?.length).toBeLessThanOrEqual(60)
    })

    it('should have meta description under 160 characters', () => {
      const document = dom.window.document
      const metaDescription = document.querySelector('meta[name="description"]')
      expect(metaDescription).not.toBeNull()
      const content = metaDescription?.getAttribute('content')
      expect(content).toBeTruthy()
      expect(content?.length).toBeGreaterThan(50)
      expect(content?.length).toBeLessThanOrEqual(160)
    })

    it('should have viewport meta tag with width=device-width', () => {
      const document = dom.window.document
      const viewport = document.querySelector('meta[name="viewport"]')
      expect(viewport).not.toBeNull()
      const content = viewport?.getAttribute('content')
      expect(content).toContain('width=device-width')
    })
  })

  describe('Open Graph Tags', () => {
    it('should have og:title, og:description, og:image tags present', () => {
      const document = dom.window.document

      const ogTitle = document.querySelector('meta[property="og:title"]')
      expect(ogTitle).not.toBeNull()
      expect(ogTitle?.getAttribute('content')).toBeTruthy()

      const ogDescription = document.querySelector('meta[property="og:description"]')
      expect(ogDescription).not.toBeNull()
      expect(ogDescription?.getAttribute('content')).toBeTruthy()

      const ogImage = document.querySelector('meta[property="og:image"]')
      expect(ogImage).not.toBeNull()
      expect(ogImage?.getAttribute('content')).toBeTruthy()
    })
  })

  describe('SEO Audit', () => {
    it('should pass SEO checklist with score >= 90', () => {
      const document = dom.window.document
      let score = 0
      const maxScore = 100

      // Title check (10 points)
      const title = document.querySelector('title')
      if (title && title.textContent && title.textContent.length > 10 && title.textContent.length <= 60) {
        score += 10
      }

      // Meta description check (10 points)
      const metaDescription = document.querySelector('meta[name="description"]')
      if (metaDescription) {
        const content = metaDescription.getAttribute('content')
        if (content && content.length > 50 && content.length <= 160) {
          score += 10
        }
      }

      // Viewport check (10 points)
      const viewport = document.querySelector('meta[name="viewport"]')
      if (viewport && viewport.getAttribute('content')?.includes('width=device-width')) {
        score += 10
      }

      // HTML lang attribute (10 points)
      const html = document.querySelector('html')
      if (html && html.getAttribute('lang')) {
        score += 10
      }

      // Charset meta (10 points)
      const charset = document.querySelector('meta[charset]')
      if (charset) {
        score += 10
      }

      // Open Graph title (10 points)
      const ogTitle = document.querySelector('meta[property="og:title"]')
      if (ogTitle && ogTitle.getAttribute('content')) {
        score += 10
      }

      // Open Graph description (10 points)
      const ogDescription = document.querySelector('meta[property="og:description"]')
      if (ogDescription && ogDescription.getAttribute('content')) {
        score += 10
      }

      // Open Graph image (10 points)
      const ogImage = document.querySelector('meta[property="og:image"]')
      if (ogImage && ogImage.getAttribute('content')) {
        score += 10
      }

      // Canonical URL (10 points)
      const canonical = document.querySelector('link[rel="canonical"]')
      if (canonical && canonical.getAttribute('href')) {
        score += 10
      }

      // Favicon (10 points)
      const favicon = document.querySelector('link[rel="icon"], link[rel="shortcut icon"]')
      if (favicon && favicon.getAttribute('href')) {
        score += 10
      }

      expect(score).toBeGreaterThanOrEqual(90)
    })
  })
})
