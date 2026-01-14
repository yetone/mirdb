import { describe, it, expect, beforeEach } from 'vitest'
import fs from 'fs'
import path from 'path'

/**
 * SEO Meta Tags Tests
 *
 * These tests verify that the homepage has proper SEO-friendly meta tags (NFR-5).
 * We directly test the index.html file to ensure meta tags are present for search
 * engines and social media platforms.
 */

describe('SEO Meta Tags', () => {
  let htmlContent: string

  beforeEach(() => {
    // Read the index.html file to test meta tags
    const indexPath = path.resolve(__dirname, '../../index.html')
    htmlContent = fs.readFileSync(indexPath, 'utf-8')
  })

  // Test Case 1: Page has a descriptive title tag with product name
  describe('Title Tag', () => {
    it('has a descriptive title tag with product name', () => {
      // Parse the HTML to find the title tag
      const titleMatch = htmlContent.match(/<title>(.*?)<\/title>/i)

      expect(titleMatch).not.toBeNull()
      expect(titleMatch![1]).toBeTruthy()

      const titleContent = titleMatch![1].toLowerCase()
      // Title should contain product-related keywords
      expect(
        titleContent.includes('url') ||
        titleContent.includes('shorten') ||
        titleContent.includes('link')
      ).toBe(true)
    })

    it('title tag is within 60 characters for optimal SEO', () => {
      const titleMatch = htmlContent.match(/<title>(.*?)<\/title>/i)
      expect(titleMatch).not.toBeNull()
      // SEO best practice: title should be under 60 characters
      expect(titleMatch![1].length).toBeLessThanOrEqual(60)
    })
  })

  // Test Case 2: Meta description exists and describes the URL shortening service
  describe('Meta Description', () => {
    it('has a meta description tag', () => {
      const descriptionMatch = htmlContent.match(
        /<meta\s+name=["']description["']\s+content=["']([^"']*)["']/i
      )

      expect(descriptionMatch).not.toBeNull()
      expect(descriptionMatch![1]).toBeTruthy()
    })

    it('meta description describes the URL shortening service', () => {
      const descriptionMatch = htmlContent.match(
        /<meta\s+name=["']description["']\s+content=["']([^"']*)["']/i
      )

      expect(descriptionMatch).not.toBeNull()
      const descContent = descriptionMatch![1].toLowerCase()

      // Description should mention URL shortening related terms
      expect(
        descContent.includes('url') ||
        descContent.includes('shorten') ||
        descContent.includes('link') ||
        descContent.includes('analytics')
      ).toBe(true)
    })

    it('meta description is between 50-160 characters for optimal SEO', () => {
      const descriptionMatch = htmlContent.match(
        /<meta\s+name=["']description["']\s+content=["']([^"']*)["']/i
      )

      expect(descriptionMatch).not.toBeNull()
      const descLength = descriptionMatch![1].length

      // SEO best practice: description should be 50-160 characters
      expect(descLength).toBeGreaterThanOrEqual(50)
      expect(descLength).toBeLessThanOrEqual(160)
    })
  })

  // Test Case 3: Open Graph meta tags are present for social sharing
  describe('Open Graph Meta Tags', () => {
    it('has og:title meta tag for social sharing', () => {
      const ogTitleMatch = htmlContent.match(
        /<meta\s+property=["']og:title["']\s+content=["']([^"']*)["']/i
      )

      expect(ogTitleMatch).not.toBeNull()
      expect(ogTitleMatch![1]).toBeTruthy()
    })

    it('has og:description meta tag for social sharing', () => {
      const ogDescMatch = htmlContent.match(
        /<meta\s+property=["']og:description["']\s+content=["']([^"']*)["']/i
      )

      expect(ogDescMatch).not.toBeNull()
      expect(ogDescMatch![1]).toBeTruthy()
    })

    it('og:title contains product-related content', () => {
      const ogTitleMatch = htmlContent.match(
        /<meta\s+property=["']og:title["']\s+content=["']([^"']*)["']/i
      )

      expect(ogTitleMatch).not.toBeNull()
      const ogTitle = ogTitleMatch![1].toLowerCase()

      expect(
        ogTitle.includes('url') ||
        ogTitle.includes('shorten') ||
        ogTitle.includes('link')
      ).toBe(true)
    })

    it('og:description describes the service', () => {
      const ogDescMatch = htmlContent.match(
        /<meta\s+property=["']og:description["']\s+content=["']([^"']*)["']/i
      )

      expect(ogDescMatch).not.toBeNull()
      const ogDesc = ogDescMatch![1].toLowerCase()

      expect(
        ogDesc.includes('url') ||
        ogDesc.includes('shorten') ||
        ogDesc.includes('link') ||
        ogDesc.includes('analytics') ||
        ogDesc.includes('service')
      ).toBe(true)
    })
  })

  // Test Case 4: Viewport meta tag is set for responsive design
  describe('Viewport Meta Tag', () => {
    it('has viewport meta tag for responsive design', () => {
      const viewportMatch = htmlContent.match(
        /<meta\s+name=["']viewport["']\s+content=["']([^"']*)["']/i
      )

      expect(viewportMatch).not.toBeNull()
      expect(viewportMatch![1]).toBeTruthy()
    })

    it('viewport meta tag includes width=device-width', () => {
      const viewportMatch = htmlContent.match(
        /<meta\s+name=["']viewport["']\s+content=["']([^"']*)["']/i
      )

      expect(viewportMatch).not.toBeNull()
      expect(viewportMatch![1]).toContain('width=device-width')
    })

    it('viewport meta tag includes initial-scale', () => {
      const viewportMatch = htmlContent.match(
        /<meta\s+name=["']viewport["']\s+content=["']([^"']*)["']/i
      )

      expect(viewportMatch).not.toBeNull()
      expect(viewportMatch![1]).toContain('initial-scale')
    })
  })

  // Additional SEO Checks
  describe('Additional SEO Requirements', () => {
    it('HTML document has lang attribute for accessibility', () => {
      const langMatch = htmlContent.match(/<html[^>]*lang=["']([^"']*)["']/i)

      expect(langMatch).not.toBeNull()
      expect(langMatch![1]).toBe('en')
    })

    it('has charset meta tag for proper encoding', () => {
      const charsetMatch = htmlContent.match(
        /<meta\s+charset=["']([^"']*)["']/i
      )

      expect(charsetMatch).not.toBeNull()
      expect(charsetMatch![1].toLowerCase()).toBe('utf-8')
    })
  })
})
