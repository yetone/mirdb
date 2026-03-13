/**
 * SEO Meta Tags Unit Tests
 * Owner: Scenario 12 - SEO Meta Tags
 *
 * Tests verify proper meta tags for SEO discoverability (NFR-3)
 * - Primary meta tags (title, description)
 * - Open Graph tags for social media sharing
 * - Twitter Card tags for Twitter sharing
 * - Canonical URL for search engine indexing
 */

import { describe, it, expect, beforeAll } from 'vitest'
import { readFileSync } from 'fs'
import { resolve } from 'path'

describe('SEO Meta Tags', () => {
  let htmlContent: string

  beforeAll(() => {
    const indexPath = resolve(__dirname, '../../index.html')
    htmlContent = readFileSync(indexPath, 'utf-8')
  })

  describe('Primary Meta Tags', () => {
    it('should have a title tag containing MirDB', () => {
      // Test Case 1: Page has <title> tag with 'MirDB'
      const titleMatch = htmlContent.match(/<title>([^<]+)<\/title>/)
      expect(titleMatch).not.toBeNull()
      expect(titleMatch![1]).toContain('MirDB')
    })

    it('should have a meta description tag that is present and descriptive', () => {
      // Test Case 2: Meta description tag is present and descriptive
      const descriptionMatch = htmlContent.match(
        /<meta\s+name=["']description["']\s+content=["']([^"']+)["']/
      )
      expect(descriptionMatch).not.toBeNull()
      expect(descriptionMatch![1].length).toBeGreaterThan(50) // Descriptive means at least 50 chars
      expect(descriptionMatch![1]).toContain('key-value') // Should mention the product type
    })
  })

  describe('Open Graph Tags', () => {
    it('should have og:title meta tag present', () => {
      // Test Case 3: og:title meta tag is present
      const ogTitleMatch = htmlContent.match(
        /<meta\s+property=["']og:title["']\s+content=["']([^"']+)["']/
      )
      expect(ogTitleMatch).not.toBeNull()
      expect(ogTitleMatch![1]).toBeTruthy()
    })

    it('should have og:description meta tag present', () => {
      // Test Case 4: og:description meta tag is present
      const ogDescriptionMatch = htmlContent.match(
        /<meta\s+property=["']og:description["']\s+content=["']([^"']+)["']/
      )
      expect(ogDescriptionMatch).not.toBeNull()
      expect(ogDescriptionMatch![1]).toBeTruthy()
    })

    it('should have og:image meta tag with valid URL', () => {
      // Test Case 5: og:image meta tag is present with valid URL
      const ogImageMatch = htmlContent.match(
        /<meta\s+property=["']og:image["']\s+content=["']([^"']+)["']/
      )
      expect(ogImageMatch).not.toBeNull()
      const imageUrl = ogImageMatch![1]
      // Valid URL should either be absolute (https://) or relative starting with /
      expect(imageUrl.startsWith('/') || imageUrl.startsWith('http')).toBe(true)
    })
  })

  describe('Twitter Card Tags', () => {
    it('should have twitter:card meta tag present', () => {
      // Test Case 6: twitter:card meta tag is present
      const twitterCardMatch = htmlContent.match(
        /<meta\s+name=["']twitter:card["']\s+content=["']([^"']+)["']/
      )
      expect(twitterCardMatch).not.toBeNull()
      expect(twitterCardMatch![1]).toBeTruthy()
      // Valid Twitter card types
      const validCardTypes = ['summary', 'summary_large_image', 'app', 'player']
      expect(validCardTypes).toContain(twitterCardMatch![1])
    })
  })

  describe('Canonical URL', () => {
    it('should have canonical URL specified', () => {
      // Test Case 7: Canonical URL is specified
      const canonicalMatch = htmlContent.match(
        /<link\s+rel=["']canonical["']\s+href=["']([^"']+)["']/
      )
      expect(canonicalMatch).not.toBeNull()
      const canonicalUrl = canonicalMatch![1]
      // Canonical URL should be a valid absolute URL
      expect(canonicalUrl.startsWith('http')).toBe(true)
    })
  })
})
