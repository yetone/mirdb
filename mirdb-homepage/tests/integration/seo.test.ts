/**
 * SEO Integration Tests.
 * Owner: Scenario 12 - SEO and Meta Tags
 *
 * Tests:
 * - Title tag content
 * - Meta description
 * - Open Graph tags
 * - Twitter card tags
 * - Canonical URL
 */

import { describe, it, expect, beforeAll } from 'vitest'
import { readFileSync } from 'fs'
import { resolve } from 'path'
import { JSDOM } from 'jsdom'

describe('SEO and Meta Tags', () => {
  let document: Document

  beforeAll(() => {
    const htmlPath = resolve(__dirname, '../../index.html')
    const htmlContent = readFileSync(htmlPath, 'utf-8')
    const dom = new JSDOM(htmlContent)
    document = dom.window.document
  })

  describe('Title Tag', () => {
    it('should contain MirDB and be between 30-60 characters', () => {
      const title = document.querySelector('title')
      expect(title).not.toBeNull()

      const titleText = title!.textContent || ''
      expect(titleText).toContain('MirDB')
      expect(titleText.length).toBeGreaterThanOrEqual(30)
      expect(titleText.length).toBeLessThanOrEqual(60)
    })
  })

  describe('Meta Description', () => {
    it('should exist, mention key-value store, and be 120-160 characters', () => {
      const metaDescription = document.querySelector('meta[name="description"]')
      expect(metaDescription).not.toBeNull()

      const content = metaDescription!.getAttribute('content') || ''
      expect(content.toLowerCase()).toContain('key-value store')
      expect(content.length).toBeGreaterThanOrEqual(120)
      expect(content.length).toBeLessThanOrEqual(160)
    })
  })

  describe('Open Graph Tags', () => {
    it('should have og:title with MirDB', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]')
      expect(ogTitle).not.toBeNull()

      const content = ogTitle!.getAttribute('content') || ''
      expect(content).toContain('MirDB')
    })

    it('should have og:description with product description', () => {
      const ogDescription = document.querySelector('meta[property="og:description"]')
      expect(ogDescription).not.toBeNull()

      const content = ogDescription!.getAttribute('content') || ''
      expect(content.length).toBeGreaterThan(0)
      expect(content.toLowerCase()).toContain('key-value store')
    })

    it('should have og:image with valid image URL', () => {
      const ogImage = document.querySelector('meta[property="og:image"]')
      expect(ogImage).not.toBeNull()

      const content = ogImage!.getAttribute('content') || ''
      expect(content).toMatch(/^https?:\/\/.+\.(png|jpg|jpeg|gif|webp)$/i)
    })
  })

  describe('Twitter Card Tags', () => {
    it('should have twitter:card meta tag', () => {
      const twitterCard = document.querySelector('meta[name="twitter:card"]')
      expect(twitterCard).not.toBeNull()

      const content = twitterCard!.getAttribute('content') || ''
      expect(['summary', 'summary_large_image', 'app', 'player']).toContain(content)
    })
  })

  describe('Canonical Link', () => {
    it('should have canonical link with valid URL', () => {
      const canonical = document.querySelector('link[rel="canonical"]')
      expect(canonical).not.toBeNull()

      const href = canonical!.getAttribute('href') || ''
      expect(href).toMatch(/^https?:\/\/.+/)
    })
  })
})
