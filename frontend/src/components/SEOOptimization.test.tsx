import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { JSDOM } from 'jsdom'
import * as fs from 'fs'
import * as path from 'path'

/**
 * SEO Optimization Tests (NFR-6)
 * Tests for proper meta tags, semantic HTML, and structured data
 */

const indexHtmlPath = path.join(__dirname, '../../index.html')
let dom: JSDOM
let document: Document

beforeAll(() => {
  const html = fs.readFileSync(indexHtmlPath, 'utf-8')
  dom = new JSDOM(html)
  document = dom.window.document
})

afterAll(() => {
  dom.window.close()
})

describe('SEO Optimization - NFR-6 Compliance', () => {
  // Test Case 1: Check document title
  describe('Document Title (Test Case 1)', () => {
    it('page has a title tag', () => {
      const title = document.querySelector('title')
      expect(title).not.toBeNull()
    })

    it('title tag contains brand keywords', () => {
      const title = document.querySelector('title')
      expect(title?.textContent?.toLowerCase()).toContain('url')
    })

    it('title tag contains service keywords', () => {
      const title = document.querySelector('title')
      const titleText = title?.textContent?.toLowerCase() || ''
      // Should contain relevant service keywords
      const hasServiceKeywords =
        titleText.includes('shorten') ||
        titleText.includes('share') ||
        titleText.includes('analyze') ||
        titleText.includes('link')
      expect(hasServiceKeywords).toBe(true)
    })

    it('title is descriptive with brand and service keywords', () => {
      const title = document.querySelector('title')
      const titleText = title?.textContent || ''
      // Title should be descriptive (between 30-70 characters for best SEO)
      expect(titleText.length).toBeGreaterThanOrEqual(20)
      expect(titleText.length).toBeLessThanOrEqual(70)
    })
  })

  // Test Case 2: Check meta description
  describe('Meta Description (Test Case 2)', () => {
    it('page has meta description tag', () => {
      const metaDescription = document.querySelector('meta[name="description"]')
      expect(metaDescription).not.toBeNull()
    })

    it('meta description is under 160 characters', () => {
      const metaDescription = document.querySelector('meta[name="description"]')
      const content = metaDescription?.getAttribute('content') || ''
      expect(content.length).toBeLessThanOrEqual(160)
    })

    it('meta description describes the service', () => {
      const metaDescription = document.querySelector('meta[name="description"]')
      const content = metaDescription?.getAttribute('content')?.toLowerCase() || ''
      // Should describe the URL shortening service
      const describesService =
        content.includes('url') ||
        content.includes('shorten') ||
        content.includes('link') ||
        content.includes('track')
      expect(describesService).toBe(true)
    })

    it('meta description has meaningful content (at least 50 chars)', () => {
      const metaDescription = document.querySelector('meta[name="description"]')
      const content = metaDescription?.getAttribute('content') || ''
      expect(content.length).toBeGreaterThanOrEqual(50)
    })
  })

  // Test Case 3: Check Open Graph tags
  describe('Open Graph Tags (Test Case 3)', () => {
    it('page has og:title meta tag', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]')
      expect(ogTitle).not.toBeNull()
    })

    it('og:title has meaningful content', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]')
      const content = ogTitle?.getAttribute('content') || ''
      expect(content.length).toBeGreaterThan(0)
    })

    it('page has og:description meta tag', () => {
      const ogDescription = document.querySelector('meta[property="og:description"]')
      expect(ogDescription).not.toBeNull()
    })

    it('og:description has meaningful content', () => {
      const ogDescription = document.querySelector('meta[property="og:description"]')
      const content = ogDescription?.getAttribute('content') || ''
      expect(content.length).toBeGreaterThan(0)
    })

    it('page has og:image meta tag', () => {
      const ogImage = document.querySelector('meta[property="og:image"]')
      expect(ogImage).not.toBeNull()
    })

    it('og:image has a valid URL', () => {
      const ogImage = document.querySelector('meta[property="og:image"]')
      const content = ogImage?.getAttribute('content') || ''
      // Should be a valid URL (starts with http, https, or /)
      const isValidUrl = content.startsWith('http') || content.startsWith('/')
      expect(isValidUrl).toBe(true)
    })

    it('page has og:type meta tag', () => {
      const ogType = document.querySelector('meta[property="og:type"]')
      expect(ogType).not.toBeNull()
    })

    it('page has og:url meta tag', () => {
      const ogUrl = document.querySelector('meta[property="og:url"]')
      expect(ogUrl).not.toBeNull()
    })
  })

  // Test Case 4: Check canonical URL
  describe('Canonical URL (Test Case 4)', () => {
    it('page has canonical link element', () => {
      const canonical = document.querySelector('link[rel="canonical"]')
      expect(canonical).not.toBeNull()
    })

    it('canonical link points to primary URL', () => {
      const canonical = document.querySelector('link[rel="canonical"]')
      const href = canonical?.getAttribute('href') || ''
      // Should be a valid URL
      expect(href).toMatch(/^https?:\/\//)
    })

    it('canonical URL is absolute', () => {
      const canonical = document.querySelector('link[rel="canonical"]')
      const href = canonical?.getAttribute('href') || ''
      expect(href.startsWith('http')).toBe(true)
    })
  })

  // Test Case 5: Verify semantic structure
  describe('Semantic HTML Structure (Test Case 5)', () => {
    it('HTML has proper lang attribute', () => {
      const html = document.querySelector('html')
      expect(html?.getAttribute('lang')).toBe('en')
    })

    it('document has proper charset declaration', () => {
      const charset = document.querySelector('meta[charset]')
      expect(charset?.getAttribute('charset')?.toLowerCase()).toBe('utf-8')
    })

    it('document has viewport meta tag', () => {
      const viewport = document.querySelector('meta[name="viewport"]')
      expect(viewport).not.toBeNull()
    })

    it('document has robots meta tag for indexing', () => {
      const robots = document.querySelector('meta[name="robots"]')
      expect(robots).not.toBeNull()
      const content = robots?.getAttribute('content') || ''
      expect(content).toContain('index')
    })
  })

  // Additional SEO tests - structured data
  describe('Structured Data (JSON-LD)', () => {
    it('page has JSON-LD structured data script', () => {
      const jsonLd = document.querySelector('script[type="application/ld+json"]')
      expect(jsonLd).not.toBeNull()
    })

    it('JSON-LD contains valid JSON', () => {
      const jsonLd = document.querySelector('script[type="application/ld+json"]')
      const content = jsonLd?.textContent || '{}'
      expect(() => JSON.parse(content)).not.toThrow()
    })

    it('JSON-LD has @context property', () => {
      const jsonLd = document.querySelector('script[type="application/ld+json"]')
      const content = jsonLd?.textContent || '{}'
      const data = JSON.parse(content)
      expect(data['@context']).toBe('https://schema.org')
    })

    it('JSON-LD has @type property', () => {
      const jsonLd = document.querySelector('script[type="application/ld+json"]')
      const content = jsonLd?.textContent || '{}'
      const data = JSON.parse(content)
      expect(data['@type']).toBeDefined()
    })

    it('JSON-LD contains organization or website type', () => {
      const jsonLd = document.querySelector('script[type="application/ld+json"]')
      const content = jsonLd?.textContent || '{}'
      const data = JSON.parse(content)
      const validTypes = ['Organization', 'WebSite', 'WebApplication', 'SoftwareApplication']
      expect(validTypes).toContain(data['@type'])
    })

    it('JSON-LD has name property', () => {
      const jsonLd = document.querySelector('script[type="application/ld+json"]')
      const content = jsonLd?.textContent || '{}'
      const data = JSON.parse(content)
      expect(data.name).toBeDefined()
      expect(data.name.length).toBeGreaterThan(0)
    })

    it('JSON-LD has url property', () => {
      const jsonLd = document.querySelector('script[type="application/ld+json"]')
      const content = jsonLd?.textContent || '{}'
      const data = JSON.parse(content)
      expect(data.url).toBeDefined()
      expect(data.url).toMatch(/^https?:\/\//)
    })

    it('JSON-LD has description property', () => {
      const jsonLd = document.querySelector('script[type="application/ld+json"]')
      const content = jsonLd?.textContent || '{}'
      const data = JSON.parse(content)
      expect(data.description).toBeDefined()
      expect(data.description.length).toBeGreaterThan(0)
    })
  })

  // Twitter Card meta tags
  describe('Twitter Card Tags', () => {
    it('page has twitter:card meta tag', () => {
      const twitterCard = document.querySelector('meta[name="twitter:card"]')
      expect(twitterCard).not.toBeNull()
    })

    it('twitter:card has valid value', () => {
      const twitterCard = document.querySelector('meta[name="twitter:card"]')
      const content = twitterCard?.getAttribute('content') || ''
      const validValues = ['summary', 'summary_large_image', 'app', 'player']
      expect(validValues).toContain(content)
    })

    it('page has twitter:title meta tag', () => {
      const twitterTitle = document.querySelector('meta[name="twitter:title"]')
      expect(twitterTitle).not.toBeNull()
    })

    it('page has twitter:description meta tag', () => {
      const twitterDescription = document.querySelector('meta[name="twitter:description"]')
      expect(twitterDescription).not.toBeNull()
    })
  })
})
