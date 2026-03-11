/**
 * SEO Optimization Tests
 * Owner: Scenario 13 - SEO Optimization
 *
 * Test coverage:
 * - Page title meta tag
 * - Meta description validation
 * - Viewport meta tag
 * - Open Graph tags (og:title, og:description, og:image, og:url)
 * - Twitter Card tags (twitter:card, twitter:title, twitter:description)
 * - Semantic HTML structure (header, main, section, footer)
 * - Canonical URL link
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import fs from 'fs'
import path from 'path'

// Read the actual index.html file
const indexHtmlPath = path.resolve(__dirname, '../../src/index.html')
let indexHtmlContent = ''

describe('SEO Optimization', () => {
  let container

  beforeEach(() => {
    // Read the actual HTML file
    indexHtmlContent = fs.readFileSync(indexHtmlPath, 'utf-8')

    // Create a DOM container
    container = document.createElement('div')
    container.innerHTML = indexHtmlContent
    document.body.appendChild(container)
  })

  afterEach(() => {
    if (container && container.parentNode) {
      container.parentNode.removeChild(container)
    }
  })

  /**
   * Test Case 1: Check page title meta tag
   * Expected: Title tag present with 'MirDB - Persistent Key-Value Store' or similar
   */
  describe('TC-1: Page title meta tag', () => {
    it('should have a title tag present', () => {
      const titleTag = container.querySelector('title')
      expect(titleTag).not.toBeNull()
    })

    it('should have title containing MirDB product name', () => {
      const titleTag = container.querySelector('title')
      expect(titleTag).not.toBeNull()

      const titleText = titleTag.textContent.toLowerCase()
      expect(titleText).toContain('mirdb')
    })

    it('should have title mentioning key-value store concept', () => {
      const titleTag = container.querySelector('title')
      expect(titleTag).not.toBeNull()

      const titleText = titleTag.textContent.toLowerCase()
      expect(
        titleText.includes('key-value') ||
        titleText.includes('persistent') ||
        titleText.includes('database') ||
        titleText.includes('store')
      ).toBe(true)
    })

    it('should have title with appropriate length (10-70 characters)', () => {
      const titleTag = container.querySelector('title')
      expect(titleTag).not.toBeNull()

      const titleText = titleTag.textContent.trim()
      expect(titleText.length).toBeGreaterThanOrEqual(10)
      expect(titleText.length).toBeLessThanOrEqual(70)
    })
  })

  /**
   * Test Case 2: Check meta description
   * Expected: Meta description present with concise product description under 160 characters
   */
  describe('TC-2: Meta description', () => {
    it('should have a meta description tag', () => {
      const metaDesc = container.querySelector('meta[name="description"]')
      expect(metaDesc).not.toBeNull()
    })

    it('should have meta description under 160 characters', () => {
      const metaDesc = container.querySelector('meta[name="description"]')
      expect(metaDesc).not.toBeNull()

      const content = metaDesc.getAttribute('content')
      expect(content).not.toBeNull()
      expect(content.length).toBeLessThanOrEqual(160)
    })

    it('should have meta description at least 50 characters for SEO value', () => {
      const metaDesc = container.querySelector('meta[name="description"]')
      expect(metaDesc).not.toBeNull()

      const content = metaDesc.getAttribute('content')
      expect(content).not.toBeNull()
      expect(content.length).toBeGreaterThanOrEqual(50)
    })

    it('should have meta description mentioning MirDB', () => {
      const metaDesc = container.querySelector('meta[name="description"]')
      expect(metaDesc).not.toBeNull()

      const content = metaDesc.getAttribute('content').toLowerCase()
      expect(content).toContain('mirdb')
    })
  })

  /**
   * Test Case 3: Check viewport meta tag
   * Expected: Viewport meta tag with 'width=device-width, initial-scale=1'
   */
  describe('TC-3: Viewport meta tag', () => {
    it('should have a viewport meta tag', () => {
      const viewport = container.querySelector('meta[name="viewport"]')
      expect(viewport).not.toBeNull()
    })

    it('should have viewport with device-width', () => {
      const viewport = container.querySelector('meta[name="viewport"]')
      expect(viewport).not.toBeNull()

      const content = viewport.getAttribute('content')
      expect(content).toContain('width=device-width')
    })

    it('should have viewport with initial-scale=1', () => {
      const viewport = container.querySelector('meta[name="viewport"]')
      expect(viewport).not.toBeNull()

      const content = viewport.getAttribute('content')
      expect(content).toMatch(/initial-scale\s*=\s*1/)
    })
  })

  /**
   * Test Case 4: Check Open Graph tags
   * Expected: og:title, og:description, og:image, og:url tags present
   */
  describe('TC-4: Open Graph tags', () => {
    it('should have og:title meta tag', () => {
      const ogTitle = container.querySelector('meta[property="og:title"]')
      expect(ogTitle).not.toBeNull()

      const content = ogTitle.getAttribute('content')
      expect(content).not.toBeNull()
      expect(content.length).toBeGreaterThan(0)
    })

    it('should have og:description meta tag', () => {
      const ogDesc = container.querySelector('meta[property="og:description"]')
      expect(ogDesc).not.toBeNull()

      const content = ogDesc.getAttribute('content')
      expect(content).not.toBeNull()
      expect(content.length).toBeGreaterThan(0)
    })

    it('should have og:image meta tag', () => {
      const ogImage = container.querySelector('meta[property="og:image"]')
      expect(ogImage).not.toBeNull()

      const content = ogImage.getAttribute('content')
      expect(content).not.toBeNull()
      expect(content.length).toBeGreaterThan(0)
    })

    it('should have og:url meta tag', () => {
      const ogUrl = container.querySelector('meta[property="og:url"]')
      expect(ogUrl).not.toBeNull()

      const content = ogUrl.getAttribute('content')
      expect(content).not.toBeNull()
      expect(content.length).toBeGreaterThan(0)
    })

    it('should have og:type meta tag', () => {
      const ogType = container.querySelector('meta[property="og:type"]')
      expect(ogType).not.toBeNull()

      const content = ogType.getAttribute('content')
      expect(content).toBe('website')
    })

    it('should have og:site_name meta tag', () => {
      const ogSiteName = container.querySelector('meta[property="og:site_name"]')
      expect(ogSiteName).not.toBeNull()

      const content = ogSiteName.getAttribute('content')
      expect(content.toLowerCase()).toContain('mirdb')
    })
  })

  /**
   * Test Case 5: Check Twitter Card tags
   * Expected: twitter:card, twitter:title, twitter:description tags present
   */
  describe('TC-5: Twitter Card tags', () => {
    it('should have twitter:card meta tag', () => {
      const twitterCard = container.querySelector('meta[name="twitter:card"]')
      expect(twitterCard).not.toBeNull()

      const content = twitterCard.getAttribute('content')
      expect(['summary', 'summary_large_image']).toContain(content)
    })

    it('should have twitter:title meta tag', () => {
      const twitterTitle = container.querySelector('meta[name="twitter:title"]')
      expect(twitterTitle).not.toBeNull()

      const content = twitterTitle.getAttribute('content')
      expect(content).not.toBeNull()
      expect(content.length).toBeGreaterThan(0)
    })

    it('should have twitter:description meta tag', () => {
      const twitterDesc = container.querySelector('meta[name="twitter:description"]')
      expect(twitterDesc).not.toBeNull()

      const content = twitterDesc.getAttribute('content')
      expect(content).not.toBeNull()
      expect(content.length).toBeGreaterThan(0)
    })

    it('should have twitter:image meta tag', () => {
      const twitterImage = container.querySelector('meta[name="twitter:image"]')
      expect(twitterImage).not.toBeNull()

      const content = twitterImage.getAttribute('content')
      expect(content).not.toBeNull()
      expect(content.length).toBeGreaterThan(0)
    })
  })

  /**
   * Test Case 6: Check semantic HTML structure
   * Expected: Page uses header, main, section, article, footer elements appropriately
   */
  describe('TC-6: Semantic HTML structure', () => {
    it('should have a header element', () => {
      const header = container.querySelector('header')
      expect(header).not.toBeNull()
    })

    it('should have a main element', () => {
      const main = container.querySelector('main')
      expect(main).not.toBeNull()
    })

    it('should have section elements for content organization', () => {
      const sections = container.querySelectorAll('section')
      expect(sections.length).toBeGreaterThan(0)
    })

    it('should have a footer element', () => {
      const footer = container.querySelector('footer')
      expect(footer).not.toBeNull()
    })

    it('should have html element with lang attribute', () => {
      const html = container.querySelector('html')
      expect(html).not.toBeNull()
      expect(html.getAttribute('lang')).toBe('en')
    })

    it('should have sections with meaningful id attributes', () => {
      const sections = container.querySelectorAll('section[id]')
      expect(sections.length).toBeGreaterThan(0)

      sections.forEach(section => {
        const id = section.getAttribute('id')
        expect(id).not.toBeNull()
        expect(id.length).toBeGreaterThan(0)
      })
    })

    it('should have proper document structure with DOCTYPE', () => {
      expect(indexHtmlContent.trim().toLowerCase().startsWith('<!doctype html')).toBe(true)
    })
  })

  /**
   * Test Case 7: Check canonical URL
   * Expected: Canonical link tag points to primary URL
   */
  describe('TC-7: Canonical URL', () => {
    it('should have a canonical link tag', () => {
      const canonical = container.querySelector('link[rel="canonical"]')
      expect(canonical).not.toBeNull()
    })

    it('should have canonical link with valid href', () => {
      const canonical = container.querySelector('link[rel="canonical"]')
      expect(canonical).not.toBeNull()

      const href = canonical.getAttribute('href')
      expect(href).not.toBeNull()
      expect(href.length).toBeGreaterThan(0)
    })

    it('should have canonical link with absolute URL', () => {
      const canonical = container.querySelector('link[rel="canonical"]')
      expect(canonical).not.toBeNull()

      const href = canonical.getAttribute('href')
      expect(href).toMatch(/^https?:\/\//)
    })
  })

  /**
   * Additional SEO best practices
   */
  describe('Additional SEO validations', () => {
    it('should have charset meta tag as first element in head', () => {
      const head = container.querySelector('head')
      expect(head).not.toBeNull()

      const firstMeta = head.querySelector('meta')
      expect(firstMeta).not.toBeNull()
      expect(firstMeta.getAttribute('charset')).toBe('UTF-8')
    })

    it('should have robots meta tag or default to index,follow', () => {
      const robotsMeta = container.querySelector('meta[name="robots"]')
      // robots meta is optional - if not present, default is index,follow
      if (robotsMeta) {
        const content = robotsMeta.getAttribute('content')
        expect(content).toMatch(/index|follow|all/)
      }
      // No robots meta is acceptable - defaults to index,follow
      expect(true).toBe(true)
    })

    it('should not have duplicate title tags', () => {
      const titles = container.querySelectorAll('title')
      expect(titles.length).toBe(1)
    })

    it('should not have duplicate meta descriptions', () => {
      const metaDescs = container.querySelectorAll('meta[name="description"]')
      expect(metaDescs.length).toBe(1)
    })

    it('should have og:image with absolute URL', () => {
      const ogImage = container.querySelector('meta[property="og:image"]')
      expect(ogImage).not.toBeNull()

      const content = ogImage.getAttribute('content')
      expect(content).toMatch(/^https?:\/\//)
    })
  })
})
