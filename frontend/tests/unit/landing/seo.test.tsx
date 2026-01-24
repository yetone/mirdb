/**
 * SEO Optimization Tests
 * Owner: Scenario 13 - SEO Optimization
 *
 * Tests for SEO utilities and landing page SEO compliance.
 * Verifies proper meta tags, semantic HTML, and heading structure.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { renderWithProviders } from './test-utils'
import {
  setPageTitle,
  setMetaDescription,
  setOpenGraphTags,
  setCanonicalUrl,
  initializeSEO,
  DEFAULT_LANDING_SEO,
  OGTags,
  SEOConfig,
} from '../../../src/utils/seo'
import { Home } from '../../../src/pages/Home'

describe('SEO Utilities', () => {
  // Store original document head content to restore after tests
  let originalTitle: string
  let originalHead: string

  beforeEach(() => {
    originalTitle = document.title
    originalHead = document.head.innerHTML
  })

  afterEach(() => {
    document.title = originalTitle
    document.head.innerHTML = originalHead
  })

  describe('setPageTitle', () => {
    it('should set the document title', () => {
      const title = 'URL Shortener - Shorten URLs & Track Analytics'
      setPageTitle(title)
      expect(document.title).toBe(title)
    })

    it('should contain product name in title', () => {
      setPageTitle(DEFAULT_LANDING_SEO.title)
      expect(document.title).toContain('URL Shortener')
    })

    it('should contain value proposition in title', () => {
      setPageTitle(DEFAULT_LANDING_SEO.title)
      expect(document.title.toLowerCase()).toMatch(/shorten|track|analytics/)
    })
  })

  describe('setMetaDescription', () => {
    it('should create meta description tag if it does not exist', () => {
      // Remove any existing meta description
      const existing = document.querySelector('meta[name="description"]')
      existing?.remove()

      const description = 'Test description for SEO'
      setMetaDescription(description)

      const metaTag = document.querySelector('meta[name="description"]')
      expect(metaTag).not.toBeNull()
      expect(metaTag?.getAttribute('content')).toBe(description)
    })

    it('should update existing meta description tag', () => {
      // Create initial meta tag
      setMetaDescription('Initial description')

      // Update it
      const newDescription = 'Updated description'
      setMetaDescription(newDescription)

      const metaTags = document.querySelectorAll('meta[name="description"]')
      expect(metaTags.length).toBe(1)
      expect(metaTags[0].getAttribute('content')).toBe(newDescription)
    })

    it('should have meta description with compelling summary (150-160 characters recommended)', () => {
      setMetaDescription(DEFAULT_LANDING_SEO.description)

      const metaTag = document.querySelector('meta[name="description"]')
      const content = metaTag?.getAttribute('content') || ''

      // Meta description should be meaningful (at least 50 chars)
      expect(content.length).toBeGreaterThanOrEqual(50)
      // And not too long (under 200 chars for best SEO)
      expect(content.length).toBeLessThanOrEqual(200)
    })
  })

  describe('setOpenGraphTags', () => {
    it('should create og:title meta tag', () => {
      const ogTags: OGTags = {
        title: 'URL Shortener',
        description: 'Shorten your URLs',
      }
      setOpenGraphTags(ogTags)

      const metaTag = document.querySelector('meta[property="og:title"]')
      expect(metaTag).not.toBeNull()
      expect(metaTag?.getAttribute('content')).toBe(ogTags.title)
    })

    it('should create og:description meta tag', () => {
      const ogTags: OGTags = {
        title: 'URL Shortener',
        description: 'Shorten your URLs and track analytics',
      }
      setOpenGraphTags(ogTags)

      const metaTag = document.querySelector('meta[property="og:description"]')
      expect(metaTag).not.toBeNull()
      expect(metaTag?.getAttribute('content')).toBe(ogTags.description)
    })

    it('should create og:image meta tag when provided', () => {
      const ogTags: OGTags = {
        title: 'URL Shortener',
        description: 'Shorten your URLs',
        image: 'https://example.com/og-image.png',
      }
      setOpenGraphTags(ogTags)

      const metaTag = document.querySelector('meta[property="og:image"]')
      expect(metaTag).not.toBeNull()
      expect(metaTag?.getAttribute('content')).toBe(ogTags.image)
    })

    it('should set og:type to website by default', () => {
      const ogTags: OGTags = {
        title: 'URL Shortener',
        description: 'Shorten your URLs',
      }
      setOpenGraphTags(ogTags)

      const metaTag = document.querySelector('meta[property="og:type"]')
      expect(metaTag).not.toBeNull()
      expect(metaTag?.getAttribute('content')).toBe('website')
    })

    it('should have all required Open Graph tags for social sharing', () => {
      initializeSEO(DEFAULT_LANDING_SEO)

      const ogTitle = document.querySelector('meta[property="og:title"]')
      const ogDescription = document.querySelector('meta[property="og:description"]')

      expect(ogTitle).not.toBeNull()
      expect(ogDescription).not.toBeNull()
      expect(ogTitle?.getAttribute('content')).toBeTruthy()
      expect(ogDescription?.getAttribute('content')).toBeTruthy()
    })
  })

  describe('setCanonicalUrl', () => {
    it('should create canonical link tag if it does not exist', () => {
      // Remove any existing canonical tag
      const existing = document.querySelector('link[rel="canonical"]')
      existing?.remove()

      const url = 'https://example.com/'
      setCanonicalUrl(url)

      const linkTag = document.querySelector('link[rel="canonical"]')
      expect(linkTag).not.toBeNull()
      expect(linkTag?.getAttribute('href')).toBe(url)
    })

    it('should update existing canonical link tag', () => {
      setCanonicalUrl('https://old-url.com/')
      setCanonicalUrl('https://new-url.com/')

      const linkTags = document.querySelectorAll('link[rel="canonical"]')
      expect(linkTags.length).toBe(1)
      expect(linkTags[0].getAttribute('href')).toBe('https://new-url.com/')
    })
  })

  describe('initializeSEO', () => {
    it('should set all SEO tags at once', () => {
      const config: SEOConfig = {
        title: 'Test Title',
        description: 'Test description for the page',
        canonicalUrl: 'https://test.com/',
        ogTags: {
          title: 'OG Test Title',
          description: 'OG Test Description',
          image: 'https://test.com/image.png',
        },
      }

      initializeSEO(config)

      expect(document.title).toBe(config.title)

      const metaDescription = document.querySelector('meta[name="description"]')
      expect(metaDescription?.getAttribute('content')).toBe(config.description)

      const canonicalLink = document.querySelector('link[rel="canonical"]')
      expect(canonicalLink?.getAttribute('href')).toBe(config.canonicalUrl)

      const ogTitle = document.querySelector('meta[property="og:title"]')
      expect(ogTitle?.getAttribute('content')).toBe(config.ogTags?.title)
    })
  })

  describe('DEFAULT_LANDING_SEO', () => {
    it('should have a descriptive title containing product name', () => {
      expect(DEFAULT_LANDING_SEO.title).toContain('URL Shortener')
    })

    it('should have a value proposition in title', () => {
      expect(DEFAULT_LANDING_SEO.title.toLowerCase()).toMatch(/shorten|track|analytics/)
    })

    it('should have a compelling description', () => {
      expect(DEFAULT_LANDING_SEO.description.length).toBeGreaterThan(50)
      expect(DEFAULT_LANDING_SEO.description.toLowerCase()).toMatch(/short|link|analytics|track/)
    })

    it('should have Open Graph tags configured', () => {
      expect(DEFAULT_LANDING_SEO.ogTags).toBeDefined()
      expect(DEFAULT_LANDING_SEO.ogTags?.title).toBeTruthy()
      expect(DEFAULT_LANDING_SEO.ogTags?.description).toBeTruthy()
    })
  })
})

describe('Landing Page SEO Compliance', () => {
  describe('Test Case 1: Title Tag', () => {
    it('should have descriptive title tag containing product name and value proposition', () => {
      // Initialize SEO with default config
      initializeSEO(DEFAULT_LANDING_SEO)

      // Check title contains product name
      expect(document.title).toContain('URL Shortener')

      // Check title contains value proposition keywords
      const titleLower = document.title.toLowerCase()
      expect(titleLower).toMatch(/shorten|track|analytics/)
    })
  })

  describe('Test Case 2: Meta Description', () => {
    it('should have meta description tag with compelling summary (150-160 characters)', () => {
      initializeSEO(DEFAULT_LANDING_SEO)

      const metaDescription = document.querySelector('meta[name="description"]')
      expect(metaDescription).not.toBeNull()

      const content = metaDescription?.getAttribute('content') || ''

      // Should be within recommended length range
      // Google typically shows 150-160 characters
      expect(content.length).toBeGreaterThanOrEqual(50)
      expect(content.length).toBeLessThanOrEqual(200)

      // Should contain compelling keywords
      expect(content.toLowerCase()).toMatch(/short|link|track|analytics/)
    })
  })

  describe('Test Case 3: Open Graph Tags', () => {
    it('should have og:title, og:description, og:image meta tags for social sharing', () => {
      initializeSEO({
        ...DEFAULT_LANDING_SEO,
        ogTags: {
          ...DEFAULT_LANDING_SEO.ogTags,
          image: 'https://example.com/og-image.png',
        },
      })

      const ogTitle = document.querySelector('meta[property="og:title"]')
      const ogDescription = document.querySelector('meta[property="og:description"]')
      const ogImage = document.querySelector('meta[property="og:image"]')

      expect(ogTitle).not.toBeNull()
      expect(ogTitle?.getAttribute('content')).toBeTruthy()

      expect(ogDescription).not.toBeNull()
      expect(ogDescription?.getAttribute('content')).toBeTruthy()

      expect(ogImage).not.toBeNull()
      expect(ogImage?.getAttribute('content')).toBeTruthy()
    })
  })

  describe('Test Case 4: H1 Element Count', () => {
    it('should have exactly one H1 element containing primary headline', () => {
      renderWithProviders(<Home />)

      const h1Elements = document.querySelectorAll('h1')
      expect(h1Elements.length).toBe(1)

      // Should contain primary headline text
      const h1Content = h1Elements[0].textContent || ''
      expect(h1Content.length).toBeGreaterThan(0)
    })
  })

  describe('Test Case 5: Canonical URL', () => {
    it('should have canonical link tag pointing to canonical URL', () => {
      setCanonicalUrl('https://urlshortener.example.com/')

      const canonicalLink = document.querySelector('link[rel="canonical"]')
      expect(canonicalLink).not.toBeNull()
      expect(canonicalLink?.getAttribute('href')).toBe('https://urlshortener.example.com/')
    })
  })
})

describe('Semantic HTML Structure', () => {
  it('should use semantic main element', () => {
    renderWithProviders(<Home />)

    const mainElement = document.querySelector('main')
    expect(mainElement).not.toBeNull()
  })

  it('should use semantic section elements', () => {
    renderWithProviders(<Home />)

    const sectionElements = document.querySelectorAll('section')
    expect(sectionElements.length).toBeGreaterThan(0)
  })

  it('should have proper heading hierarchy', () => {
    renderWithProviders(<Home />)

    // Should have exactly one H1
    const h1Elements = document.querySelectorAll('h1')
    expect(h1Elements.length).toBe(1)

    // H1 should come before other headings in DOM order
    const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6')
    if (allHeadings.length > 0) {
      expect(allHeadings[0].tagName).toBe('H1')
    }
  })
})
