import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, cleanup } from '@testing-library/react'
import { Head } from '@/components/seo/Head'

describe('Head Component', () => {
  beforeEach(() => {
    // Clear any existing meta tags from previous tests
    const existingMetas = document.head.querySelectorAll('meta[data-testid="seo-meta"]')
    existingMetas.forEach(meta => meta.remove())
    const existingCanonical = document.head.querySelector('link[rel="canonical"]')
    if (existingCanonical) existingCanonical.remove()
  })

  afterEach(() => {
    cleanup()
    // Clean up meta tags after each test
    const metas = document.head.querySelectorAll('meta[data-testid="seo-meta"]')
    metas.forEach(meta => meta.remove())
    const canonical = document.head.querySelector('link[rel="canonical"]')
    if (canonical) canonical.remove()
  })

  // Test Case 1: Title tag contains 'MirDB' and describes the product
  it('updates document title to contain MirDB and describe the product', () => {
    render(<Head />)

    expect(document.title).toContain('MirDB')
    expect(document.title.toLowerCase()).toMatch(/key-value|store|memcached|persistent/)
  })

  // Test Case 2: Meta description tag is present and between 150-160 characters
  it('adds meta description with proper length (150-160 characters)', () => {
    render(<Head />)

    const metaDescription = document.head.querySelector('meta[name="description"]')
    expect(metaDescription).not.toBeNull()

    const content = metaDescription?.getAttribute('content') || ''
    expect(content.length).toBeGreaterThanOrEqual(150)
    expect(content.length).toBeLessThanOrEqual(160)
  })

  // Test Case 3: og:title meta tag is present
  it('adds og:title meta tag', () => {
    render(<Head />)

    const ogTitle = document.head.querySelector('meta[property="og:title"]')
    expect(ogTitle).not.toBeNull()
    expect(ogTitle?.getAttribute('content')).toContain('MirDB')
  })

  // Test Case 4: og:description meta tag is present
  it('adds og:description meta tag', () => {
    render(<Head />)

    const ogDescription = document.head.querySelector('meta[property="og:description"]')
    expect(ogDescription).not.toBeNull()
    expect(ogDescription?.getAttribute('content')).toBeTruthy()
  })

  // Test Case 5: og:image meta tag is present with valid URL
  it('adds og:image meta tag with valid URL', () => {
    render(<Head />)

    const ogImage = document.head.querySelector('meta[property="og:image"]')
    expect(ogImage).not.toBeNull()

    const imageUrl = ogImage?.getAttribute('content') || ''
    // Check it's a valid URL (starts with http/https or is a valid relative path)
    expect(imageUrl).toMatch(/^(https?:\/\/|\/)/i)
  })

  // Test Case 6: Canonical link tag is present
  it('adds canonical link tag', () => {
    render(<Head />)

    const canonical = document.head.querySelector('link[rel="canonical"]')
    expect(canonical).not.toBeNull()
    expect(canonical?.getAttribute('href')).toBeTruthy()
  })

  it('adds og:type meta tag', () => {
    render(<Head />)

    const ogType = document.head.querySelector('meta[property="og:type"]')
    expect(ogType).not.toBeNull()
    expect(ogType?.getAttribute('content')).toBe('website')
  })

  it('adds og:url meta tag', () => {
    render(<Head />)

    const ogUrl = document.head.querySelector('meta[property="og:url"]')
    expect(ogUrl).not.toBeNull()
    expect(ogUrl?.getAttribute('content')).toBeTruthy()
  })

  it('adds Twitter card meta tags', () => {
    render(<Head />)

    const twitterCard = document.head.querySelector('meta[name="twitter:card"]')
    expect(twitterCard).not.toBeNull()
    expect(twitterCard?.getAttribute('content')).toBe('summary_large_image')
  })
})
