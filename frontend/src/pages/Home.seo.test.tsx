import { describe, it, expect, beforeAll } from 'vitest'
import { readFileSync } from 'fs'
import { resolve } from 'path'

describe('SEO - Meta Tags', () => {
  let htmlContent: string

  beforeAll(() => {
    // Read the index.html file which contains the static SEO meta tags
    const indexPath = resolve(__dirname, '../../index.html')
    htmlContent = readFileSync(indexPath, 'utf-8')
  })

  // Test Case 1: Page has descriptive title including product name
  it('has a descriptive title including product name', () => {
    // Extract title tag content
    const titleMatch = htmlContent.match(/<title>([^<]+)<\/title>/i)
    expect(titleMatch).toBeTruthy()

    const titleContent = titleMatch![1]
    expect(titleContent).toBeTruthy()
    expect(titleContent.length).toBeGreaterThan(0)
    // Title should include product name reference (URL Shortener or similar)
    expect(titleContent.toLowerCase()).toMatch(/url|shortener|shorten|link/i)
  })

  // Test Case 2: Meta description exists and describes the URL shortening service
  it('has meta description that describes the URL shortening service', () => {
    // Extract meta description content
    const descMatch = htmlContent.match(/<meta\s+name=["']description["']\s+content=["']([^"']+)["']/i)
    expect(descMatch).toBeTruthy()

    const descContent = descMatch![1]
    expect(descContent).toBeTruthy()
    expect(descContent.length).toBeGreaterThan(0)
    // Description should mention URL shortening or links
    expect(descContent.toLowerCase()).toMatch(/shorten|url|link|analytics|track/i)
  })

  // Test Case 3: og:title meta tag exists with appropriate content
  it('has og:title meta tag with appropriate content', () => {
    // Extract og:title content
    const ogTitleMatch = htmlContent.match(/<meta\s+property=["']og:title["']\s+content=["']([^"']+)["']/i)
    expect(ogTitleMatch).toBeTruthy()

    const ogTitleContent = ogTitleMatch![1]
    expect(ogTitleContent).toBeTruthy()
    expect(ogTitleContent.length).toBeGreaterThan(0)
    // OG title should be descriptive
    expect(ogTitleContent.toLowerCase()).toMatch(/url|shortener|shorten|link/i)
  })

  // Test Case 4: og:description meta tag exists with appropriate content
  it('has og:description meta tag with appropriate content', () => {
    // Extract og:description content
    const ogDescMatch = htmlContent.match(/<meta\s+property=["']og:description["']\s+content=["']([^"']+)["']/i)
    expect(ogDescMatch).toBeTruthy()

    const ogDescContent = ogDescMatch![1]
    expect(ogDescContent).toBeTruthy()
    expect(ogDescContent.length).toBeGreaterThan(0)
    // OG description should describe the service
    expect(ogDescContent.toLowerCase()).toMatch(/shorten|url|link|analytics|track|click/i)
  })

  // Test Case 5: og:image meta tag exists for social sharing
  it('has og:image meta tag for social sharing', () => {
    // Extract og:image content
    const ogImageMatch = htmlContent.match(/<meta\s+property=["']og:image["']\s+content=["']([^"']+)["']/i)
    expect(ogImageMatch).toBeTruthy()

    const ogImageContent = ogImageMatch![1]
    expect(ogImageContent).toBeTruthy()
    expect(ogImageContent.length).toBeGreaterThan(0)
    // OG image should be a valid URL or path to an image
    expect(ogImageContent).toMatch(/\.(png|jpg|jpeg|gif|svg|webp)|^https?:\/\//i)
  })
})
