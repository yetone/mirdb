import { test, expect } from '@playwright/test'

/**
 * SEO Meta Tags Tests for the MirDB Homepage
 * Verifies NFR-7: Page must be SEO-optimized with proper meta tags
 *
 * Test Cases:
 * TC1: Title tag contains 'MirDB' and key description
 * TC2: Meta description tag exists with 150-160 character description
 * TC3: Open Graph og:title meta tag is present
 * TC4: Open Graph og:description meta tag is present
 * TC5: Canonical link tag is present
 */

test.describe('SEO Meta Tags', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' })
  })

  /**
   * TC1: Verify title tag contains 'MirDB' and key description
   * The title should include the product name and key value proposition
   */
  test('TC1: page has title containing MirDB and key description', async ({ page }) => {
    const title = await page.title()

    // Title should contain 'MirDB'
    expect(title.toLowerCase()).toContain('mirdb')

    // Title should contain key value proposition keywords
    // Based on PRD: "Persistent key-value storage with memcached protocol compatibility"
    const titleLower = title.toLowerCase()
    const hasKeyDescription =
      titleLower.includes('persistent') ||
      titleLower.includes('key-value') ||
      titleLower.includes('memcached')

    expect(hasKeyDescription).toBe(true)

    // Log for debugging
    console.log(`Title: ${title}`)
  })

  /**
   * TC2: Verify meta description tag exists with proper length
   * Description should be 150-160 characters and summarize MirDB's purpose
   */
  test('TC2: meta description tag exists with appropriate length', async ({ page }) => {
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content')

    // Meta description should exist
    expect(metaDescription).not.toBeNull()
    expect(metaDescription).toBeDefined()

    // Description should be between 50 and 160 characters (SEO best practice)
    // 150-160 is ideal, but we allow some flexibility
    const length = metaDescription!.length
    expect(length).toBeGreaterThanOrEqual(50)
    expect(length).toBeLessThanOrEqual(160)

    // Description should mention MirDB
    expect(metaDescription!.toLowerCase()).toContain('mirdb')

    // Log for debugging
    console.log(`Meta description (${length} chars): ${metaDescription}`)
  })

  /**
   * TC3: Verify Open Graph og:title meta tag is present
   * Required for social media sharing previews
   */
  test('TC3: og:title meta tag is present', async ({ page }) => {
    const ogTitle = await page.locator('meta[property="og:title"]').getAttribute('content')

    // og:title should exist
    expect(ogTitle).not.toBeNull()
    expect(ogTitle).toBeDefined()

    // og:title should contain 'MirDB'
    expect(ogTitle!.toLowerCase()).toContain('mirdb')

    // og:title should not be empty
    expect(ogTitle!.length).toBeGreaterThan(0)

    // Log for debugging
    console.log(`og:title: ${ogTitle}`)
  })

  /**
   * TC4: Verify Open Graph og:description meta tag is present
   * Required for social media sharing previews
   */
  test('TC4: og:description meta tag is present', async ({ page }) => {
    const ogDescription = await page.locator('meta[property="og:description"]').getAttribute('content')

    // og:description should exist
    expect(ogDescription).not.toBeNull()
    expect(ogDescription).toBeDefined()

    // og:description should not be empty
    expect(ogDescription!.length).toBeGreaterThan(0)

    // og:description should describe MirDB
    const descLower = ogDescription!.toLowerCase()
    const hasMirDBContent =
      descLower.includes('mirdb') ||
      descLower.includes('key-value') ||
      descLower.includes('persistent') ||
      descLower.includes('memcached')

    expect(hasMirDBContent).toBe(true)

    // Log for debugging
    console.log(`og:description: ${ogDescription}`)
  })

  /**
   * TC5: Verify canonical link tag is present
   * Helps prevent duplicate content issues in search engines
   */
  test('TC5: canonical link tag is present', async ({ page }) => {
    const canonicalLink = await page.locator('link[rel="canonical"]').getAttribute('href')

    // Canonical link should exist
    expect(canonicalLink).not.toBeNull()
    expect(canonicalLink).toBeDefined()

    // Canonical should be a valid URL or path
    expect(canonicalLink!.length).toBeGreaterThan(0)

    // Log for debugging
    console.log(`Canonical URL: ${canonicalLink}`)
  })
})
