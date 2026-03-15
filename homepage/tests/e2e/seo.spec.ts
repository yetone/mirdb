import { test, expect } from '@playwright/test'

test.describe('SEO Best Practices', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  // Test Case 1: Title tag contains 'MirDB' and describes the product
  test('page title contains MirDB and describes the product', async ({ page }) => {
    const title = await page.title()
    expect(title).toContain('MirDB')
    expect(title.toLowerCase()).toMatch(/key-value|store|memcached|persistent/)
  })

  // Test Case 2: Meta description tag is present and between 150-160 characters
  test('meta description is present with proper length', async ({ page }) => {
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content')
    expect(metaDescription).not.toBeNull()
    expect(metaDescription!.length).toBeGreaterThanOrEqual(150)
    expect(metaDescription!.length).toBeLessThanOrEqual(160)
  })

  // Test Case 3: og:title meta tag is present
  test('og:title meta tag is present', async ({ page }) => {
    const ogTitle = await page.locator('meta[property="og:title"]').getAttribute('content')
    expect(ogTitle).not.toBeNull()
    expect(ogTitle).toContain('MirDB')
  })

  // Test Case 4: og:description meta tag is present
  test('og:description meta tag is present', async ({ page }) => {
    const ogDescription = await page.locator('meta[property="og:description"]').getAttribute('content')
    expect(ogDescription).not.toBeNull()
    expect(ogDescription!.length).toBeGreaterThan(0)
  })

  // Test Case 5: og:image meta tag is present with valid URL
  test('og:image meta tag is present with valid URL', async ({ page }) => {
    const ogImage = await page.locator('meta[property="og:image"]').getAttribute('content')
    expect(ogImage).not.toBeNull()
    // Check it's a valid URL
    expect(ogImage).toMatch(/^(https?:\/\/|\/)/i)
  })

  // Test Case 6: Canonical link tag is present
  test('canonical link tag is present', async ({ page }) => {
    const canonical = await page.locator('link[rel="canonical"]').getAttribute('href')
    expect(canonical).not.toBeNull()
    expect(canonical!.length).toBeGreaterThan(0)
  })

  // Test Case 7: JSON-LD script with SoftwareApplication schema is present
  test('JSON-LD structured data with SoftwareApplication schema is present', async ({ page }) => {
    const scripts = await page.locator('script[type="application/ld+json"]').all()
    expect(scripts.length).toBeGreaterThan(0)

    let foundSoftwareApp = false
    for (const script of scripts) {
      const content = await script.textContent()
      try {
        const data = JSON.parse(content || '{}')
        if (data['@type'] === 'SoftwareApplication' ||
            (Array.isArray(data['@graph']) && data['@graph'].some((item: { '@type': string }) => item['@type'] === 'SoftwareApplication'))) {
          foundSoftwareApp = true
          break
        }
      } catch {
        // Ignore parse errors
      }
    }
    expect(foundSoftwareApp).toBe(true)
  })

  // Test Case 8: No critical SEO issues detected
  test('no critical SEO issues detected', async ({ page }) => {
    // Check for critical SEO elements
    const criticalChecks = []

    // Title exists and is not empty
    const title = await page.title()
    criticalChecks.push({ check: 'Title exists', pass: title.length > 0 })

    // Meta description exists
    const metaDescription = await page.locator('meta[name="description"]').getAttribute('content')
    criticalChecks.push({ check: 'Meta description exists', pass: metaDescription !== null && metaDescription.length > 0 })

    // H1 tag exists and contains product name
    const h1 = await page.locator('h1').first()
    const h1Text = await h1.textContent()
    criticalChecks.push({ check: 'H1 exists with content', pass: h1Text !== null && h1Text.includes('MirDB') })

    // Images have alt attributes
    const imagesWithoutAlt = await page.locator('img:not([alt])').count()
    criticalChecks.push({ check: 'All images have alt text', pass: imagesWithoutAlt === 0 })

    // Viewport meta tag exists
    const viewport = await page.locator('meta[name="viewport"]').getAttribute('content')
    criticalChecks.push({ check: 'Viewport meta tag exists', pass: viewport !== null })

    // Lang attribute on html
    const lang = await page.locator('html').getAttribute('lang')
    criticalChecks.push({ check: 'HTML lang attribute exists', pass: lang !== null && lang.length > 0 })

    // Open Graph tags
    const ogTitle = await page.locator('meta[property="og:title"]').getAttribute('content')
    criticalChecks.push({ check: 'Open Graph title exists', pass: ogTitle !== null })

    // Canonical URL
    const canonical = await page.locator('link[rel="canonical"]').getAttribute('href')
    criticalChecks.push({ check: 'Canonical URL exists', pass: canonical !== null })

    // Structured data
    const structuredDataCount = await page.locator('script[type="application/ld+json"]').count()
    criticalChecks.push({ check: 'Structured data exists', pass: structuredDataCount > 0 })

    // All checks should pass
    const failedChecks = criticalChecks.filter(c => !c.pass)
    expect(failedChecks, `Failed SEO checks: ${failedChecks.map(c => c.check).join(', ')}`).toHaveLength(0)
  })

  test('og:type meta tag is present', async ({ page }) => {
    const ogType = await page.locator('meta[property="og:type"]').getAttribute('content')
    expect(ogType).toBe('website')
  })

  test('og:url meta tag is present', async ({ page }) => {
    const ogUrl = await page.locator('meta[property="og:url"]').getAttribute('content')
    expect(ogUrl).not.toBeNull()
  })

  test('Twitter card meta tags are present', async ({ page }) => {
    const twitterCard = await page.locator('meta[name="twitter:card"]').getAttribute('content')
    expect(twitterCard).toBe('summary_large_image')
  })
})
