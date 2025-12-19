import { test, expect } from '@playwright/test'

test.describe('Hero Section Value Proposition', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test('displays product name MirDB in hero section on page load', async ({ page }) => {
    // The product name should be visible within the viewport without scrolling
    const productName = page.locator('h1')
    await expect(productName).toBeVisible()
    await expect(productName).toContainText('MirDB')

    // Verify it's in the viewport (above the fold)
    const boundingBox = await productName.boundingBox()
    expect(boundingBox).not.toBeNull()
    if (boundingBox) {
      // The heading should be within the initial viewport (typically 768px height)
      expect(boundingBox.y).toBeLessThan(768)
    }
  })

  test('displays value proposition mentioning persistent key-value store and memcached compatibility', async ({ page }) => {
    // Check for the value proposition text
    const heroSection = page.locator('.hero')
    await expect(heroSection).toBeVisible()

    // Should mention "persistent key-value store"
    const persistentText = page.getByText(/persistent key-value store/i)
    await expect(persistentText).toBeVisible()

    // Should mention "memcached" compatibility
    const memcachedText = page.getByText(/memcached/i)
    await expect(memcachedText.first()).toBeVisible()
  })

  test('displays CTA buttons linking to quick start or GitHub', async ({ page }) => {
    // Check for at least one CTA button
    const ctaButtons = page.locator('.hero a.cta-primary, .hero a.cta-secondary')
    const buttonCount = await ctaButtons.count()
    expect(buttonCount).toBeGreaterThanOrEqual(1)

    // Verify at least one button has a valid href
    const firstCta = ctaButtons.first()
    await expect(firstCta).toBeVisible()
    const href = await firstCta.getAttribute('href')
    expect(href).toBeTruthy()

    // Check for specific CTAs (either Get Started or GitHub link)
    const getStartedLink = page.getByRole('link', { name: /get started/i })
    const githubLink = page.getByRole('link', { name: /github/i })

    const hasGetStarted = await getStartedLink.isVisible().catch(() => false)
    const hasGithub = await githubLink.isVisible().catch(() => false)

    // At least one of these should be present
    expect(hasGetStarted || hasGithub).toBeTruthy()
  })

  test('hero section is above the fold on standard screen size', async ({ page }) => {
    // Set a standard viewport size
    await page.setViewportSize({ width: 1280, height: 720 })

    // Verify hero section is visible without scrolling
    const heroSection = page.locator('.hero')
    await expect(heroSection).toBeVisible()

    // Check that the hero section is within the viewport
    const boundingBox = await heroSection.boundingBox()
    expect(boundingBox).not.toBeNull()
    if (boundingBox) {
      // The hero section should start near the top
      expect(boundingBox.y).toBeLessThan(100)
    }
  })

  test('tagline is visible in hero section', async ({ page }) => {
    // The tagline should communicate the value proposition
    // Possible taglines: "Memcached, but persistent." or similar
    const taglineOptions = [
      /memcached,?\s*but\s*persistent/i,
      /key-value\s*store\s*that\s*remembers/i,
      /fast\s*caching.*durable\s*storage/i,
    ]

    let taglineFound = false
    for (const pattern of taglineOptions) {
      const tagline = page.getByText(pattern)
      if (await tagline.isVisible().catch(() => false)) {
        taglineFound = true
        break
      }
    }

    expect(taglineFound).toBeTruthy()
  })
})
