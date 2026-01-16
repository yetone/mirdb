import { test, expect } from '@playwright/test'

test.describe('Error Handling - Slow Network (3G)', () => {
  test('should handle slow network gracefully and eventually load content', async ({ page }) => {
    // Create a CDP session to emulate slow network (3G)
    const client = await page.context().newCDPSession(page)
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (750 * 1024) / 8, // 750 kb/s
      uploadThroughput: (250 * 1024) / 8, // 250 kb/s
      latency: 100, // 100ms RTT
    })

    // Navigate to homepage with extended timeout for slow network
    await page.goto('/', { timeout: 60000 })

    // Wait for content to be visible (even if slow)
    await expect(page.locator('body')).toBeVisible({ timeout: 30000 })

    // The page should eventually render content
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible({ timeout: 30000 })

    // Verify main headline is visible
    const headline = page.getByTestId('hero-headline')
    await expect(headline).toBeVisible({ timeout: 30000 })
    await expect(headline).toContainText('Shorten')
  })

  test('should display page structure correctly on slow connection', async ({ page }) => {
    // Emulate slow 3G network
    const client = await page.context().newCDPSession(page)
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (750 * 1024) / 8,
      uploadThroughput: (250 * 1024) / 8,
      latency: 100,
    })

    await page.goto('/', { timeout: 60000 })

    // Wait for the main content to load
    await expect(page.locator('main')).toBeVisible({ timeout: 30000 })

    // Verify features section loads
    const featuresSection = page.getByTestId('features-section')
    await expect(featuresSection).toBeVisible({ timeout: 30000 })
  })

  test('should maintain interactivity after slow load', async ({ page }) => {
    // Emulate slow 3G network
    const client = await page.context().newCDPSession(page)
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (750 * 1024) / 8,
      uploadThroughput: (250 * 1024) / 8,
      latency: 100,
    })

    await page.goto('/', { timeout: 60000 })

    // Wait for content
    await expect(page.getByTestId('hero-section')).toBeVisible({ timeout: 30000 })

    // Restore normal network to test interaction
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: -1,
      uploadThroughput: -1,
      latency: 0,
    })

    // Verify CTA buttons are clickable
    const ctaButton = page.getByTestId('cta-get-started')
    await expect(ctaButton).toBeVisible()
    await expect(ctaButton).toBeEnabled()
  })

  test('should not show blank page during slow load', async ({ page }) => {
    // Emulate very slow network
    const client = await page.context().newCDPSession(page)
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (400 * 1024) / 8, // Very slow
      uploadThroughput: (100 * 1024) / 8,
      latency: 200,
    })

    // Start navigation
    await page.goto('/', { timeout: 90000 })

    // The body should have content, not be empty
    const bodyContent = await page.locator('body').innerHTML()
    expect(bodyContent.length).toBeGreaterThan(100)

    // Root div should be present
    const root = page.locator('#root')
    await expect(root).toBeAttached()
  })
})
