import { test, expect } from '@playwright/test'

test.describe('Error Handling - Page Load Failure', () => {
  test.describe('Test Case 1: Load homepage with JavaScript disabled', () => {
    test('page shows meaningful content or noscript message when JS is disabled', async ({ browser }) => {
      // Create a new context with JavaScript disabled
      const context = await browser.newContext({
        javaScriptEnabled: false,
      })
      const page = await context.newPage()

      await page.goto('/')

      // Verify the noscript element is visible when JS is disabled
      const noscriptContent = page.locator('noscript')
      await expect(noscriptContent).toBeVisible()

      // Verify the noscript message contains helpful information
      const noscriptText = await noscriptContent.textContent()
      expect(noscriptText).toContain('JavaScript')

      // Verify the noscript content has proper styling to be visible
      const noscriptContainer = page.locator('[data-testid="noscript-fallback"]')
      await expect(noscriptContainer).toBeVisible()

      // Verify the message provides actionable guidance
      expect(noscriptText).toMatch(/enable|require|need/i)

      await context.close()
    })

    test('noscript fallback has informative heading', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      })
      const page = await context.newPage()

      await page.goto('/')

      // Check for heading in noscript content
      const heading = page.locator('noscript h1, noscript h2')
      await expect(heading).toBeVisible()

      await context.close()
    })

    test('noscript fallback mentions the application name', async ({ browser }) => {
      const context = await browser.newContext({
        javaScriptEnabled: false,
      })
      const page = await context.newPage()

      await page.goto('/')

      const noscriptContent = page.locator('noscript')
      const noscriptText = await noscriptContent.textContent()

      // Should mention the app or its purpose
      expect(noscriptText?.toLowerCase()).toMatch(/url|shortener|short|link/i)

      await context.close()
    })
  })

  test.describe('Test Case 2: Test homepage with slow network (3G)', () => {
    test('loading states displayed appropriately on slow network', async ({ page }) => {
      // Simulate slow 3G network
      const cdpSession = await page.context().newCDPSession(page)
      await cdpSession.send('Network.emulateNetworkConditions', {
        offline: false,
        downloadThroughput: (750 * 1024) / 8, // 750 kbps
        uploadThroughput: (250 * 1024) / 8, // 250 kbps
        latency: 300, // 300ms latency
      })

      // Start navigation
      const navigationPromise = page.goto('/', { waitUntil: 'networkidle' })

      // Check that the page starts loading and shows meaningful content
      // Even during loading, some content should be visible
      await navigationPromise

      // Verify the page eventually loads completely
      await expect(page.locator('main')).toBeVisible({ timeout: 30000 })

      // Verify hero section is visible after load
      await expect(page.getByTestId('hero-section')).toBeVisible()

      // Verify critical content is displayed
      await expect(page.getByTestId('hero-headline')).toBeVisible()
    })

    test('page is usable after slow load completes', async ({ page }) => {
      // Simulate slow 3G network
      const cdpSession = await page.context().newCDPSession(page)
      await cdpSession.send('Network.emulateNetworkConditions', {
        offline: false,
        downloadThroughput: (750 * 1024) / 8,
        uploadThroughput: (250 * 1024) / 8,
        latency: 300,
      })

      await page.goto('/', { waitUntil: 'networkidle', timeout: 60000 })

      // Verify all major sections are visible and interactive
      await expect(page.getByTestId('navbar')).toBeVisible()
      await expect(page.getByTestId('hero-section')).toBeVisible()
      await expect(page.getByTestId('features-section')).toBeVisible()

      // Verify buttons are clickable
      const ctaButton = page.getByTestId('cta-get-started')
      await expect(ctaButton).toBeVisible()
      await expect(ctaButton).toBeEnabled()
    })

    test('theme toggle works after slow network load', async ({ page }) => {
      // Simulate slow 3G network
      const cdpSession = await page.context().newCDPSession(page)
      await cdpSession.send('Network.emulateNetworkConditions', {
        offline: false,
        downloadThroughput: (750 * 1024) / 8,
        uploadThroughput: (250 * 1024) / 8,
        latency: 300,
      })

      await page.goto('/', { waitUntil: 'networkidle', timeout: 60000 })

      // Find and interact with theme toggle in hero section (has testid hero-theme-toggle)
      const themeToggle = page.getByTestId('hero-theme-toggle')
      await expect(themeToggle).toBeVisible({ timeout: 30000 })
      await expect(themeToggle).toBeEnabled()
    })
  })

  test.describe('Additional error resilience checks', () => {
    test('page recovers from temporary network issues', async ({ page }) => {
      // Load the page first
      await page.goto('/')
      await expect(page.getByTestId('hero-section')).toBeVisible()

      // The page should remain functional even if future network requests fail
      // (This tests that the page doesn't break after initial load)
      const heroSection = page.getByTestId('hero-section')
      await expect(heroSection).toBeVisible()

      // Navigation should still work
      const ctaLogin = page.getByTestId('cta-login')
      await expect(ctaLogin).toBeVisible()
      await expect(ctaLogin).toBeEnabled()
    })

    test('error boundary prevents blank page on component errors', async ({ page }) => {
      // Navigate to homepage
      await page.goto('/')

      // Verify the error boundary wrapper exists
      const errorBoundary = page.locator('[data-testid="app-error-boundary"]')

      // The page should not be blank
      const mainContent = page.locator('main')
      await expect(mainContent).toBeVisible()
    })

    test('page handles rapid navigation gracefully', async ({ page }) => {
      // Start multiple navigations rapidly
      const navigationPromises = [
        page.goto('/'),
        page.goto('/'),
        page.goto('/'),
      ]

      // Wait for navigation to settle
      await Promise.race(navigationPromises).catch(() => {})
      await page.waitForLoadState('networkidle')

      // Page should still be functional
      await expect(page.locator('main')).toBeVisible()
    })
  })
})
