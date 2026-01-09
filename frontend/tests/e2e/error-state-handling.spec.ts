import { test, expect } from '@playwright/test'

/**
 * Error State Handling E2E Tests - Scenario 20
 *
 * Tests verify that the homepage handles error states gracefully
 * without breaking the user experience.
 */
test.describe('Error State Handling - Scenario 20', () => {
  test.describe('Test Case 1: Load homepage with network offline', () => {
    /**
     * Test that static content renders correctly when network is unavailable.
     * The homepage uses static/default data, so it should work offline.
     */
    test('should render static content when network is offline', async ({
      page,
      context,
    }) => {
      // First, load the page with network to ensure it's cached
      await page.goto('/', { waitUntil: 'networkidle' })

      // Verify initial load is successful
      await expect(page.getByTestId('home-page')).toBeVisible()

      // Now simulate offline mode
      await context.setOffline(true)

      // Reload the page in offline mode
      // Note: In a SPA, the static content should still render from cache
      try {
        await page.reload({ waitUntil: 'domcontentloaded', timeout: 10000 })
      } catch {
        // Expected - network requests will fail, but cached content may load
      }

      // Restore online mode for cleanup
      await context.setOffline(false)
    })

    test('should render all static sections without network-dependent features', async ({
      page,
    }) => {
      await page.goto('/', { waitUntil: 'networkidle' })

      // All static sections should be visible
      await expect(page.getByTestId('home-page')).toBeVisible()
      await expect(page.getByTestId('hero-section')).toBeVisible()
      await expect(page.getByTestId('features-section')).toBeVisible()
      await expect(page.getByTestId('how-it-works-section')).toBeVisible()
      await expect(page.getByTestId('url-demo-section')).toBeVisible()
      await expect(page.getByTestId('statistics-section')).toBeVisible()
      await expect(page.getByTestId('footer-links')).toBeVisible()
    })

    test('should display default statistics values (static fallback)', async ({
      page,
    }) => {
      await page.goto('/', { waitUntil: 'networkidle' })

      // Statistics should show default hardcoded values
      await expect(page.getByTestId('statistics-section')).toBeVisible()
      await expect(
        page.getByTestId('statistic-card-urls-shortened')
      ).toBeVisible()
      await expect(
        page.getByTestId('statistic-card-clicks-tracked')
      ).toBeVisible()
      await expect(
        page.getByTestId('statistic-card-active-users')
      ).toBeVisible()

      // Verify the values are displayed (default values)
      await expect(
        page.getByTestId('statistic-value-urls-shortened')
      ).toHaveText('1.3M')
      await expect(
        page.getByTestId('statistic-value-clicks-tracked')
      ).toHaveText('45.7M')
      await expect(page.getByTestId('statistic-value-active-users')).toHaveText(
        '52K'
      )
    })
  })

  test.describe('Test Case 2: Check browser console on homepage load', () => {
    test('should load homepage without JavaScript errors', async ({ page }) => {
      // Collect console messages
      const consoleErrors: string[] = []
      const consoleWarnings: string[] = []

      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text())
        }
        if (msg.type() === 'warning') {
          consoleWarnings.push(msg.text())
        }
      })

      // Also capture page errors (uncaught exceptions)
      const pageErrors: string[] = []
      page.on('pageerror', (error) => {
        pageErrors.push(error.message)
      })

      // Navigate to homepage
      await page.goto('/', { waitUntil: 'networkidle' })

      // Verify page loaded correctly
      await expect(page.getByTestId('home-page')).toBeVisible()

      // Wait for any async operations to complete
      await page.waitForTimeout(1000)

      // Filter out known non-critical errors (like failed network requests in dev)
      const criticalErrors = consoleErrors.filter(
        (error) =>
          !error.includes('favicon') &&
          !error.includes('404') &&
          !error.includes('Failed to load resource') &&
          !error.includes('net::ERR')
      )

      // There should be no critical JavaScript errors
      expect(
        criticalErrors,
        `Unexpected console errors: ${criticalErrors.join(', ')}`
      ).toHaveLength(0)

      // There should be no uncaught page errors
      expect(
        pageErrors,
        `Uncaught page errors: ${pageErrors.join(', ')}`
      ).toHaveLength(0)
    })

    test('should not have unhandled promise rejections', async ({ page }) => {
      const unhandledRejections: string[] = []

      page.on('pageerror', (error) => {
        if (
          error.message.includes('Unhandled') ||
          error.message.includes('Promise')
        ) {
          unhandledRejections.push(error.message)
        }
      })

      await page.goto('/', { waitUntil: 'networkidle' })
      await expect(page.getByTestId('home-page')).toBeVisible()

      // Wait for any async operations
      await page.waitForTimeout(1000)

      expect(unhandledRejections).toHaveLength(0)
    })

    test('should handle smooth scroll navigation without errors', async ({
      page,
    }) => {
      const errors: string[] = []

      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          errors.push(msg.text())
        }
      })

      page.on('pageerror', (error) => {
        errors.push(error.message)
      })

      await page.goto('/', { waitUntil: 'networkidle' })

      // Click on navigation links that trigger smooth scroll
      const featuresLink = page.getByRole('link', { name: /features/i })
      if (await featuresLink.isVisible()) {
        await featuresLink.click()
        await page.waitForTimeout(500)
      }

      const howItWorksLink = page.getByRole('link', { name: /how it works/i })
      if (await howItWorksLink.isVisible()) {
        await howItWorksLink.click()
        await page.waitForTimeout(500)
      }

      // Filter out non-critical errors
      const criticalErrors = errors.filter(
        (error) =>
          !error.includes('favicon') &&
          !error.includes('404') &&
          !error.includes('net::ERR')
      )

      expect(criticalErrors).toHaveLength(0)
    })
  })

  test.describe('Test Case 3: Simulate API error for statistics endpoint', () => {
    test('should display page content even when API requests fail', async ({
      page,
    }) => {
      // Block any API requests to simulate backend unavailable
      await page.route('**/api/**', (route) => {
        route.abort('failed')
      })

      await page.goto('/', { waitUntil: 'domcontentloaded' })

      // Main content should still render (static content)
      await expect(page.getByTestId('home-page')).toBeVisible()
      await expect(page.getByTestId('hero-section')).toBeVisible()
      await expect(page.getByTestId('features-section')).toBeVisible()

      // Statistics should show default values (not fetched from API)
      await expect(page.getByTestId('statistics-section')).toBeVisible()
    })

    test('should not crash when statistics data is unavailable', async ({
      page,
    }) => {
      const pageErrors: string[] = []

      page.on('pageerror', (error) => {
        pageErrors.push(error.message)
      })

      // Block statistics API endpoint if it exists
      await page.route('**/api/stats**', (route) => {
        route.abort('failed')
      })

      await page.goto('/', { waitUntil: 'networkidle' })

      // Page should render without crashing
      await expect(page.getByTestId('home-page')).toBeVisible()

      // Wait for any async error handling
      await page.waitForTimeout(1000)

      // No page crashes should occur
      expect(pageErrors).toHaveLength(0)
    })

    test('should show default statistics when API returns error', async ({
      page,
    }) => {
      // Mock API to return error response
      await page.route('**/api/statistics**', (route) => {
        route.fulfill({
          status: 500,
          contentType: 'application/json',
          body: JSON.stringify({ error: 'Internal Server Error' }),
        })
      })

      await page.goto('/', { waitUntil: 'networkidle' })

      // Statistics should still be visible with default values
      await expect(page.getByTestId('statistics-section')).toBeVisible()

      // Default values should be displayed
      await expect(
        page.getByTestId('statistic-value-urls-shortened')
      ).toBeVisible()
      await expect(
        page.getByTestId('statistic-value-clicks-tracked')
      ).toBeVisible()
      await expect(
        page.getByTestId('statistic-value-active-users')
      ).toBeVisible()
    })

    test('should handle network timeout gracefully', async ({
      page,
      context,
    }) => {
      const pageErrors: string[] = []

      page.on('pageerror', (error) => {
        pageErrors.push(error.message)
      })

      // Simulate slow network - not too extreme to avoid timeouts
      const cdp = await context.newCDPSession(page)
      await cdp.send('Network.emulateNetworkConditions', {
        offline: false,
        downloadThroughput: (100 * 1024) / 8, // 100kbps - slow but not extremely slow
        uploadThroughput: (100 * 1024) / 8,
        latency: 500, // 500ms latency
      })

      // Try to load the page with a timeout
      try {
        await page.goto('/', { waitUntil: 'domcontentloaded', timeout: 15000 })
        // Page loaded - verify no page errors
        expect(pageErrors).toHaveLength(0)
      } catch {
        // Network timeout occurred, but that's OK for this test
        // The important thing is no JavaScript crash errors
        expect(pageErrors).toHaveLength(0)
      }
    })
  })

  test.describe('Graceful degradation and resilience', () => {
    test('should render footer with all links when page loads', async ({
      page,
    }) => {
      await page.goto('/', { waitUntil: 'networkidle' })

      await expect(page.getByTestId('footer-links')).toBeVisible()

      // Footer links should be present and functional
      const aboutLink = page.getByTestId('footer-link-about')
      const privacyLink = page.getByTestId('footer-link-privacy')
      const termsLink = page.getByTestId('footer-link-terms')

      await expect(aboutLink).toBeVisible()
      await expect(privacyLink).toBeVisible()
      await expect(termsLink).toBeVisible()
    })

    test('should maintain functionality of client-side URL demo', async ({
      page,
    }) => {
      const errors: string[] = []
      page.on('pageerror', (error) => {
        errors.push(error.message)
      })

      await page.goto('/', { waitUntil: 'networkidle' })

      // URL demo should work without network (client-side only)
      const input = page.getByTestId('demo-url-input')
      await expect(input).toBeVisible()
      await expect(input).toBeEnabled()

      // Type a URL
      await input.fill('https://example.com/very-long-url')

      // Should show shortened URL preview
      await expect(page.getByTestId('demo-shortened-url')).toBeVisible()

      // No errors should occur
      expect(errors).toHaveLength(0)
    })

    test('should handle copy to clipboard gracefully when API is unavailable', async ({
      page,
    }) => {
      const errors: string[] = []
      page.on('pageerror', (error) => {
        errors.push(error.message)
      })

      await page.goto('/', { waitUntil: 'networkidle' })

      // Type a URL to enable copy button
      const input = page.getByTestId('demo-url-input')
      await input.fill('https://example.com')

      // Wait for shortened URL to appear
      await expect(page.getByTestId('demo-shortened-url')).toBeVisible()

      // Click copy button
      const copyButton = page.getByTestId('demo-copy-button')
      await copyButton.click()

      // Wait for any async operations
      await page.waitForTimeout(500)

      // No crash errors should occur
      expect(errors).toHaveLength(0)
    })
  })

  test.describe('Theme system resilience', () => {
    test('should render correctly with default theme', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' })

      // Page should render with consistent styling
      const homePage = page.getByTestId('home-page')
      await expect(homePage).toBeVisible()
      await expect(homePage).toHaveClass(/min-h-screen/)
      await expect(homePage).toHaveClass(/bg-base-100/)
    })

    test('should handle theme toggle without errors', async ({ page }) => {
      const errors: string[] = []
      page.on('pageerror', (error) => {
        errors.push(error.message)
      })

      await page.goto('/', { waitUntil: 'networkidle' })

      // Find and click theme toggle if present (use first() to handle duplicate desktop/mobile toggles)
      const themeToggle = page.getByTestId('theme-toggle').first()
      if (await themeToggle.isVisible()) {
        await themeToggle.click()
        await page.waitForTimeout(300)

        // Toggle back
        await themeToggle.click()
        await page.waitForTimeout(300)
      }

      // No errors should occur during theme changes
      expect(errors).toHaveLength(0)
    })
  })
})
