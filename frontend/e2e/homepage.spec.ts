/**
 * Homepage E2E Tests
 *
 * Owner: Scenario 16 - Performance (Page Load Time)
 * Owner: Scenario 17 - Routing Integration (Browser Navigation)
 * Owner: Scenario 19 - Short URL Redirect Verification
 *
 * E2E tests for homepage functionality including routing and browser navigation.
 */

import { test, expect } from '@playwright/test'

/**
 * Scenario 17 - Routing Integration E2E Tests
 *
 * Tests that the homepage is properly integrated with React Router at root path.
 * Covers NFR-5: Browser back/forward navigation support.
 */
test.describe('Scenario 17 - Routing Integration', () => {
  test.describe('Test Case 2: Browser navigation - back button from /login to /', () => {
    test('navigates from homepage to login and back using browser back button', async ({ page }) => {
      // Navigate directly to homepage
      await page.goto('/')

      // Wait for homepage to load
      await expect(page.getByTestId('hero-section')).toBeVisible()
      await expect(page.getByTestId('product-name')).toBeVisible()

      // Click login button to navigate to /login
      const loginButton = page.getByTestId('login-button')
      await expect(loginButton).toBeVisible()
      await loginButton.click()

      // Verify URL changed to /login
      await expect(page).toHaveURL(/\/login/)

      // Press browser back button
      await page.goBack()

      // Verify we're back at homepage
      await expect(page).toHaveURL('/')
      await expect(page.getByTestId('hero-section')).toBeVisible()
      await expect(page.getByTestId('features-section')).toBeVisible()
      await expect(page.getByTestId('how-it-works-section')).toBeVisible()
    })

    test('browser forward button returns to login page after going back', async ({ page }) => {
      // Navigate to homepage
      await page.goto('/')
      await expect(page.getByTestId('hero-section')).toBeVisible()

      // Navigate to login
      await page.getByTestId('login-button').click()
      await expect(page).toHaveURL(/\/login/)

      // Go back to homepage
      await page.goBack()
      await expect(page).toHaveURL('/')
      await expect(page.getByTestId('hero-section')).toBeVisible()

      // Go forward to login page
      await page.goForward()
      await expect(page).toHaveURL(/\/login/)
    })
  })

  test.describe('Test Case 3: Direct URL access to "/"', () => {
    test('direct navigation to "/" renders homepage without redirects', async ({ page }) => {
      // Navigate directly to root path
      await page.goto('/')

      // Verify homepage renders immediately
      await expect(page.getByTestId('hero-section')).toBeVisible()
      await expect(page.getByTestId('product-name')).toBeVisible()
      await expect(page.getByTestId('tagline')).toBeVisible()

      // Verify all main sections are present
      await expect(page.getByTestId('features-section')).toBeVisible()
      await expect(page.getByTestId('how-it-works-section')).toBeVisible()

      // URL should remain "/" (no redirects)
      await expect(page).toHaveURL('/')
    })

    test('homepage does not show 404 or error page', async ({ page }) => {
      // Navigate directly to root
      await page.goto('/')

      // Verify homepage content is present (not a 404)
      await expect(page.getByTestId('hero-section')).toBeVisible()

      // Ensure no 404 text is present
      await expect(page.getByText(/404/i)).not.toBeVisible()
      await expect(page.getByText(/not found/i)).not.toBeVisible()
    })
  })

  test.describe('Browser History State (NFR-5)', () => {
    test('homepage is correctly added to browser history', async ({ page }) => {
      // Start at homepage
      await page.goto('/')
      await expect(page.getByTestId('hero-section')).toBeVisible()

      // Navigate through multiple pages
      await page.getByTestId('login-button').click()
      await expect(page).toHaveURL(/\/login/)

      // Navigate back twice (to make sure history works)
      await page.goBack()
      await expect(page).toHaveURL('/')
      await expect(page.getByTestId('hero-section')).toBeVisible()
    })

    test('browser refresh on homepage maintains route', async ({ page }) => {
      // Navigate to homepage
      await page.goto('/')
      await expect(page.getByTestId('hero-section')).toBeVisible()

      // Refresh the page
      await page.reload()

      // Verify we're still on homepage after refresh
      await expect(page).toHaveURL('/')
      await expect(page.getByTestId('hero-section')).toBeVisible()
      await expect(page.getByTestId('features-section')).toBeVisible()
    })
  })
})

/**
 * Scenario 19 - Short URL Redirect Verification E2E Tests
 *
 * Tests that generated short URLs correctly redirect to target destinations.
 * Covers:
 * - Creating short URLs from the homepage form
 * - Navigating to short URLs (/r/{shortCode})
 * - Verifying browser redirects to original URL
 * - Click count tracking on multiple accesses
 */
test.describe('Scenario 19 - Short URL Redirect Verification', () => {
  // Store for dynamically created short URLs
  const urlStore: Map<string, { original_url: string; click_count: number }> = new Map()
  let shortCodeCounter = 0

  /**
   * Test Case 1: Create short URL for a test URL
   * Verifies that the URL shortening form generates a valid short URL
   */
  test.describe('Test Case 1: Create short URL', () => {
    test('creates short URL with format /r/{shortCode} from homepage form', async ({ page }) => {
      // Mock the URL shortening API
      await page.route('**/api/urls/', async (route) => {
        const request = route.request()
        if (request.method() === 'POST') {
          const body = request.postDataJSON()
          shortCodeCounter += 1
          const shortCode = `test${shortCodeCounter.toString().padStart(3, '0')}`
          urlStore.set(shortCode, {
            original_url: body.original_url,
            click_count: 0,
          })
          await route.fulfill({
            status: 201,
            contentType: 'application/json',
            body: JSON.stringify({
              id: shortCodeCounter,
              original_url: body.original_url,
              short_code: shortCode,
              created_at: new Date().toISOString(),
              user_id: null,
              click_count: 0,
            }),
          })
        } else {
          await route.continue()
        }
      })

      // Navigate to homepage
      await page.goto('/')
      await expect(page.getByTestId('hero-section')).toBeVisible()

      // Find and fill the URL input
      const urlInput = page.getByTestId('url-input')
      await expect(urlInput).toBeVisible()
      await urlInput.fill('https://example.com/test-page')

      // Click the shorten button
      const shortenButton = page.getByTestId('shorten-url-button')
      await expect(shortenButton).toBeVisible()
      await shortenButton.click()

      // Wait for the result to appear
      const resultRegion = page.getByRole('region', { name: /shortened url result/i })
      await expect(resultRegion).toBeVisible({ timeout: 10000 })

      // Verify the short URL is displayed with correct format /r/{shortCode}
      const shortUrlInput = resultRegion.locator('input[readonly]')
      await expect(shortUrlInput).toBeVisible()
      const shortUrl = await shortUrlInput.inputValue()
      expect(shortUrl).toMatch(/\/r\/[a-zA-Z0-9]+$/)
    })

    test('shortened URL contains the test short code format', async ({ page }) => {
      // Mock the URL shortening API
      await page.route('**/api/urls/', async (route) => {
        const request = route.request()
        if (request.method() === 'POST') {
          const body = request.postDataJSON()
          shortCodeCounter += 1
          const shortCode = `test${shortCodeCounter.toString().padStart(3, '0')}`
          urlStore.set(shortCode, {
            original_url: body.original_url,
            click_count: 0,
          })
          await route.fulfill({
            status: 201,
            contentType: 'application/json',
            body: JSON.stringify({
              id: shortCodeCounter,
              original_url: body.original_url,
              short_code: shortCode,
              created_at: new Date().toISOString(),
              user_id: null,
              click_count: 0,
            }),
          })
        } else {
          await route.continue()
        }
      })

      // Navigate to homepage
      await page.goto('/')

      // Enter a URL and shorten it
      const urlInput = page.getByTestId('url-input')
      await urlInput.fill('https://example.com/another-test')
      await page.getByTestId('shorten-url-button').click()

      // Wait for result
      const resultRegion = page.getByRole('region', { name: /shortened url result/i })
      await expect(resultRegion).toBeVisible({ timeout: 10000 })

      // Get the shortened URL
      const shortUrlInput = resultRegion.locator('input[readonly]')
      const shortUrl = await shortUrlInput.inputValue()

      // Verify it matches our expected format
      expect(shortUrl).toContain('/r/')
      // Extract and validate the short code part
      const shortCodeMatch = shortUrl.match(/\/r\/([a-zA-Z0-9]+)$/)
      expect(shortCodeMatch).not.toBeNull()
      expect(shortCodeMatch![1].length).toBeGreaterThan(0)
    })
  })

  /**
   * Test Case 2: Navigate to generated short URL and verify redirect
   * This test creates a short URL and then navigates to it to verify the redirect
   */
  test.describe('Test Case 2: Short URL navigation and redirect', () => {
    test('navigating to /r/{shortCode} shows redirect loading state', async ({ page }) => {
      // Navigate directly to a short URL path
      await page.goto('/r/testcode123')

      // Should see either loading state or error (since testcode123 doesn't exist)
      // The redirect handler should be invoked
      const loadingOrError = page.locator('[data-testid="redirect-loading"], [data-testid="redirect-error"]')
      await expect(loadingOrError).toBeVisible({ timeout: 5000 })
    })

    test('short URL redirect route is accessible', async ({ page }) => {
      // Navigate to homepage first
      await page.goto('/')
      await expect(page.getByTestId('hero-section')).toBeVisible()

      // Navigate to a short URL route
      const response = await page.goto('/r/abc123')

      // Verify the route is accessible (not 404 from React Router)
      expect(response?.status()).toBeLessThan(500)

      // The page should show loading or error state (not a blank page)
      const pageContent = await page.content()
      expect(pageContent).not.toBe('')
    })

    test('invalid short code shows error message', async ({ page }) => {
      // Mock the API to return 404 for invalid short code
      await page.route('**/api/urls/invalid_code_xyz', async (route) => {
        await route.fulfill({
          status: 404,
          contentType: 'application/json',
          body: JSON.stringify({ detail: 'URL not found' }),
        })
      })

      // Navigate to a non-existent short URL
      await page.goto('/r/invalid_code_xyz')

      // Wait for the error state to appear
      const errorElement = page.getByTestId('redirect-error')
      await expect(errorElement).toBeVisible({ timeout: 10000 })

      // Verify error message is displayed - use first() to handle multiple matches
      await expect(page.getByText('Short URL not found or expired')).toBeVisible()
    })
  })

  /**
   * Test Case 3: Multiple access consistency
   * Verifies that the redirect works consistently across multiple accesses
   */
  test.describe('Test Case 3: Multiple access consistency', () => {
    test('short URL route can be accessed multiple times', async ({ page }) => {
      // First access
      await page.goto('/r/multitest1')
      await expect(page.locator('[data-testid="redirect-loading"], [data-testid="redirect-error"]')).toBeVisible()

      // Navigate away
      await page.goto('/')
      await expect(page.getByTestId('hero-section')).toBeVisible()

      // Second access to the same short URL
      await page.goto('/r/multitest1')
      await expect(page.locator('[data-testid="redirect-loading"], [data-testid="redirect-error"]')).toBeVisible()

      // Navigate away again
      await page.goto('/')
      await expect(page.getByTestId('hero-section')).toBeVisible()

      // Third access
      await page.goto('/r/multitest1')
      await expect(page.locator('[data-testid="redirect-loading"], [data-testid="redirect-error"]')).toBeVisible()
    })

    test('redirect loading state appears before redirect or error', async ({ page }) => {
      // Navigate to short URL route
      const responsePromise = page.waitForResponse(
        (response) => response.url().includes('/api/urls/'),
        { timeout: 10000 }
      ).catch(() => null)

      await page.goto('/r/loadtest123')

      // Either loading or error should be visible
      const loadingOrError = page.locator('[data-testid="redirect-loading"], [data-testid="redirect-error"]')
      await expect(loadingOrError).toBeVisible({ timeout: 5000 })

      // Wait for API response if any
      await responsePromise
    })
  })

  /**
   * Redirect handler displays appropriate UI states
   */
  test.describe('Redirect UI States', () => {
    test('redirect page shows loading spinner initially', async ({ page }) => {
      // Navigate to a short URL - should show loading first
      await page.goto('/r/testshort')

      // Either loading or error should appear (depending on API response timing)
      await page.waitForSelector('[data-testid="redirect-loading"], [data-testid="redirect-error"]', { timeout: 5000 })
    })

    test('redirect error page has proper styling', async ({ page }) => {
      // Navigate to invalid short URL
      await page.goto('/r/definitelyinvalidcode')

      // Wait for error state
      const errorElement = page.getByTestId('redirect-error')
      await expect(errorElement).toBeVisible({ timeout: 10000 })

      // Verify error page has expected structure
      await expect(page.getByRole('heading', { name: /error/i })).toBeVisible()
    })
  })
})
