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
