/**
 * E2E Navigation Tests
 *
 * Scenario 3: Navigation to Registration
 * - Test Case 1: Click primary CTA button navigates to /register page
 * - Test Case 2: Navigation occurs immediately with visual feedback
 * - Test Case 3: CTA button is keyboard accessible and has proper ARIA labels
 *
 * Scenario 4: Navigation to Login
 * - Test Case 1: Click login link/button navigates to /login page
 * - Test Case 2: Login form is displayed and ready for user input
 */

import { test, expect } from '@playwright/test'

test.describe('Navigation to Registration', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/')
    // Wait for the page to be ready
    await page.waitForLoadState('domcontentloaded')
  })

  /**
   * Test Case 1: Click primary CTA button navigates to /register page
   * Input: Click primary CTA button
   * Expected: User is navigated to /register page
   */
  test('clicking primary CTA navigates to registration page', async ({ page }) => {
    // Find the primary CTA button by its text content
    const primaryCta = page.getByRole('button', { name: /get started/i })

    // Verify button is visible and enabled
    await expect(primaryCta).toBeVisible()
    await expect(primaryCta).toBeEnabled()

    // Click the primary CTA
    await primaryCta.click()

    // Verify navigation to /register
    await expect(page).toHaveURL(/\/register/)

    // Verify registration page content is loaded
    await expect(page.getByRole('heading', { name: /register/i })).toBeVisible()
  })

  /**
   * Test Case 2: Navigation occurs immediately with visual feedback
   * Input: Verify transition feedback
   * Expected: Navigation occurs immediately with visual feedback
   */
  test('navigation occurs immediately with visual feedback', async ({ page }) => {
    const primaryCta = page.getByRole('button', { name: /get started/i })

    // Measure the time it takes to navigate
    const startTime = Date.now()

    // Click the primary CTA and wait for navigation
    await primaryCta.click()
    await page.waitForURL(/\/register/)

    const navigationTime = Date.now() - startTime

    // Navigation should occur within 2 seconds (generous threshold for CI)
    expect(navigationTime).toBeLessThan(2000)

    // Verify the page has loaded (visual feedback)
    await expect(page.getByRole('heading')).toBeVisible()
  })

  /**
   * Additional test: Verify CTA button state feedback on interaction
   */
  test('primary CTA shows interactive states', async ({ page }) => {
    const primaryCta = page.getByRole('button', { name: /get started/i })

    // Button should be visible and have proper styling
    await expect(primaryCta).toBeVisible()

    // Button should contain appropriate text
    const buttonText = await primaryCta.textContent()
    expect(buttonText?.toLowerCase()).toMatch(/get started|sign up|register|start/i)

    // Button should have primary styling classes
    await expect(primaryCta).toHaveClass(/bg-primary/)
  })

  /**
   * Test: Multiple clicks don't cause issues
   */
  test('navigation works correctly on subsequent visits', async ({ page }) => {
    const primaryCta = page.getByRole('button', { name: /get started/i })

    // Click to navigate to register
    await primaryCta.click()
    await expect(page).toHaveURL(/\/register/)

    // Navigate back to home
    await page.goto('/')
    await page.waitForLoadState('domcontentloaded')

    // Click again - should still work
    await page.getByRole('button', { name: /get started/i }).click()
    await expect(page).toHaveURL(/\/register/)
  })

  /**
   * Test Case 3: CTA button is keyboard accessible and has proper ARIA labels
   * Input: Check button accessibility
   * Expected: CTA button is keyboard accessible and has proper ARIA labels
   */
  test('primary CTA is keyboard accessible', async ({ page }) => {
    const primaryCta = page.getByRole('button', { name: /get started/i })

    // Verify button is a proper button element (inherently keyboard accessible)
    await expect(primaryCta).toBeVisible()

    // Verify button is focusable via keyboard
    await primaryCta.focus()
    await expect(primaryCta).toBeFocused()

    // Verify button can be activated via keyboard (Enter key)
    await page.keyboard.press('Enter')
    await expect(page).toHaveURL(/\/register/)
  })

  /**
   * Test: CTA button is accessible via Tab navigation
   */
  test('primary CTA is reachable via Tab key navigation', async ({ page }) => {
    // Focus on the body to start tab navigation
    await page.keyboard.press('Tab')

    // Tab through focusable elements until we reach the primary CTA
    // This tests that the button is in the tab order
    let foundCta = false
    let maxTabs = 15 // Safety limit

    while (!foundCta && maxTabs > 0) {
      const focusedElement = page.locator(':focus')
      const text = await focusedElement.textContent()

      if (text?.toLowerCase().includes('get started')) {
        foundCta = true
        break
      }

      await page.keyboard.press('Tab')
      maxTabs--
    }

    expect(foundCta).toBe(true)

    // Verify we can activate the button with Space key as well
    await page.keyboard.press('Space')
    await expect(page).toHaveURL(/\/register/)
  })

  /**
   * Test: Primary CTA meets minimum touch target size (44x44 pixels)
   */
  test('primary CTA meets minimum touch target size', async ({ page }) => {
    const primaryCta = page.getByRole('button', { name: /get started/i })

    // Get button dimensions
    const boundingBox = await primaryCta.boundingBox()

    expect(boundingBox).not.toBeNull()
    if (boundingBox) {
      // WCAG 2.1 AAA recommends 44x44 minimum touch target
      expect(boundingBox.width).toBeGreaterThanOrEqual(44)
      expect(boundingBox.height).toBeGreaterThanOrEqual(44)
    }
  })
})

/**
 * Scenario 4: Navigation to Login
 * Tests that clicking the secondary CTA navigates existing users to the login page
 */
test.describe('Navigation to Login', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/')
    // Wait for the hero section to be visible
    await page.waitForSelector('[data-testid="hero-section"]', { state: 'visible' })
  })

  /**
   * Test Case 1: Click login link/button navigates to /login page
   * Input: Click login link/button
   * Expected: User is navigated to /login page
   */
  test('clicking login CTA navigates to login page', async ({ page }) => {
    // Find the login CTA button
    const loginCta = page.getByTestId('hero-login-cta')

    // Verify button is visible and enabled
    await expect(loginCta).toBeVisible()
    await expect(loginCta).toBeEnabled()

    // Click the login CTA
    await loginCta.click()

    // Verify navigation to /login
    await expect(page).toHaveURL(/\/login/)
  })

  /**
   * Test Case 2: Login form is displayed and ready for user input
   * Input: Verify login form ready
   * Expected: Login form is displayed and ready for user input
   */
  test('login form is displayed and ready for user input', async ({ page }) => {
    // Click the login CTA
    const loginCta = page.getByTestId('hero-login-cta')
    await loginCta.click()

    // Wait for navigation to complete
    await expect(page).toHaveURL(/\/login/)

    // Verify login page is displayed
    await expect(page.getByTestId('login-page')).toBeVisible()

    // Verify login heading is visible
    await expect(page.getByTestId('login-heading')).toBeVisible()
    await expect(page.getByTestId('login-heading')).toHaveText('Login')

    // Verify login form is present
    const loginForm = page.getByTestId('login-form')
    await expect(loginForm).toBeVisible()

    // Verify email input is ready for user input
    const emailInput = page.getByTestId('login-email-input')
    await expect(emailInput).toBeVisible()
    await expect(emailInput).toBeEnabled()
    await expect(emailInput).toHaveAttribute('type', 'email')

    // Verify password input is ready for user input
    const passwordInput = page.getByTestId('login-password-input')
    await expect(passwordInput).toBeVisible()
    await expect(passwordInput).toBeEnabled()
    await expect(passwordInput).toHaveAttribute('type', 'password')

    // Verify submit button is present and enabled
    const submitButton = page.getByTestId('login-submit-button')
    await expect(submitButton).toBeVisible()
    await expect(submitButton).toBeEnabled()
  })

  /**
   * Test: Login CTA button has proper styling (secondary/outline variant)
   */
  test('login CTA shows secondary styling', async ({ page }) => {
    const loginCta = page.getByTestId('hero-login-cta')

    // Button should be visible
    await expect(loginCta).toBeVisible()

    // Button should contain appropriate text
    const buttonText = await loginCta.textContent()
    expect(buttonText?.toLowerCase()).toMatch(/log in|login|sign in/i)

    // Button should have outline/secondary styling (border)
    await expect(loginCta).toHaveClass(/border/)
  })

  /**
   * Test: Navigation occurs immediately (within acceptable threshold)
   */
  test('login navigation occurs immediately', async ({ page }) => {
    const loginCta = page.getByTestId('hero-login-cta')

    // Measure the time it takes to navigate
    const startTime = Date.now()

    // Click the login CTA and wait for navigation
    await loginCta.click()
    await page.waitForURL(/\/login/)

    const navigationTime = Date.now() - startTime

    // Navigation should occur within 2 seconds (generous threshold for CI)
    expect(navigationTime).toBeLessThan(2000)
  })

  /**
   * Test: Login CTA is keyboard accessible
   */
  test('login CTA is keyboard accessible with proper ARIA labels', async ({ page }) => {
    const loginCta = page.getByTestId('hero-login-cta')

    // Verify button has proper ARIA label
    const ariaLabel = await loginCta.getAttribute('aria-label')
    expect(ariaLabel).toBeTruthy()
    expect(ariaLabel?.toLowerCase()).toMatch(/log in|login|account/i)

    // Verify button is focusable via keyboard
    await loginCta.focus()
    await expect(loginCta).toBeFocused()

    // Verify button can be activated via keyboard (Enter key)
    await page.keyboard.press('Enter')
    await expect(page).toHaveURL(/\/login/)
  })

  /**
   * Test: Login CTA is reachable via Tab key navigation
   */
  test('login CTA is reachable via Tab key navigation', async ({ page }) => {
    // Focus on the body to start tab navigation
    await page.keyboard.press('Tab')

    // Tab through focusable elements until we reach the login CTA
    let foundCta = false
    let maxTabs = 15 // Safety limit

    while (!foundCta && maxTabs > 0) {
      const focusedElement = page.locator(':focus')
      const testId = await focusedElement.getAttribute('data-testid')

      if (testId === 'hero-login-cta') {
        foundCta = true
        break
      }

      await page.keyboard.press('Tab')
      maxTabs--
    }

    expect(foundCta).toBe(true)

    // Verify we can activate the button with Space key as well
    await page.keyboard.press('Space')
    await expect(page).toHaveURL(/\/login/)
  })

  /**
   * Test: Login CTA meets minimum touch target size (44x44 pixels)
   */
  test('login CTA meets minimum touch target size', async ({ page }) => {
    const loginCta = page.getByTestId('hero-login-cta')

    // Get button dimensions
    const boundingBox = await loginCta.boundingBox()

    expect(boundingBox).not.toBeNull()
    if (boundingBox) {
      // WCAG 2.1 AAA recommends 44x44 minimum touch target
      expect(boundingBox.width).toBeGreaterThanOrEqual(44)
      expect(boundingBox.height).toBeGreaterThanOrEqual(44)
    }
  })

  /**
   * Test: Navigation works correctly on subsequent visits
   */
  test('login navigation works correctly on subsequent visits', async ({ page }) => {
    const loginCta = page.getByTestId('hero-login-cta')

    // Click to navigate to login
    await loginCta.click()
    await expect(page).toHaveURL(/\/login/)

    // Navigate back to home
    await page.goto('/')
    await page.waitForSelector('[data-testid="hero-section"]', { state: 'visible' })

    // Click again - should still work
    await page.getByTestId('hero-login-cta').click()
    await expect(page).toHaveURL(/\/login/)
  })

  /**
   * Test: Login form accepts input
   */
  test('login form accepts user input', async ({ page }) => {
    // Navigate to login page
    await page.getByTestId('hero-login-cta').click()
    await expect(page).toHaveURL(/\/login/)

    // Fill in the email field
    const emailInput = page.getByTestId('login-email-input')
    await emailInput.fill('test@example.com')
    await expect(emailInput).toHaveValue('test@example.com')

    // Fill in the password field
    const passwordInput = page.getByTestId('login-password-input')
    await passwordInput.fill('password123')
    await expect(passwordInput).toHaveValue('password123')
  })
})
