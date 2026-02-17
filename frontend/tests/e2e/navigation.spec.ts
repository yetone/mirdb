/**
 * E2E Navigation Tests
 *
 * Scenario 3: Navigation to Registration
 * - Test Case 1: Click primary CTA button navigates to /register page
 * - Test Case 2: Navigation occurs immediately with visual feedback
 *
 * Scenario 4 will add login navigation tests to this file.
 */

import { test, expect } from '@playwright/test'

test.describe('Navigation to Registration', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/')
    // Wait for the hero section to be visible
    await page.waitForSelector('[data-testid="hero-section"]', { state: 'visible' })
  })

  /**
   * Test Case 1: Click primary CTA button navigates to /register page
   * Input: Click primary CTA button
   * Expected: User is navigated to /register page
   */
  test('clicking primary CTA navigates to registration page', async ({ page }) => {
    // Find the primary CTA button
    const primaryCta = page.getByTestId('hero-primary-cta')

    // Verify button is visible and enabled
    await expect(primaryCta).toBeVisible()
    await expect(primaryCta).toBeEnabled()

    // Click the primary CTA
    await primaryCta.click()

    // Verify navigation to /register
    await expect(page).toHaveURL(/\/register/)

    // Verify registration page content is loaded
    await expect(page.getByRole('heading', { name: /create account/i })).toBeVisible()
  })

  /**
   * Test Case 2: Navigation occurs immediately with visual feedback
   * Input: Verify transition feedback
   * Expected: Navigation occurs immediately with visual feedback
   */
  test('navigation occurs immediately with visual feedback', async ({ page }) => {
    const primaryCta = page.getByTestId('hero-primary-cta')

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
    const primaryCta = page.getByTestId('hero-primary-cta')

    // Button should be visible and have proper styling
    await expect(primaryCta).toBeVisible()

    // Button should contain appropriate text
    const buttonText = await primaryCta.textContent()
    expect(buttonText?.toLowerCase()).toMatch(/get started|sign up|register|start/i)

    // Button should have btn-primary class for visual distinction
    await expect(primaryCta).toHaveClass(/btn-primary/)
  })

  /**
   * Test: Multiple clicks don't cause issues
   */
  test('navigation works correctly on subsequent visits', async ({ page }) => {
    const primaryCta = page.getByTestId('hero-primary-cta')

    // Click to navigate to register
    await primaryCta.click()
    await expect(page).toHaveURL(/\/register/)

    // Navigate back to home
    await page.goto('/')
    await page.waitForSelector('[data-testid="hero-section"]', { state: 'visible' })

    // Click again - should still work
    await page.getByTestId('hero-primary-cta').click()
    await expect(page).toHaveURL(/\/register/)
  })

  /**
   * Test Case 3: CTA button is keyboard accessible and has proper ARIA labels
   * Input: Check button accessibility
   * Expected: CTA button is keyboard accessible and has proper ARIA labels
   */
  test('primary CTA is keyboard accessible with proper ARIA labels', async ({ page }) => {
    const primaryCta = page.getByTestId('hero-primary-cta')

    // Verify button has proper ARIA label
    const ariaLabel = await primaryCta.getAttribute('aria-label')
    expect(ariaLabel).toBeTruthy()
    expect(ariaLabel?.toLowerCase()).toMatch(/get started|url|shortening|register/i)

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
      const testId = await focusedElement.getAttribute('data-testid')

      if (testId === 'hero-primary-cta') {
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
    const primaryCta = page.getByTestId('hero-primary-cta')

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
