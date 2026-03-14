/**
 * E2E tests for Hero section.
 * Owner: Scenario 2 - Hero Section Implementation
 *
 * Test cases:
 * - Hero displays headline, subheadline, CTA
 * - CTA button click navigation
 * - CTA hover and focus states
 * - H1 heading presence
 */

import { test, expect } from '@playwright/test'

test.describe('Hero Section E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  // Test Case 4: Click primary CTA button - Button triggers navigation or action as intended
  test('Test Case 4: CTA button click triggers navigation', async ({ page }) => {
    // Get the CTA button
    const ctaButton = page.getByTestId('hero-cta-button')
    await expect(ctaButton).toBeVisible()

    // Get the href attribute
    const href = await ctaButton.getAttribute('href')
    expect(href).toBe('#signup')

    // Click the button
    await ctaButton.click()

    // Verify URL has changed to include the hash
    await expect(page).toHaveURL(/#signup/)
  })

  // Test Case 6: CTA button hover state - Button displays visible hover effect
  test('Test Case 6: CTA button displays visible hover effect', async ({ page }) => {
    const ctaButton = page.getByTestId('hero-cta-button')
    await expect(ctaButton).toBeVisible()

    // Get initial styles
    const initialStyles = await ctaButton.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        backgroundColor: styles.backgroundColor,
        transform: styles.transform,
        boxShadow: styles.boxShadow,
      }
    })

    // Hover over the button
    await ctaButton.hover()

    // Wait for transition to complete
    await page.waitForTimeout(250)

    // Get hover styles
    const hoverStyles = await ctaButton.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        backgroundColor: styles.backgroundColor,
        transform: styles.transform,
        boxShadow: styles.boxShadow,
      }
    })

    // Verify at least one visual property changed on hover
    const hasVisibleChange =
      initialStyles.backgroundColor !== hoverStyles.backgroundColor ||
      initialStyles.transform !== hoverStyles.transform ||
      initialStyles.boxShadow !== hoverStyles.boxShadow

    expect(hasVisibleChange).toBeTruthy()
  })

  // Test Case 7: CTA button focus state - Button displays visible focus indicator
  test('Test Case 7: CTA button displays visible focus indicator for keyboard navigation', async ({
    page,
  }) => {
    const ctaButton = page.getByTestId('hero-cta-button')
    await expect(ctaButton).toBeVisible()

    // Get initial styles before focus
    const initialStyles = await ctaButton.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        outline: styles.outline,
        boxShadow: styles.boxShadow,
        outlineOffset: styles.outlineOffset,
      }
    })

    // Focus the button using keyboard navigation
    await ctaButton.focus()

    // Wait for focus styles to apply
    await page.waitForTimeout(100)

    // Get focused styles
    const focusStyles = await ctaButton.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        outline: styles.outline,
        boxShadow: styles.boxShadow,
        outlineOffset: styles.outlineOffset,
      }
    })

    // The button should have a visible focus indicator (ring/outline/shadow)
    // Tailwind's focus:ring-4 creates a box-shadow for the focus ring
    const hasFocusIndicator =
      focusStyles.boxShadow !== 'none' ||
      focusStyles.outline !== 'none' ||
      focusStyles.boxShadow !== initialStyles.boxShadow

    expect(hasFocusIndicator).toBeTruthy()
  })

  // Additional E2E tests for Hero section
  test('Hero section renders correctly', async ({ page }) => {
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    const headline = page.getByTestId('hero-headline')
    await expect(headline).toBeVisible()
    await expect(headline).toHaveText(/Transform Your Workflow/)

    const subheadline = page.getByTestId('hero-subheadline')
    await expect(subheadline).toBeVisible()
  })

  test('Hero headline is an h1 element', async ({ page }) => {
    const h1 = page.locator('h1')
    await expect(h1).toBeVisible()
    await expect(h1).toHaveAttribute('id', 'hero-headline')
  })

  test('CTA button is keyboard accessible', async ({ page }) => {
    // Tab to the CTA button
    await page.keyboard.press('Tab')

    // The CTA button should be focusable
    const ctaButton = page.getByTestId('hero-cta-button')
    await expect(ctaButton).toBeFocused()

    // Press Enter to activate the button
    await page.keyboard.press('Enter')

    // Verify navigation occurred
    await expect(page).toHaveURL(/#signup/)
  })

  test('Hero section has proper accessibility attributes', async ({ page }) => {
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-headline')

    const ctaButton = page.getByTestId('hero-cta-button')
    await expect(ctaButton).toHaveAttribute('role', 'button')
    await expect(ctaButton).toHaveAttribute('aria-label')
  })

  test('CTA button has readable text', async ({ page }) => {
    const ctaButton = page.getByTestId('hero-cta-button')
    const buttonText = await ctaButton.textContent()

    expect(buttonText).toBeTruthy()
    expect(buttonText!.length).toBeGreaterThan(0)
    expect(buttonText).toBe('Get Started Free')
  })
})
