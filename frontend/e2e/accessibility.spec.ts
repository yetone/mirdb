/**
 * Accessibility E2E Tests
 * Owner: Scenarios 11, 12, 13 (Accessibility - Keyboard Navigation, ARIA Labels, Color Contrast)
 *
 * Tests for keyboard accessibility, ARIA labels, and color contrast on the homepage
 */

import { test, expect, Page } from '@playwright/test'
import AxeBuilder from '@axe-core/playwright'

test.describe('Accessibility - Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for the page to be fully loaded
    await expect(page.getByTestId('hero-section')).toBeVisible()
  })

  test('Tab through HomePage from start to end - focus moves through all interactive elements in logical order', async ({ page }) => {
    // Start by focusing the body to ensure we're at the beginning
    await page.keyboard.press('Tab')

    // Collect all focusable elements in expected order
    const expectedFocusOrder = [
      'url-input',
      'shorten-url-button',
      'signup-button',
      'login-button',
    ]

    // We need to account for potential navbar elements, then form elements, then footer
    // Let's verify the main interactive elements receive focus in a logical order

    // Find all interactive elements
    const urlInput = page.getByTestId('url-input')
    const shortenButton = page.getByTestId('shorten-url-button')
    const signupButton = page.getByTestId('signup-button')
    const loginButton = page.getByTestId('login-button')

    // Tab through the page and verify focus order on hero section elements
    // First, let's find the URL input in the focus order
    let tabCount = 0
    const maxTabs = 20 // Safety limit

    // Tab until we reach the URL input
    while (tabCount < maxTabs) {
      const focused = await page.evaluate(() => {
        const el = document.activeElement
        return el?.getAttribute('data-testid') || el?.tagName?.toLowerCase()
      })

      if (focused === 'url-input') break
      await page.keyboard.press('Tab')
      tabCount++
    }

    // Verify URL input is focused
    await expect(urlInput).toBeFocused()

    // Tab to shorten button
    await page.keyboard.press('Tab')
    await expect(shortenButton).toBeFocused()

    // Tab to signup button
    await page.keyboard.press('Tab')
    await expect(signupButton).toBeFocused()

    // Tab to login button
    await page.keyboard.press('Tab')
    await expect(loginButton).toBeFocused()

    // Continue tabbing to verify we reach footer elements
    await page.keyboard.press('Tab')

    // Verify we can continue tabbing through the page (features, how it works, footer)
    // The footer should have links that are focusable
    const footerLinks = page.locator('[data-testid="footer-links"] a')
    const footerLinksCount = await footerLinks.count()
    expect(footerLinksCount).toBeGreaterThan(0)

    // Tab through to footer links
    let foundFooterLink = false
    tabCount = 0
    while (tabCount < 15) {
      const focused = await page.evaluate(() => {
        const el = document.activeElement
        return el?.closest('[data-testid="footer-links"]') !== null
      })

      if (focused) {
        foundFooterLink = true
        break
      }
      await page.keyboard.press('Tab')
      tabCount++
    }

    expect(foundFooterLink).toBe(true)
  })

  test('Visible focus indicator is displayed on each interactive element', async ({ page }) => {
    // Helper function to check if element has visible focus indicator
    async function hasVisibleFocusIndicator(element: ReturnType<Page['locator']>): Promise<boolean> {
      await element.focus()

      // Check for focus-visible styles - DaisyUI/Tailwind typically uses outline or ring
      const styles = await element.evaluate((el) => {
        const computed = window.getComputedStyle(el)
        return {
          outline: computed.outline,
          outlineWidth: computed.outlineWidth,
          outlineStyle: computed.outlineStyle,
          outlineColor: computed.outlineColor,
          boxShadow: computed.boxShadow,
          ring: computed.getPropertyValue('--tw-ring-color'),
        }
      })

      // Check if there's a visible outline or box-shadow (ring in Tailwind)
      const hasOutline = styles.outlineWidth !== '0px' && styles.outlineStyle !== 'none'
      const hasBoxShadow = styles.boxShadow !== 'none' && styles.boxShadow !== ''

      return hasOutline || hasBoxShadow
    }

    // Test URL input focus indicator
    const urlInput = page.getByTestId('url-input')
    await urlInput.focus()
    await expect(urlInput).toBeFocused()
    const inputHasFocus = await hasVisibleFocusIndicator(urlInput)
    expect(inputHasFocus).toBe(true)

    // Test Shorten URL button focus indicator
    const shortenButton = page.getByTestId('shorten-url-button')
    await shortenButton.focus()
    await expect(shortenButton).toBeFocused()
    const shortenButtonHasFocus = await hasVisibleFocusIndicator(shortenButton)
    expect(shortenButtonHasFocus).toBe(true)

    // Test Sign Up button focus indicator
    const signupButton = page.getByTestId('signup-button')
    await signupButton.focus()
    await expect(signupButton).toBeFocused()
    const signupButtonHasFocus = await hasVisibleFocusIndicator(signupButton)
    expect(signupButtonHasFocus).toBe(true)

    // Test Log In button focus indicator
    const loginButton = page.getByTestId('login-button')
    await loginButton.focus()
    await expect(loginButton).toBeFocused()
    const loginButtonHasFocus = await hasVisibleFocusIndicator(loginButton)
    expect(loginButtonHasFocus).toBe(true)

    // Test footer links focus indicators
    const footerAboutLink = page.getByTestId('footer-link-about')
    await footerAboutLink.focus()
    await expect(footerAboutLink).toBeFocused()
    const aboutLinkHasFocus = await hasVisibleFocusIndicator(footerAboutLink)
    expect(aboutLinkHasFocus).toBe(true)

    // Test social media icons focus indicators
    const twitterLink = page.getByTestId('footer-social-twitter')
    await twitterLink.focus()
    await expect(twitterLink).toBeFocused()
    const twitterHasFocus = await hasVisibleFocusIndicator(twitterLink)
    expect(twitterHasFocus).toBe(true)
  })

  test('Focus URL input and press Enter with valid URL - form submits as if Shorten button was clicked', async ({ page }) => {
    const urlInput = page.getByTestId('url-input')
    const testUrl = 'https://example.com/test-keyboard-navigation'

    // Focus the URL input
    await urlInput.focus()
    await expect(urlInput).toBeFocused()

    // Type a valid URL
    await urlInput.fill(testUrl)

    // Press Enter to submit the form
    await page.keyboard.press('Enter')

    // Wait for either a result or an error (API might not be running)
    // In a real scenario, the form should attempt to submit
    // We check for loading state or result/error appearing
    const submitButton = page.getByTestId('shorten-url-button')

    // The button should show loading state or the result should appear
    // Since the API might not be available, we verify the form was submitted
    // by checking for either loading indicator, result, or error message
    await expect(async () => {
      const isLoading = await submitButton.locator('.loading').isVisible().catch(() => false)
      const hasResult = await page.locator('[role="region"][aria-label="Shortened URL result"]').isVisible().catch(() => false)
      const hasError = await page.locator('[role="alert"]').isVisible().catch(() => false)

      // At least one of these should be true (form was processed)
      expect(isLoading || hasResult || hasError).toBe(true)
    }).toPass({ timeout: 5000 })
  })

  test('Focus on Log In button and press Enter - navigation to /login occurs', async ({ page }) => {
    const loginButton = page.getByTestId('login-button')

    // Focus the Log In button
    await loginButton.focus()
    await expect(loginButton).toBeFocused()

    // Press Enter to activate the button
    await page.keyboard.press('Enter')

    // Wait for navigation to /login
    await page.waitForURL('**/login')

    // Verify we're on the login page
    expect(page.url()).toContain('/login')
  })
})

test.describe('Accessibility - Color Contrast', () => {
  test.describe('Light Mode', () => {
    test('hero section text contrast meets WCAG AA standards in light mode', async ({
      page,
    }) => {
      // Navigate to homepage
      await page.goto('/')

      // Ensure light mode is active (DaisyUI default)
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'light')
      })

      // Wait for theme to apply
      await page.waitForTimeout(100)

      // Verify hero section is visible
      const heroSection = page.getByTestId('hero-section')
      await expect(heroSection).toBeVisible()

      // Run axe accessibility scan focused on color contrast
      const accessibilityScanResults = await new AxeBuilder({ page })
        .include('[data-testid="hero-section"]')
        .withRules(['color-contrast'])
        .analyze()

      // Log any violations for debugging
      if (accessibilityScanResults.violations.length > 0) {
        console.log(
          'Light mode contrast violations:',
          JSON.stringify(accessibilityScanResults.violations, null, 2)
        )
      }

      // Assert no color contrast violations
      expect(accessibilityScanResults.violations).toEqual([])
    })

    test('all page text contrast meets WCAG AA standards in light mode', async ({
      page,
    }) => {
      await page.goto('/')

      // Ensure light mode is active
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'light')
      })

      await page.waitForTimeout(100)

      // Run axe accessibility scan on full page for color contrast
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withRules(['color-contrast'])
        .analyze()

      // Log any violations for debugging
      if (accessibilityScanResults.violations.length > 0) {
        console.log(
          'Light mode full page contrast violations:',
          JSON.stringify(accessibilityScanResults.violations, null, 2)
        )
      }

      expect(accessibilityScanResults.violations).toEqual([])
    })
  })

  test.describe('Dark Mode', () => {
    test('hero section text contrast meets WCAG AA standards in dark mode', async ({
      page,
    }) => {
      await page.goto('/')

      // Switch to dark mode
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'dark')
      })

      // Wait for theme to apply
      await page.waitForTimeout(100)

      // Verify hero section is visible
      const heroSection = page.getByTestId('hero-section')
      await expect(heroSection).toBeVisible()

      // Run axe accessibility scan focused on color contrast
      const accessibilityScanResults = await new AxeBuilder({ page })
        .include('[data-testid="hero-section"]')
        .withRules(['color-contrast'])
        .analyze()

      // Log any violations for debugging
      if (accessibilityScanResults.violations.length > 0) {
        console.log(
          'Dark mode contrast violations:',
          JSON.stringify(accessibilityScanResults.violations, null, 2)
        )
      }

      // Assert no color contrast violations
      expect(accessibilityScanResults.violations).toEqual([])
    })

    test('all page text contrast meets WCAG AA standards in dark mode', async ({
      page,
    }) => {
      await page.goto('/')

      // Switch to dark mode
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'dark')
      })

      await page.waitForTimeout(100)

      // Run axe accessibility scan on full page for color contrast
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withRules(['color-contrast'])
        .analyze()

      // Log any violations for debugging
      if (accessibilityScanResults.violations.length > 0) {
        console.log(
          'Dark mode full page contrast violations:',
          JSON.stringify(accessibilityScanResults.violations, null, 2)
        )
      }

      expect(accessibilityScanResults.violations).toEqual([])
    })
  })

  test.describe('CTA Button Contrast', () => {
    test('CTA button text contrast meets WCAG AA standards', async ({
      page,
    }) => {
      await page.goto('/')

      // Test in light mode
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'light')
      })
      await page.waitForTimeout(100)

      // Verify CTA buttons are visible
      const shortenButton = page.getByTestId('shorten-url-button')
      const signupButton = page.getByTestId('signup-button')
      const loginButton = page.getByTestId('login-button')

      await expect(shortenButton).toBeVisible()
      await expect(signupButton).toBeVisible()
      await expect(loginButton).toBeVisible()

      // Run axe scan on buttons
      const lightModeResults = await new AxeBuilder({ page })
        .include('[data-testid="shorten-url-button"]')
        .include('[data-testid="signup-button"]')
        .include('[data-testid="login-button"]')
        .withRules(['color-contrast'])
        .analyze()

      if (lightModeResults.violations.length > 0) {
        console.log(
          'Light mode button contrast violations:',
          JSON.stringify(lightModeResults.violations, null, 2)
        )
      }

      expect(lightModeResults.violations).toEqual([])

      // Test in dark mode
      await page.evaluate(() => {
        document.documentElement.setAttribute('data-theme', 'dark')
      })
      await page.waitForTimeout(100)

      const darkModeResults = await new AxeBuilder({ page })
        .include('[data-testid="shorten-url-button"]')
        .include('[data-testid="signup-button"]')
        .include('[data-testid="login-button"]')
        .withRules(['color-contrast'])
        .analyze()

      if (darkModeResults.violations.length > 0) {
        console.log(
          'Dark mode button contrast violations:',
          JSON.stringify(darkModeResults.violations, null, 2)
        )
      }

      expect(darkModeResults.violations).toEqual([])
    })
  })
})
