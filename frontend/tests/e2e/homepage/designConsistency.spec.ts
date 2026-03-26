/**
 * Design System Consistency E2E Tests
 * Owner: Scenario 15 - Design System Consistency
 *
 * Tests:
 * - Homepage theme matches login page (color scheme, typography)
 * - Both pages use consistent DaisyUI theming
 * - No visual regressions in design system usage
 *
 * Requirements: NFR-4
 */

import { test, expect } from '@playwright/test'

test.describe('Design System Consistency E2E', () => {
  test.describe('Test Case 3: Homepage Theme Matches Login Page', () => {
    test('homepage and login page share the same navbar styling', async ({ page }) => {
      // Get homepage navbar styling
      await page.goto('/')
      const homepageNavbar = page.getByTestId('navbar')
      await expect(homepageNavbar).toBeVisible()
      const homepageNavbarClasses = await homepageNavbar.getAttribute('class')

      // Navigate to login and check navbar styling
      await page.goto('/login')
      const loginNavbar = page.getByTestId('navbar')
      await expect(loginNavbar).toBeVisible()
      const loginNavbarClasses = await loginNavbar.getAttribute('class')

      // Both pages should have identical navbar classes
      expect(homepageNavbarClasses).toBe(loginNavbarClasses)
    })

    test('homepage and login page use consistent background colors', async ({ page }) => {
      // Visit homepage
      await page.goto('/')

      // Get computed background color of main content area
      const homepageBody = page.locator('body')
      const homepageBgColor = await homepageBody.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor
      })

      // Visit login page
      await page.goto('/login')

      const loginBody = page.locator('body')
      const loginBgColor = await loginBody.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor
      })

      // Both pages should have the same base background color
      expect(homepageBgColor).toBe(loginBgColor)
    })

    test('homepage and login page use consistent font family', async ({ page }) => {
      // Get homepage font
      await page.goto('/')
      const homepageFont = await page.evaluate(() => {
        return window.getComputedStyle(document.body).fontFamily
      })

      // Get login page font
      await page.goto('/login')
      const loginFont = await page.evaluate(() => {
        return window.getComputedStyle(document.body).fontFamily
      })

      // Both pages should use the same font family
      expect(homepageFont).toBe(loginFont)
    })

    test('homepage headings and login headings have consistent typography', async ({ page }) => {
      // Get homepage heading style
      await page.goto('/')
      const homepageHeading = page.locator('h1').first()
      const homepageHeadingStyle = await homepageHeading.evaluate((el) => ({
        fontWeight: window.getComputedStyle(el).fontWeight,
        fontFamily: window.getComputedStyle(el).fontFamily,
      }))

      // Get login page heading style
      await page.goto('/login')
      const loginHeading = page.locator('h1').first()
      const loginHeadingStyle = await loginHeading.evaluate((el) => ({
        fontWeight: window.getComputedStyle(el).fontWeight,
        fontFamily: window.getComputedStyle(el).fontFamily,
      }))

      // Both should have consistent typography
      expect(homepageHeadingStyle.fontWeight).toBe(loginHeadingStyle.fontWeight)
      expect(homepageHeadingStyle.fontFamily).toBe(loginHeadingStyle.fontFamily)
    })

    test('navbar buttons have consistent styling across pages', async ({ page }) => {
      // Check homepage navbar buttons
      await page.goto('/')
      const homepageLoginBtn = page.getByTestId('navbar-login-button')
      const homepageSignupBtn = page.getByTestId('navbar-signup-button')

      await expect(homepageLoginBtn).toBeVisible()
      await expect(homepageSignupBtn).toBeVisible()

      const homepageLoginClasses = await homepageLoginBtn.getAttribute('class')
      const homepageSignupClasses = await homepageSignupBtn.getAttribute('class')

      // Check login page navbar buttons
      await page.goto('/login')
      const loginLoginBtn = page.getByTestId('navbar-login-button')
      const loginSignupBtn = page.getByTestId('navbar-signup-button')

      await expect(loginLoginBtn).toBeVisible()
      await expect(loginSignupBtn).toBeVisible()

      const loginLoginClasses = await loginLoginBtn.getAttribute('class')
      const loginSignupClasses = await loginSignupBtn.getAttribute('class')

      // Button classes should be identical
      expect(homepageLoginClasses).toBe(loginLoginClasses)
      expect(homepageSignupClasses).toBe(loginSignupClasses)
    })
  })

  test.describe('DaisyUI Theme Consistency', () => {
    test('homepage uses DaisyUI color scheme variables', async ({ page }) => {
      await page.goto('/')

      // Check that DaisyUI CSS variables are applied
      const hasThemeVariables = await page.evaluate(() => {
        const styles = window.getComputedStyle(document.documentElement)
        // DaisyUI uses oklch colors by default, or hex values via CSS variables
        const primaryColor = styles.getPropertyValue('--p') || styles.getPropertyValue('--primary')
        const baseContent = styles.getPropertyValue('--bc') || styles.getPropertyValue('--base-content')

        // At minimum, the page should have base styling applied
        return document.body.className !== '' || primaryColor !== '' || baseContent !== ''
      })

      expect(hasThemeVariables).toBeTruthy()
    })

    test('all homepage buttons follow DaisyUI btn pattern', async ({ page }) => {
      await page.goto('/')

      // Find all buttons on the page
      const buttons = await page.locator('button').all()

      expect(buttons.length).toBeGreaterThan(0)

      for (const button of buttons) {
        const classes = await button.getAttribute('class')
        // All buttons should have the base 'btn' class
        expect(classes).toContain('btn')
      }
    })

    test('feature cards use consistent DaisyUI card styling', async ({ page }) => {
      await page.goto('/')

      const featureCards = await page.getByTestId('feature-card').all()

      expect(featureCards.length).toBeGreaterThanOrEqual(3)

      for (const card of featureCards) {
        const classes = await card.getAttribute('class')
        // Cards should use DaisyUI card classes
        expect(classes).toContain('card')
        expect(classes).toContain('bg-base-100')
      }
    })

    test('homepage inputs follow DaisyUI input pattern', async ({ page }) => {
      await page.goto('/')

      const urlInput = page.getByTestId('url-input')
      await expect(urlInput).toBeVisible()

      const classes = await urlInput.getAttribute('class')
      // Input should have DaisyUI input classes
      expect(classes).toContain('input')
      expect(classes).toContain('input-bordered')
    })

    test('alert components use DaisyUI alert classes when displayed', async ({ page }) => {
      await page.goto('/')

      // Trigger an error by submitting empty or invalid URL
      const shortenButton = page.getByTestId('shorten-button')
      const urlInput = page.getByTestId('url-input')

      // Enter invalid URL
      await urlInput.fill('not-a-valid-url')
      await shortenButton.click()

      // Wait for error message
      const errorMessage = page.getByTestId('error-message')
      await expect(errorMessage).toBeVisible({ timeout: 5000 })

      const classes = await errorMessage.getAttribute('class')
      // Error should use DaisyUI alert classes
      expect(classes).toContain('alert')
      expect(classes).toContain('alert-error')
    })
  })

  test.describe('Tailwind Utility Classes Usage', () => {
    test('homepage hero section uses Tailwind layout utilities', async ({ page }) => {
      await page.goto('/')

      const heroSection = page.getByTestId('hero-section')
      const classes = await heroSection.getAttribute('class')

      // Should use Tailwind flexbox utilities
      expect(classes).toContain('flex')
      expect(classes).toContain('items-center')
      expect(classes).toContain('justify-center')
    })

    test('features grid uses Tailwind responsive grid classes', async ({ page }) => {
      await page.goto('/')

      const featuresGrid = page.getByTestId('features-grid')
      const classes = await featuresGrid.getAttribute('class')

      // Should have responsive grid classes
      expect(classes).toContain('grid')
      expect(classes).toContain('grid-cols-1')
      expect(classes).toMatch(/md:grid-cols-\d/)
      expect(classes).toMatch(/lg:grid-cols-\d/)
    })

    test('no inline styles are present on main components', async ({ page }) => {
      await page.goto('/')

      const mainComponents = [
        'hero-section',
        'inline-shortener',
        'features-section',
      ]

      for (const testId of mainComponents) {
        const component = page.getByTestId(testId)
        const style = await component.getAttribute('style')

        // Components should not have inline styles (null or empty string is acceptable)
        expect(!style || style === '').toBeTruthy()
      }
    })

    test('spacing uses Tailwind scale consistently', async ({ page }) => {
      await page.goto('/')

      const featuresSection = page.getByTestId('features-section')
      const classes = await featuresSection.getAttribute('class')

      // Should use Tailwind spacing utilities
      expect(classes).toMatch(/py-\d+/)
    })
  })

  test.describe('Visual Consistency Checks', () => {
    test('primary buttons have consistent appearance', async ({ page }) => {
      await page.goto('/')

      // Get all primary buttons
      const primaryCta = page.getByTestId('primary-cta')
      const shortenButton = page.getByTestId('shorten-button')
      const signupButton = page.getByTestId('navbar-signup-button')

      // Check they all have btn-primary class
      for (const button of [primaryCta, shortenButton, signupButton]) {
        const classes = await button.getAttribute('class')
        expect(classes).toContain('btn-primary')
      }
    })

    test('secondary/outline buttons have consistent appearance', async ({ page }) => {
      await page.goto('/')

      // Get secondary/outline button
      const secondaryCta = page.getByTestId('secondary-cta')
      await expect(secondaryCta).toBeVisible()

      const classes = await secondaryCta.getAttribute('class')
      expect(classes).toContain('btn')
      expect(classes).toContain('btn-outline')
    })

    test('ghost buttons have consistent appearance', async ({ page }) => {
      await page.goto('/')

      const loginButton = page.getByTestId('navbar-login-button')
      await expect(loginButton).toBeVisible()

      const classes = await loginButton.getAttribute('class')
      expect(classes).toContain('btn')
      expect(classes).toContain('btn-ghost')
    })
  })
})
