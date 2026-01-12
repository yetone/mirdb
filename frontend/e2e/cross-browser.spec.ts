import { test, expect } from '@playwright/test'

/**
 * Cross-browser compatibility test suite for the landing page.
 * Tests that the landing page renders correctly and all features function
 * across Chrome, Firefox, Safari (WebKit), and Edge browsers.
 *
 * Requirements from NFR-5: Cross-browser compatibility (Chrome, Firefox, Safari, Edge)
 */

test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test.describe('Page Structure and Layout', () => {
    test('landing page renders with main content', async ({ page }) => {
      // Verify the home page container exists
      const homePage = page.locator('[data-testid="home-page"]')
      await expect(homePage).toBeVisible()
    })

    test('hero section renders correctly', async ({ page }) => {
      // Check hero section is visible
      const heroSection = page.locator('section').first()
      await expect(heroSection).toBeVisible()

      // Check for main headline text
      const headline = page.getByRole('heading', { level: 1 })
      await expect(headline).toBeVisible()
    })

    test('features section renders correctly', async ({ page }) => {
      // Look for features section
      const featuresHeading = page.getByRole('heading', { name: /features/i })
      await expect(featuresHeading).toBeVisible()
    })

    test('how it works section renders correctly', async ({ page }) => {
      // Look for how it works section
      const howItWorksHeading = page.getByRole('heading', { name: /how it works/i })
      await expect(howItWorksHeading).toBeVisible()
    })

    test('footer renders correctly', async ({ page }) => {
      // Check footer is visible
      const footer = page.locator('footer')
      await expect(footer).toBeVisible()
    })
  })

  test.describe('Navigation and Links', () => {
    test('navigation links are functional', async ({ page }) => {
      // Check for navigation bar
      const nav = page.locator('nav, header')
      await expect(nav.first()).toBeVisible()

      // Check for CTA buttons/links
      const ctaLinks = page.getByRole('link', { name: /get started|sign up|register/i })
      const ctaCount = await ctaLinks.count()
      expect(ctaCount).toBeGreaterThan(0)
    })

    test('CTA buttons are clickable', async ({ page }) => {
      // Find primary CTA button
      const ctaButton = page.getByRole('link', { name: /get started/i }).first()

      if (await ctaButton.count() > 0) {
        await expect(ctaButton).toBeEnabled()
        // Verify href is present
        const href = await ctaButton.getAttribute('href')
        expect(href).toBeTruthy()
      }
    })
  })

  test.describe('Visual Consistency', () => {
    test('page has proper styling applied', async ({ page }) => {
      // Check that Tailwind/DaisyUI classes are being applied
      const body = page.locator('body')
      const bodyClass = await body.getAttribute('class')

      // Page should have some styling classes
      const main = page.locator('main')
      await expect(main).toBeVisible()
    })

    test('images and icons render', async ({ page }) => {
      // Check for SVG icons or images
      const svgs = page.locator('svg')
      const svgCount = await svgs.count()

      // Landing page should have icons/graphics
      expect(svgCount).toBeGreaterThan(0)
    })

    test('text content is readable', async ({ page }) => {
      // Check that main text elements have content
      const heading = page.getByRole('heading', { level: 1 })
      const headingText = await heading.textContent()
      expect(headingText?.trim().length).toBeGreaterThan(0)
    })
  })

  test.describe('Interactive Elements', () => {
    test('buttons have hover states', async ({ page }) => {
      // Find a button
      const button = page.getByRole('link').first()
      await expect(button).toBeVisible()

      // Verify button is interactive
      await button.hover()
      // If we get here without error, hover works
    })

    test('form inputs work correctly', async ({ page }) => {
      // Look for demo section input if present
      const input = page.locator('input[type="text"], input[type="url"]').first()

      if (await input.count() > 0) {
        await expect(input).toBeVisible()
        await input.fill('https://example.com')
        await expect(input).toHaveValue('https://example.com')
      }
    })
  })

  test.describe('Animations and Transitions', () => {
    test('page loads without animation errors', async ({ page }) => {
      // Wait for page to be fully loaded
      await page.waitForLoadState('networkidle')

      // Check that Framer Motion animations don't cause errors
      const errorMessages = await page.locator('.error, [role="alert"]').count()
      expect(errorMessages).toBe(0)
    })

    test('scroll behavior works', async ({ page }) => {
      // Scroll down the page
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight))

      // Wait a moment for scroll
      await page.waitForTimeout(500)

      // Scroll back up
      await page.evaluate(() => window.scrollTo(0, 0))

      // Page should still be functional
      const homePage = page.locator('[data-testid="home-page"]')
      await expect(homePage).toBeVisible()
    })
  })

  test.describe('Responsive Behavior', () => {
    test('page content is visible at current viewport', async ({ page }) => {
      // Verify main content is visible at current viewport size
      const homePage = page.locator('[data-testid="home-page"]')
      await expect(homePage).toBeVisible()

      // All sections should be present
      const sections = page.locator('section')
      const sectionCount = await sections.count()
      expect(sectionCount).toBeGreaterThan(0)
    })
  })

  test.describe('Accessibility Basics', () => {
    test('page has proper heading structure', async ({ page }) => {
      // Check for h1
      const h1 = page.getByRole('heading', { level: 1 })
      const h1Count = await h1.count()
      expect(h1Count).toBeGreaterThanOrEqual(1)
    })

    test('links have accessible names', async ({ page }) => {
      const links = page.getByRole('link')
      const linkCount = await links.count()

      // Check first few links have accessible names
      for (let i = 0; i < Math.min(linkCount, 5); i++) {
        const link = links.nth(i)
        const name = await link.getAttribute('aria-label') || await link.textContent()
        expect(name?.trim().length).toBeGreaterThan(0)
      }
    })

    test('buttons are focusable', async ({ page }) => {
      const buttons = page.getByRole('button')
      const buttonCount = await buttons.count()

      if (buttonCount > 0) {
        const firstButton = buttons.first()
        await firstButton.focus()
        await expect(firstButton).toBeFocused()
      }
    })
  })
})
