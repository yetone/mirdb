import { test, expect } from '@playwright/test'

test.describe('Hero Section E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test.describe('Test Case 4: Click CTA button scrolls to Getting Started', () => {
    test('clicking Get Started button scrolls to the Getting Started section', async ({ page }) => {
      // Find and click the Get Started button
      const ctaButton = page.getByRole('link', { name: /get started/i })
      await expect(ctaButton).toBeVisible()

      // Get the initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY)

      // Click the CTA button
      await ctaButton.click()

      // Wait for smooth scroll to complete
      await page.waitForTimeout(1000)

      // Verify scroll happened (scroll position changed)
      const newScrollY = await page.evaluate(() => window.scrollY)

      // The Getting Started section should be visible in viewport
      const gettingStartedSection = page.locator('#getting-started')
      await expect(gettingStartedSection).toBeInViewport()
    })

    test('Get Started link has correct href attribute', async ({ page }) => {
      const ctaButton = page.getByRole('link', { name: /get started/i })
      await expect(ctaButton).toHaveAttribute('href', '#getting-started')
    })
  })

  test.describe('Test Case 5: Hero section styling', () => {
    test('hero section has visually distinct background', async ({ page }) => {
      const heroSection = page.locator('#hero')
      await expect(heroSection).toBeVisible()

      // Check the hero section has gradient background styling
      const hasGradient = await heroSection.evaluate((el) => {
        const style = window.getComputedStyle(el)
        return style.backgroundImage.includes('gradient') ||
               el.classList.contains('bg-gradient-to-br')
      })
      expect(hasGradient).toBe(true)
    })

    test('hero section has appropriate minimum height', async ({ page }) => {
      const heroSection = page.locator('#hero')
      const box = await heroSection.boundingBox()

      // Hero should take up significant viewport height (at least 60%)
      const viewportHeight = await page.evaluate(() => window.innerHeight)
      expect(box?.height).toBeGreaterThanOrEqual(viewportHeight * 0.6)
    })

    test('headline has appropriate typography styling', async ({ page }) => {
      const headline = page.getByRole('heading', { level: 1 })
      await expect(headline).toBeVisible()

      // Check font size is large (at least 24px for mobile, larger for desktop)
      const fontSize = await headline.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize)
      })
      expect(fontSize).toBeGreaterThanOrEqual(24)
    })

    test('CTA button is visually prominent', async ({ page }) => {
      const ctaButton = page.getByRole('link', { name: /get started/i })
      await expect(ctaButton).toBeVisible()

      // Check button has a background color (primary button styling)
      const bgColor = await ctaButton.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor
      })

      // Should not be transparent
      expect(bgColor).not.toBe('transparent')
      expect(bgColor).not.toBe('rgba(0, 0, 0, 0)')
    })
  })

  test.describe('Hero section content', () => {
    test('displays the value proposition headline', async ({ page }) => {
      const headline = page.getByRole('heading', { level: 1 })
      await expect(headline).toContainText(/persistent/i)
      await expect(headline).toContainText(/key-value/i)
      await expect(headline).toContainText(/memcached/i)
    })

    test('displays descriptive paragraph', async ({ page }) => {
      const description = page.locator('#hero p')
      await expect(description).toBeVisible()
      await expect(description).toContainText(/mirdb/i)
    })
  })
})
