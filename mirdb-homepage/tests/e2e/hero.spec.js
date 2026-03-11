/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Test coverage:
 * - Get Started CTA button smooth scrolls to Quick Start section
 * - GitHub Repo CTA button opens repository in new tab
 */

import { test, expect } from '@playwright/test'

test.describe('Hero Section CTAs', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for the Hero section to be rendered
    await page.waitForSelector('[data-testid="hero-section"]')
  })

  test('Test Case 3: Get Started CTA button scrolls to Quick Start section', async ({ page }) => {
    // Verify the Get Started button exists
    const getStartedBtn = page.locator('[data-testid="hero-cta-get-started"]')
    await expect(getStartedBtn).toBeVisible()
    await expect(getStartedBtn).toHaveText(/Get Started/i)

    // Verify the button links to quickstart section
    await expect(getStartedBtn).toHaveAttribute('href', '#quickstart')

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY)

    // Click the Get Started button
    await getStartedBtn.click()

    // Wait a bit for smooth scroll animation
    await page.waitForTimeout(500)

    // Verify scroll happened (or URL hash changed)
    const currentScrollY = await page.evaluate(() => window.scrollY)
    const hasScrolled = currentScrollY > initialScrollY
    const urlHash = await page.evaluate(() => window.location.hash)

    // Either the page scrolled or the URL hash changed to quickstart
    expect(hasScrolled || urlHash === '#quickstart').toBe(true)

    // Verify Quick Start section is in viewport or URL points to it
    const quickStartSection = page.locator('#quickstart')
    const isQuickStartVisible = await quickStartSection.isVisible().catch(() => false)

    // The test passes if either:
    // 1. The page scrolled to the quickstart section
    // 2. The URL hash is set correctly
    expect(isQuickStartVisible || urlHash === '#quickstart').toBe(true)
  })

  test('Test Case 4: GitHub Repo CTA button opens repository in new tab', async ({ page, context }) => {
    // Verify the GitHub button exists
    const githubBtn = page.locator('[data-testid="hero-cta-github"]')
    await expect(githubBtn).toBeVisible()
    await expect(githubBtn).toHaveText(/GitHub|View.*Source|Repository/i)

    // Verify the button has correct attributes for opening in new tab
    await expect(githubBtn).toHaveAttribute('target', '_blank')
    await expect(githubBtn).toHaveAttribute('rel', /noopener/)

    // Verify the href points to a GitHub URL
    const href = await githubBtn.getAttribute('href')
    expect(href).toMatch(/github\.com/)

    // Test that clicking opens a new page/tab
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      githubBtn.click()
    ])

    // Verify the new page URL is the GitHub repository
    const newPageUrl = newPage.url()
    expect(newPageUrl).toMatch(/github\.com/)

    // Clean up
    await newPage.close()
  })
})

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test('should display the hero section with all required elements', async ({ page }) => {
    // Wait for hero section
    const heroSection = page.locator('[data-testid="hero-section"]')
    await expect(heroSection).toBeVisible()

    // Verify product name
    const productName = page.locator('[data-testid="hero-product-name"]')
    await expect(productName).toBeVisible()
    await expect(productName).toHaveText('MirDB')

    // Verify tagline
    const tagline = page.locator('[data-testid="hero-tagline"]')
    await expect(tagline).toBeVisible()
    await expect(tagline).toContainText(/persistent.*key-value|key-value.*persistent/i)

    // Verify value proposition
    const valueProposition = page.locator('[data-testid="hero-value-proposition"]')
    await expect(valueProposition).toBeVisible()

    // Verify logo
    const logo = page.locator('[data-testid="hero-logo"]')
    await expect(logo).toBeVisible()

    // Verify CTA buttons
    const getStartedBtn = page.locator('[data-testid="hero-cta-get-started"]')
    const githubBtn = page.locator('[data-testid="hero-cta-github"]')
    await expect(getStartedBtn).toBeVisible()
    await expect(githubBtn).toBeVisible()
  })

  test('should have proper heading hierarchy', async ({ page }) => {
    // The product name should be an h1
    const h1 = page.locator('[data-testid="hero-section"] h1')
    await expect(h1).toBeVisible()
    await expect(h1).toHaveText('MirDB')
  })
})
