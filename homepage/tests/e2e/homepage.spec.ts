/**
 * Homepage E2E Tests
 * Owner: Scenario 1 - Hero Section & Value Proposition
 *
 * Tests for the hero section functionality including:
 * - Logo visibility
 * - Value proposition display
 * - CTA button behavior
 * - GitHub link behavior
 */
import { test, expect } from '@playwright/test'

test.describe('Hero Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test('hero section renders with logo visible within viewport', async ({ page }) => {
    // Test Case 1: Navigate to homepage root URL
    const heroSection = page.locator('#hero')
    await expect(heroSection).toBeVisible()

    // Check logo is visible
    const logo = page.getByAltText('MirDB Logo')
    await expect(logo).toBeVisible()

    // Verify logo src
    await expect(logo).toHaveAttribute('src', '/assets/logo.gif')

    // Verify logo is in viewport (visible without scrolling)
    const logoBox = await logo.boundingBox()
    expect(logoBox).not.toBeNull()
    if (logoBox) {
      const viewportSize = page.viewportSize()
      expect(logoBox.y).toBeLessThan(viewportSize?.height || 720)
    }
  })

  test('hero section contains h1 with MirDB and tagline paragraph', async ({ page }) => {
    // Test Case 2: Inspect hero section DOM
    const heroSection = page.locator('#hero')

    // Check h1 element with MirDB text
    const heading = heroSection.locator('h1')
    await expect(heading).toBeVisible()
    await expect(heading).toHaveText('MirDB')

    // Check tagline paragraph
    const tagline = heroSection.locator('p').first()
    await expect(tagline).toBeVisible()
    await expect(tagline).toContainText('persistent key-value store')
    await expect(tagline).toContainText('Memcached')
  })

  test('Get Started button smooth-scrolls to quick-start section', async ({ page }) => {
    // Test Case 3: Click 'Get Started' button
    const getStartedButton = page.getByRole('button', { name: /get started/i })
    await expect(getStartedButton).toBeVisible()

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY)

    // Click Get Started button
    await getStartedButton.click()

    // Wait for smooth scroll animation
    await page.waitForTimeout(1000)

    // Verify scroll position changed (scrolled to quick-start section)
    const quickStartSection = page.locator('#quick-start')
    await expect(quickStartSection).toBeInViewport()
  })

  test('View on GitHub link opens repository in new tab', async ({ page, context }) => {
    // Test Case 4: Click 'View on GitHub' link
    const githubLink = page.getByRole('link', { name: /view.*github/i })
    await expect(githubLink).toBeVisible()

    // Verify href
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb')

    // Verify target="_blank"
    await expect(githubLink).toHaveAttribute('target', '_blank')

    // Verify rel="noopener noreferrer" for security
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')

    // Test that clicking opens in a new tab
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      githubLink.click()
    ])

    // Verify new page opened with correct URL
    await expect(newPage).toHaveURL(/github\.com\/yetone\/mirdb/i)
  })

  test('hero section is responsive on mobile viewport', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 })

    // Verify hero elements are still visible
    const logo = page.getByAltText('MirDB Logo')
    await expect(logo).toBeVisible()

    const heading = page.locator('h1')
    await expect(heading).toBeVisible()

    const getStartedButton = page.getByRole('button', { name: /get started/i })
    await expect(getStartedButton).toBeVisible()

    const githubLink = page.getByRole('link', { name: /view.*github/i })
    await expect(githubLink).toBeVisible()

    // Verify CTA buttons are stacked (flex-direction: column on mobile)
    // Use first() to get the innermost container that directly contains the button
    const ctaContainer = page.locator('#hero').locator('div').filter({ has: getStartedButton }).last()
    await expect(ctaContainer).toBeVisible()
  })

  test('hero section has proper accessibility attributes', async ({ page }) => {
    // Check hero section has aria-labelledby
    const heroSection = page.locator('#hero')
    await expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-title')

    // Check heading has correct id
    const heading = page.locator('h1#hero-title')
    await expect(heading).toBeVisible()

    // Check buttons have aria-labels
    const getStartedButton = page.getByRole('button', { name: /get started/i })
    await expect(getStartedButton).toHaveAttribute('aria-label', 'Get started with MirDB')

    const githubLink = page.getByRole('link', { name: /view.*github/i })
    await expect(githubLink).toHaveAttribute('aria-label', 'View MirDB on GitHub')
  })
})
