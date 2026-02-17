/**
 * E2E Responsive Layout Tests
 *
 * Tests responsive behavior of the homepage across different viewport sizes:
 * - Scenario 8: Mobile Responsive Layout (viewport < 768px)
 * - Scenario 9: Tablet Responsive Layout (768px-1023px)
 * - Scenario 10: Desktop Layout (viewport >= 1024px)
 */

import { test, expect } from '@playwright/test'

/**
 * Scenario 9: Tablet Responsive Layout
 * Verifies that homepage displays correctly on tablet viewport sizes (768px-1023px)
 *
 * Test Cases:
 * 1. Render at 768px viewport width - Layout adapts to tablet size, features show in 2-column grid
 * 2. Verify navigation at tablet size - Navigation elements are accessible, may show condensed menu
 * 3. Check content readability - All text is readable, images scale appropriately
 */
test.describe('Tablet Responsive Layout', () => {
  // Configure tablet viewport for all tests in this describe block
  test.use({
    viewport: { width: 768, height: 1024 },
  })

  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/')
    // Wait for the page to be fully loaded
    await page.waitForLoadState('domcontentloaded')
  })

  /**
   * Test Case 1: Layout adapts to tablet size (768px viewport)
   * Input: Render at 768px viewport width
   * Expected: Layout adapts to tablet size, features may show in 2-column grid
   */
  test('layout adapts to tablet viewport size at 768px', async ({ page }) => {
    // Verify the page is rendered
    await expect(page.locator('body')).toBeVisible()

    // Verify hero section is visible and properly sized
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Get hero section dimensions
    const heroBoundingBox = await heroSection.boundingBox()
    expect(heroBoundingBox).not.toBeNull()
    if (heroBoundingBox) {
      // Hero should fill viewport width (allowing for small margin)
      expect(heroBoundingBox.width).toBeGreaterThanOrEqual(700)
    }

    // Verify features grid uses 2-column layout at tablet size
    // The CSS class is: grid-cols-1 md:grid-cols-2 lg:grid-cols-4
    // At 768px (md breakpoint), it should be 2 columns
    const featuresGrid = page.getByTestId('features-grid')
    await expect(featuresGrid).toBeVisible()

    // Get all feature cards
    const featureCards = page.locator('[data-testid^="feature-card-"]')
    const cardCount = await featureCards.count()
    expect(cardCount).toBeGreaterThanOrEqual(3) // At least 3 feature cards

    // Verify cards are arranged in a grid layout
    // Get the first two cards and check they are side by side (same row)
    if (cardCount >= 2) {
      const firstCard = featureCards.nth(0)
      const secondCard = featureCards.nth(1)

      const firstBox = await firstCard.boundingBox()
      const secondBox = await secondCard.boundingBox()

      expect(firstBox).not.toBeNull()
      expect(secondBox).not.toBeNull()

      if (firstBox && secondBox) {
        // At tablet width with 2-column grid, cards should be on the same row
        // (similar Y position within tolerance)
        const yDifference = Math.abs(firstBox.y - secondBox.y)
        expect(yDifference).toBeLessThan(50) // Allow for small differences

        // Cards should be side by side (different X positions)
        expect(Math.abs(firstBox.x - secondBox.x)).toBeGreaterThan(100)
      }
    }
  })

  /**
   * Test Case 1 (additional): Verify at 1000px viewport (upper tablet range)
   */
  test('layout works at upper tablet range (1000px)', async ({ page }) => {
    // Resize to upper tablet range
    await page.setViewportSize({ width: 1000, height: 1024 })
    await page.reload()
    await page.waitForLoadState('domcontentloaded')

    // Verify hero section is visible
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Verify features grid is visible and functional
    const featuresGrid = page.getByTestId('features-grid')
    await expect(featuresGrid).toBeVisible()

    // At 1000px (still < lg:1024px), should still be 2-column grid
    const featureCards = page.locator('[data-testid^="feature-card-"]')
    const cardCount = await featureCards.count()
    expect(cardCount).toBeGreaterThanOrEqual(3)

    // Verify first two cards are on the same row
    if (cardCount >= 2) {
      const firstCard = featureCards.nth(0)
      const secondCard = featureCards.nth(1)

      const firstBox = await firstCard.boundingBox()
      const secondBox = await secondCard.boundingBox()

      if (firstBox && secondBox) {
        // Cards should be on the same row
        const yDifference = Math.abs(firstBox.y - secondBox.y)
        expect(yDifference).toBeLessThan(50)
      }
    }
  })

  /**
   * Test Case 2: Navigation elements are accessible at tablet size
   * Input: Verify navigation at tablet size
   * Expected: Navigation elements are accessible, may show condensed menu
   */
  test('navigation elements are accessible at tablet viewport', async ({ page }) => {
    // The navbar uses lg:flex for center menu and lg:hidden for mobile dropdown
    // At tablet size (768px, which is < lg:1024px), the dropdown menu should be visible

    // Check for main navbar presence using the navbar class
    const navbar = page.locator('nav.navbar')
    await expect(navbar).toBeVisible()

    // At tablet viewport (< lg:1024px), the horizontal menu should be hidden
    // and the mobile dropdown should be visible
    const centerMenu = page.locator('.navbar-center.hidden.lg\\:flex')
    const isHorizontalMenuHidden = await centerMenu.isHidden()
    expect(isHorizontalMenuHidden).toBe(true)

    // The mobile dropdown toggle should be visible
    const dropdownToggle = page.locator('.lg\\:hidden.dropdown')
    await expect(dropdownToggle).toBeVisible()

    // Click the dropdown toggle to open the menu
    const dropdownLabel = dropdownToggle.locator('label')
    await dropdownLabel.click()

    // Verify dropdown menu items are accessible
    const dropdownMenu = dropdownToggle.locator('ul.dropdown-content')
    await expect(dropdownMenu).toBeVisible()

    // Check that navigation links are present in the dropdown
    const homeLink = dropdownMenu.getByRole('link', { name: /home/i })
    const loginLink = dropdownMenu.getByRole('link', { name: /login/i })
    const registerLink = dropdownMenu.getByRole('link', { name: /register/i })

    await expect(homeLink).toBeVisible()
    await expect(loginLink).toBeVisible()
    await expect(registerLink).toBeVisible()

    // Verify the theme selector is still accessible
    const themeSelector = page.locator('select[aria-label="Select theme"]')
    await expect(themeSelector).toBeVisible()
    await expect(themeSelector).toBeEnabled()
  })

  /**
   * Test Case 2 (additional): Mobile menu items are clickable
   */
  test('mobile dropdown menu items navigate correctly at tablet size', async ({ page }) => {
    // Open the mobile dropdown
    const dropdownToggle = page.locator('.lg\\:hidden.dropdown label')
    await dropdownToggle.click()

    // Click on Login link
    const loginLink = page.locator('.lg\\:hidden.dropdown ul a').filter({ hasText: /login/i })
    await loginLink.click()

    // Verify navigation to login page
    await expect(page).toHaveURL(/\/login/)
  })

  /**
   * Test Case 3: Content readability at tablet viewport
   * Input: Check content readability
   * Expected: All text is readable, images scale appropriately
   */
  test('all text is readable at tablet viewport', async ({ page }) => {
    // Verify hero heading is visible and readable
    const heroHeading = page.locator('h1')
    await expect(heroHeading).toBeVisible()

    // Get computed font size of the heading
    const headingFontSize = await heroHeading.evaluate((el) => {
      return window.getComputedStyle(el).fontSize
    })

    // Heading should have reasonable font size (at least 24px for readability)
    const fontSizeValue = parseFloat(headingFontSize)
    expect(fontSizeValue).toBeGreaterThanOrEqual(24)

    // Verify hero subheading is visible
    const heroSubheading = page.locator('[data-testid="hero-section"] p')
    await expect(heroSubheading.first()).toBeVisible()

    // Check subheading font size
    const subheadingFontSize = await heroSubheading.first().evaluate((el) => {
      return window.getComputedStyle(el).fontSize
    })
    const subFontSizeValue = parseFloat(subheadingFontSize)
    expect(subFontSizeValue).toBeGreaterThanOrEqual(14) // At least 14px for body text

    // Verify feature card titles are readable
    const featureTitles = page.locator('[data-testid="feature-title"]')
    const titleCount = await featureTitles.count()
    expect(titleCount).toBeGreaterThan(0)

    for (let i = 0; i < titleCount; i++) {
      const title = featureTitles.nth(i)
      await expect(title).toBeVisible()

      const titleFontSize = await title.evaluate((el) => {
        return window.getComputedStyle(el).fontSize
      })
      const titleSize = parseFloat(titleFontSize)
      expect(titleSize).toBeGreaterThanOrEqual(16) // At least 16px for titles
    }

    // Verify feature descriptions are visible
    const featureDescriptions = page.locator('[data-testid="feature-description"]')
    const descCount = await featureDescriptions.count()
    expect(descCount).toBeGreaterThan(0)

    for (let i = 0; i < descCount; i++) {
      const desc = featureDescriptions.nth(i)
      await expect(desc).toBeVisible()
    }
  })

  /**
   * Test Case 3 (additional): Images/icons scale appropriately
   */
  test('images and icons scale appropriately at tablet viewport', async ({ page }) => {
    // Verify feature icons are visible and appropriately sized
    const featureIcons = page.locator('[data-testid="feature-icon"]')
    const iconCount = await featureIcons.count()
    expect(iconCount).toBeGreaterThan(0)

    for (let i = 0; i < iconCount; i++) {
      const icon = featureIcons.nth(i)
      await expect(icon).toBeVisible()

      const iconBox = await icon.boundingBox()
      expect(iconBox).not.toBeNull()

      if (iconBox) {
        // Icons should be reasonably sized (at least 24x24 for visibility)
        expect(iconBox.width).toBeGreaterThanOrEqual(24)
        expect(iconBox.height).toBeGreaterThanOrEqual(24)
      }
    }
  })

  /**
   * Additional test: CTA buttons are properly sized for touch at tablet
   */
  test('CTA buttons meet touch target size requirements at tablet viewport', async ({ page }) => {
    // Find primary CTA button
    const primaryCta = page.getByTestId('hero-primary-cta')
    await expect(primaryCta).toBeVisible()

    const primaryBox = await primaryCta.boundingBox()
    expect(primaryBox).not.toBeNull()
    if (primaryBox) {
      // WCAG 2.1 AAA recommends 44x44 minimum touch target
      expect(primaryBox.width).toBeGreaterThanOrEqual(44)
      expect(primaryBox.height).toBeGreaterThanOrEqual(44)
    }

    // Find secondary CTA button
    const secondaryCta = page.getByTestId('hero-login-cta')
    await expect(secondaryCta).toBeVisible()

    const secondaryBox = await secondaryCta.boundingBox()
    expect(secondaryBox).not.toBeNull()
    if (secondaryBox) {
      expect(secondaryBox.width).toBeGreaterThanOrEqual(44)
      expect(secondaryBox.height).toBeGreaterThanOrEqual(44)
    }
  })

  /**
   * Additional test: No horizontal scroll at tablet viewport
   */
  test('no horizontal scroll at tablet viewport', async ({ page }) => {
    // Get viewport and document widths
    const { viewportWidth, documentWidth } = await page.evaluate(() => {
      return {
        viewportWidth: window.innerWidth,
        documentWidth: document.documentElement.scrollWidth,
      }
    })

    // Document should not be wider than viewport (no horizontal scroll)
    expect(documentWidth).toBeLessThanOrEqual(viewportWidth)
  })

  /**
   * Additional test: Footer is visible and properly laid out at tablet
   */
  test('footer displays correctly at tablet viewport', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('footer')
    await footer.scrollIntoViewIfNeeded()
    await expect(footer).toBeVisible()

    // Verify footer links are visible
    const footerLinks = footer.locator('a')
    const linkCount = await footerLinks.count()
    expect(linkCount).toBeGreaterThan(0)

    // Verify all links are visible
    for (let i = 0; i < linkCount; i++) {
      await expect(footerLinks.nth(i)).toBeVisible()
    }
  })

  /**
   * Additional test: Hero CTAs are arranged properly at tablet
   */
  test('hero CTAs are properly arranged at tablet viewport', async ({ page }) => {
    const primaryCta = page.getByTestId('hero-primary-cta')
    const secondaryCta = page.getByTestId('hero-login-cta')

    await expect(primaryCta).toBeVisible()
    await expect(secondaryCta).toBeVisible()

    const primaryBox = await primaryCta.boundingBox()
    const secondaryBox = await secondaryCta.boundingBox()

    expect(primaryBox).not.toBeNull()
    expect(secondaryBox).not.toBeNull()

    if (primaryBox && secondaryBox) {
      // At tablet viewport, CTAs should be on the same row (flex container)
      const yDifference = Math.abs(primaryBox.y - secondaryBox.y)
      expect(yDifference).toBeLessThan(20) // Allow for small differences
    }
  })
})
