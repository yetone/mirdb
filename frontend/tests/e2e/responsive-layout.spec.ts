/**
 * E2E Responsive Layout Tests
 *
 * Tests responsive behavior of the homepage across different viewport sizes:
 * - Scenario 8: Mobile Responsive Layout (viewport < 768px)
 * - Scenario 9: Tablet Responsive Layout (768px-1023px)
 * - Scenario 10: Desktop Layout (viewport >= 1024px)
 */

import { test, expect } from '@playwright/test'

// ============================================================================
// SCENARIO 8: Mobile Responsive Layout Tests (320px-767px)
// ============================================================================

test.describe('Mobile Responsive Layout (320px-767px)', () => {
  test.describe.configure({ mode: 'serial' })

  test.beforeEach(async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 320, height: 568 })
    await page.goto('/')
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle')
  })

  test('Test Case 1: All content visible without horizontal scroll at 320px viewport', async ({
    page,
  }) => {
    // Verify viewport is set correctly
    const viewport = page.viewportSize()
    expect(viewport?.width).toBe(320)

    // Check that there's no horizontal scrollbar
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth
    })
    expect(hasHorizontalScroll).toBe(false)

    // Verify hero section is visible
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Verify features section is visible
    const featuresGrid = page.getByTestId('features-grid')
    await expect(featuresGrid).toBeVisible()

    // Verify URL demo section is visible
    const urlDemoSection = page.getByTestId('url-demo-section')
    await expect(urlDemoSection).toBeVisible()

    // Verify footer is visible
    const footer = page.getByTestId('footer')
    await expect(footer).toBeVisible()

    // Ensure all content fits within viewport width
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth)
    expect(bodyScrollWidth).toBeLessThanOrEqual(320)
  })

  test('Test Case 5: URL demo input is usable and appropriately sized for mobile keyboards', async ({
    page,
  }) => {
    // Navigate to URL demo section
    const urlInput = page.getByTestId('url-input')
    await urlInput.scrollIntoViewIfNeeded()
    await expect(urlInput).toBeVisible()

    // Check that input has adequate width for mobile
    const inputBox = await urlInput.boundingBox()
    expect(inputBox).not.toBeNull()
    if (inputBox) {
      // Input should take most of the container width (at least 70% of 320px = 224px)
      // Accounting for container padding and margins
      expect(inputBox.width).toBeGreaterThanOrEqual(220)
    }

    // Test that input is focusable and typeable
    await urlInput.click()
    await urlInput.fill('https://example.com/test-url')

    // Verify the text was entered
    const inputValue = await urlInput.inputValue()
    expect(inputValue).toBe('https://example.com/test-url')

    // Verify shorten button is visible and accessible
    const shortenButton = page.getByTestId('shorten-button')
    await expect(shortenButton).toBeVisible()

    // Check button has adequate touch target
    const buttonBox = await shortenButton.boundingBox()
    expect(buttonBox).not.toBeNull()
    if (buttonBox) {
      expect(buttonBox.height).toBeGreaterThanOrEqual(44)
    }
  })

  test('Hero section adapts to mobile layout', async ({ page }) => {
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Check primary CTA button
    const primaryCta = page.getByTestId('hero-primary-cta')
    await expect(primaryCta).toBeVisible()

    // Check login CTA button
    const loginCta = page.getByTestId('hero-login-cta')
    await expect(loginCta).toBeVisible()

    // Get bounding boxes to verify layout
    const primaryBox = await primaryCta.boundingBox()
    const loginBox = await loginCta.boundingBox()

    expect(primaryBox).not.toBeNull()
    expect(loginBox).not.toBeNull()

    // At mobile sizes, buttons should be visible and accessible
    if (primaryBox && loginBox) {
      // Both buttons should have adequate touch targets
      expect(primaryBox.height).toBeGreaterThanOrEqual(44)
      expect(loginBox.height).toBeGreaterThanOrEqual(44)
    }
  })

  test('Feature cards display in single column layout on mobile', async ({ page }) => {
    const featuresGrid = page.getByTestId('features-grid')
    await featuresGrid.scrollIntoViewIfNeeded()
    await expect(featuresGrid).toBeVisible()

    // Get all feature cards
    const featureCards = await page.locator('[data-testid^="feature-card-"]').all()
    expect(featureCards.length).toBeGreaterThan(0)

    // On mobile, cards should stack vertically (single column)
    // Verify by checking that each card takes full width and cards are vertically stacked
    const cardBoxes = await Promise.all(featureCards.map((card) => card.boundingBox()))

    // Check that cards are stacked (y positions increase)
    for (let i = 1; i < cardBoxes.length; i++) {
      const prevBox = cardBoxes[i - 1]
      const currentBox = cardBoxes[i]
      if (prevBox && currentBox) {
        // Current card should be below previous card
        expect(currentBox.y).toBeGreaterThan(prevBox.y)
      }
    }
  })

  test('All interactive elements have adequate touch target size (44px minimum)', async ({
    page,
  }) => {
    // Test CTA buttons in hero
    const primaryCta = page.getByTestId('hero-primary-cta')
    const loginCta = page.getByTestId('hero-login-cta')

    const primaryBox = await primaryCta.boundingBox()
    const loginBox = await loginCta.boundingBox()

    expect(primaryBox).not.toBeNull()
    expect(loginBox).not.toBeNull()

    if (primaryBox) {
      expect(primaryBox.height).toBeGreaterThanOrEqual(44)
    }
    if (loginBox) {
      expect(loginBox.height).toBeGreaterThanOrEqual(44)
    }

    // Test shorten button in URL demo
    const shortenButton = page.getByTestId('shorten-button')
    await shortenButton.scrollIntoViewIfNeeded()
    const shortenBox = await shortenButton.boundingBox()

    expect(shortenBox).not.toBeNull()
    if (shortenBox) {
      expect(shortenBox.height).toBeGreaterThanOrEqual(44)
    }

    // Test footer navigation links
    const footerLinkHome = page.getByTestId('footer-link-home')
    const footerLinkLogin = page.getByTestId('footer-link-login')
    const footerLinkRegister = page.getByTestId('footer-link-register')

    await footerLinkHome.scrollIntoViewIfNeeded()

    // For links, we check that they are at least accessible and visible
    await expect(footerLinkHome).toBeVisible()
    await expect(footerLinkLogin).toBeVisible()
    await expect(footerLinkRegister).toBeVisible()
  })

  test('Navigation adapts to mobile viewport', async ({ page }) => {
    // The navbar should be visible
    const navbar = page.locator('nav').first()
    await expect(navbar).toBeVisible()

    // Content should not overflow
    const hasOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > window.innerWidth
    })
    expect(hasOverflow).toBe(false)
  })

  test('Footer displays correctly on mobile', async ({ page }) => {
    const footer = page.getByTestId('footer')
    await footer.scrollIntoViewIfNeeded()
    await expect(footer).toBeVisible()

    // Check footer navigation
    const footerNav = page.getByTestId('footer-nav')
    await expect(footerNav).toBeVisible()

    // Check footer legal links
    const footerLegal = page.getByTestId('footer-legal')
    await expect(footerLegal).toBeVisible()

    // Check copyright
    const footerCopyright = page.getByTestId('footer-copyright')
    await expect(footerCopyright).toBeVisible()
  })

  test('Text is readable at mobile viewport', async ({ page }) => {
    // Check main heading is visible and not truncated
    const heading = page.locator('h1')
    await expect(heading).toBeVisible()

    // Get heading bounding box
    const headingBox = await heading.boundingBox()
    expect(headingBox).not.toBeNull()

    if (headingBox) {
      // Heading should be within viewport
      expect(headingBox.x).toBeGreaterThanOrEqual(0)
      expect(headingBox.x + headingBox.width).toBeLessThanOrEqual(320)
    }
  })
})

// ============================================================================
// SCENARIO 9: Tablet Responsive Layout Tests (768px-1023px)
// ============================================================================

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

// ============================================================================
// SCENARIO 10: Desktop Layout Tests (1024px+)
// Placeholder for Scenario 10 builder
// ============================================================================
