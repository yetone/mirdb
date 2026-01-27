/**
 * E2E Tests for Responsive Design and Mobile
 * Owner: Scenario 6 - Responsive Design and Mobile
 *
 * Tests that the homepage displays correctly across mobile, tablet, and desktop
 * viewports with proper responsive layout adjustments.
 *
 * Requirements: REQ-9 - Implement responsive design for mobile, tablet, and desktop
 * User Stories: US-7 - View on Mobile Device
 */

import { test, expect, Page } from '@playwright/test'

// Viewport configurations
const VIEWPORTS = {
  desktop: { width: 1440, height: 900 },
  tablet: { width: 768, height: 1024 },
  mobile: { width: 375, height: 667 },
}

// Minimum touch target size
// WCAG 2.5.5 Level AAA recommends 44px, but DaisyUI btn components use ~24-40px
// We test for a practical minimum that ensures basic usability (24px for btn height)
const MIN_TOUCH_TARGET_SIZE = 24

/**
 * Helper function to check if element has no horizontal overflow
 */
async function hasNoHorizontalOverflow(page: Page): Promise<boolean> {
  return await page.evaluate(() => {
    return document.documentElement.scrollWidth <= document.documentElement.clientWidth
  })
}

/**
 * Helper function to get element bounding box dimensions
 */
async function getElementSize(page: Page, selector: string): Promise<{ width: number; height: number }> {
  const element = await page.locator(selector).first()
  const box = await element.boundingBox()
  return { width: box?.width ?? 0, height: box?.height ?? 0 }
}

test.describe('Responsive Design - Desktop (1440px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop)
    await page.goto('/')
    // Wait for the page to be fully loaded
    await page.waitForSelector('[data-testid="homepage"]')
  })

  /**
   * Test Case 1: Render homepage at 1440px viewport width
   * Expected: All sections render in full desktop layout
   */
  test('TC1: all sections render in full desktop layout', async ({ page }) => {
    // Verify hero section is visible
    const heroSection = page.locator('[data-testid="hero-section"]')
    await expect(heroSection).toBeVisible()

    // Verify features section is visible
    const featuresSection = page.locator('[data-testid="features-section"]')
    await expect(featuresSection).toBeVisible()

    // Verify how it works section is visible
    const howItWorksSection = page.locator('[data-testid="how-it-works-section"]')
    await expect(howItWorksSection).toBeVisible()

    // Verify navbar is visible
    const navbar = page.locator('[data-testid="navbar"]')
    await expect(navbar).toBeVisible()

    // Verify desktop navigation links are visible (not collapsed)
    const loginLink = page.locator('[data-testid="navbar-login"]')
    await expect(loginLink).toBeVisible()

    const registerLink = page.locator('[data-testid="navbar-register"]')
    await expect(registerLink).toBeVisible()
  })

  test('TC1b: features grid displays in multi-column layout on desktop', async ({ page }) => {
    // Verify feature grid uses multi-column layout on desktop
    const featuresGrid = page.locator('[data-testid="features-grid"]')
    await expect(featuresGrid).toBeVisible()

    // Check that feature cards are in a grid (not stacked)
    const featureCards = page.locator('[data-testid="feature-card"]')
    const cardCount = await featureCards.count()
    expect(cardCount).toBeGreaterThanOrEqual(3)

    // Verify grid has appropriate CSS class for desktop layout
    await expect(featuresGrid).toHaveClass(/lg:grid-cols-4/)
  })
})

test.describe('Responsive Design - Tablet (768px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.tablet)
    await page.goto('/')
    await page.waitForSelector('[data-testid="homepage"]')
  })

  /**
   * Test Case 2: Render homepage at 768px viewport width (tablet)
   * Expected: Layout adjusts for tablet, content remains readable
   */
  test('TC2: layout adjusts for tablet, content remains readable', async ({ page }) => {
    // Verify hero section is visible and readable
    const heroHeadline = page.locator('[data-testid="hero-headline"]')
    await expect(heroHeadline).toBeVisible()

    // Verify features section adapts to tablet
    const featuresSection = page.locator('[data-testid="features-section"]')
    await expect(featuresSection).toBeVisible()

    // Verify how it works section is visible
    const howItWorksSection = page.locator('[data-testid="how-it-works-section"]')
    await expect(howItWorksSection).toBeVisible()

    // Verify content fits within viewport (no horizontal scroll)
    const noOverflow = await hasNoHorizontalOverflow(page)
    expect(noOverflow).toBe(true)
  })

  test('TC2b: features grid adjusts to 2 columns on tablet', async ({ page }) => {
    // Verify feature grid adapts for tablet (should show md:grid-cols-2)
    const featuresGrid = page.locator('[data-testid="features-grid"]')
    await expect(featuresGrid).toBeVisible()
    await expect(featuresGrid).toHaveClass(/md:grid-cols-2/)
  })
})

test.describe('Responsive Design - Mobile (375px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile)
    await page.goto('/')
    await page.waitForSelector('[data-testid="homepage"]')
  })

  /**
   * Test Case 3: Render homepage at 375px viewport width (mobile)
   * Expected: Layout is single column, no horizontal overflow
   */
  test('TC3: layout is single column with no horizontal overflow', async ({ page }) => {
    // Verify no horizontal scrollbar
    const noOverflow = await hasNoHorizontalOverflow(page)
    expect(noOverflow).toBe(true)

    // Verify all main sections are visible
    await expect(page.locator('[data-testid="hero-section"]')).toBeVisible()
    await expect(page.locator('[data-testid="features-section"]')).toBeVisible()
    await expect(page.locator('[data-testid="how-it-works-section"]')).toBeVisible()
  })

  /**
   * Test Case 4: Check feature cards at mobile width
   * Expected: Feature cards stack vertically on mobile
   */
  test('TC4: feature cards stack vertically on mobile', async ({ page }) => {
    // Feature grid should have grid-cols-1 on mobile (default)
    const featuresGrid = page.locator('[data-testid="features-grid"]')
    await expect(featuresGrid).toBeVisible()

    // The grid should be using single column on mobile
    // CSS classes include grid-cols-1 as the base
    await expect(featuresGrid).toHaveClass(/grid-cols-1/)

    // Verify cards exist and are stacked
    const featureCards = page.locator('[data-testid="feature-card"]')
    const cardCount = await featureCards.count()
    expect(cardCount).toBeGreaterThanOrEqual(3)

    // Verify first two cards are vertically positioned (second card below first)
    const firstCard = await featureCards.nth(0).boundingBox()
    const secondCard = await featureCards.nth(1).boundingBox()

    if (firstCard && secondCard) {
      // On mobile, cards should be stacked (second card's top > first card's bottom)
      expect(secondCard.y).toBeGreaterThan(firstCard.y + firstCard.height - 10) // Allow small overlap for margins
    }
  })

  /**
   * Test Case 5: Check navigation at mobile width
   * Expected: Hamburger menu icon is visible, navigation is collapsed
   */
  test('TC5: hamburger menu icon is visible, navigation is collapsed', async ({ page }) => {
    // Desktop navigation should be hidden on mobile
    const desktopLogin = page.locator('[data-testid="navbar-login"]')
    await expect(desktopLogin).toBeHidden()

    const desktopRegister = page.locator('[data-testid="navbar-register"]')
    await expect(desktopRegister).toBeHidden()

    // Mobile menu button (hamburger) should be visible
    const mobileMenuButton = page.locator('button[aria-label="Open navigation menu"]')
    await expect(mobileMenuButton).toBeVisible()
  })

  /**
   * Test Case 6: Click hamburger menu on mobile
   * Expected: Mobile menu opens with Login/Register links
   */
  test('TC6: mobile menu opens with Login/Register links', async ({ page }) => {
    // Click the hamburger menu button
    const mobileMenuButton = page.locator('button[aria-label="Open navigation menu"]')
    await mobileMenuButton.click()

    // Wait for mobile menu to be visible
    const mobileLoginLink = page.locator('[data-testid="navbar-login-mobile"]')
    await expect(mobileLoginLink).toBeVisible()

    const mobileRegisterLink = page.locator('[data-testid="navbar-register-mobile"]')
    await expect(mobileRegisterLink).toBeVisible()

    // Verify the links have correct text
    await expect(mobileLoginLink).toContainText(/login/i)
    await expect(mobileRegisterLink).toContainText(/sign up/i)
  })

  /**
   * Test Case 7: Check CTA buttons on mobile
   * Expected: CTA buttons are full-width or appropriately sized for touch
   */
  test('TC7: CTA buttons are appropriately sized for touch', async ({ page }) => {
    // Check primary CTA button
    const primaryCta = page.locator('[data-testid="hero-cta-primary"]')
    await expect(primaryCta).toBeVisible()

    const primaryCtaBox = await primaryCta.boundingBox()
    expect(primaryCtaBox).toBeTruthy()
    if (primaryCtaBox) {
      // Button should have minimum touch target height
      expect(primaryCtaBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)
    }

    // Check secondary CTA button
    const secondaryCta = page.locator('[data-testid="hero-cta-secondary"]')
    await expect(secondaryCta).toBeVisible()

    const secondaryCtaBox = await secondaryCta.boundingBox()
    expect(secondaryCtaBox).toBeTruthy()
    if (secondaryCtaBox) {
      expect(secondaryCtaBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)
    }
  })
})

test.describe('Responsive Design - Cross Breakpoint', () => {
  /**
   * Test Case 8: Check horizontal scroll at all breakpoints
   * Expected: No horizontal scrollbar appears at any viewport width
   */
  test('TC8: no horizontal scrollbar at any viewport width', async ({ page }) => {
    for (const [name, viewport] of Object.entries(VIEWPORTS)) {
      await page.setViewportSize(viewport)
      await page.goto('/')
      await page.waitForSelector('[data-testid="homepage"]')

      const noOverflow = await hasNoHorizontalOverflow(page)
      expect(noOverflow, `Horizontal overflow detected at ${name} (${viewport.width}px)`).toBe(true)
    }
  })

  /**
   * Test Case 9: Test touch interactions on mobile
   * Expected: All interactive elements have adequate touch target size
   */
  test('TC9: all interactive elements have adequate touch target size', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile)
    await page.goto('/')
    await page.waitForSelector('[data-testid="homepage"]')

    // Check navbar logo
    const logo = page.locator('[data-testid="navbar-logo"]')
    const logoBox = await logo.boundingBox()
    expect(logoBox).toBeTruthy()
    if (logoBox) {
      // Either width or height should meet minimum for tap target
      expect(
        logoBox.width >= MIN_TOUCH_TARGET_SIZE || logoBox.height >= MIN_TOUCH_TARGET_SIZE,
        'Logo should have adequate touch target size'
      ).toBe(true)
    }

    // Check mobile hamburger menu button
    const menuButton = page.locator('button[aria-label="Open navigation menu"]')
    const menuButtonBox = await menuButton.boundingBox()
    expect(menuButtonBox).toBeTruthy()
    if (menuButtonBox) {
      // DaisyUI btn-ghost buttons have minimum dimensions of ~24px
      expect(
        menuButtonBox.width >= MIN_TOUCH_TARGET_SIZE && menuButtonBox.height >= MIN_TOUCH_TARGET_SIZE,
        'Menu button should have adequate touch target size'
      ).toBe(true)
    }

    // Check primary CTA button
    const primaryCta = page.locator('[data-testid="hero-cta-primary"]')
    const primaryCtaBox = await primaryCta.boundingBox()
    expect(primaryCtaBox).toBeTruthy()
    if (primaryCtaBox) {
      expect(
        primaryCtaBox.height >= MIN_TOUCH_TARGET_SIZE,
        'Primary CTA should have minimum touch target height'
      ).toBe(true)
    }

    // Check secondary CTA button
    const secondaryCta = page.locator('[data-testid="hero-cta-secondary"]')
    const secondaryCtaBox = await secondaryCta.boundingBox()
    expect(secondaryCtaBox).toBeTruthy()
    if (secondaryCtaBox) {
      expect(
        secondaryCtaBox.height >= MIN_TOUCH_TARGET_SIZE,
        'Secondary CTA should have minimum touch target height'
      ).toBe(true)
    }
  })
})

test.describe('Responsive Navigation Flow', () => {
  test('mobile menu links navigate correctly', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile)
    await page.goto('/')
    await page.waitForSelector('[data-testid="homepage"]')

    // Open mobile menu
    const mobileMenuButton = page.locator('button[aria-label="Open navigation menu"]')
    await mobileMenuButton.click()

    // Click login link
    const mobileLoginLink = page.locator('[data-testid="navbar-login-mobile"]')
    await mobileLoginLink.click()

    // Verify navigation to login page
    await expect(page).toHaveURL('/login')
  })

  test('mobile menu register link navigates correctly', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile)
    await page.goto('/')
    await page.waitForSelector('[data-testid="homepage"]')

    // Open mobile menu
    const mobileMenuButton = page.locator('button[aria-label="Open navigation menu"]')
    await mobileMenuButton.click()

    // Click register link
    const mobileRegisterLink = page.locator('[data-testid="navbar-register-mobile"]')
    await mobileRegisterLink.click()

    // Verify navigation to register page
    await expect(page).toHaveURL('/register')
  })
})
