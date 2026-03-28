/**
 * E2E tests for mobile responsive design.
 * Owner: Scenario 7 - Responsive Design - Mobile
 *
 * Tests validate homepage displays correctly on mobile devices (375px-414px viewport)
 * with hamburger menu and proper layout.
 *
 * Test Cases:
 * 1. All content visible without horizontal scrolling at 375px
 * 2. Navigation displays hamburger menu icon instead of inline links
 * 3. Mobile menu expands showing all navigation items when clicked
 * 4. Feature cards stack vertically in single column
 * 5. CTA buttons have minimum tap target of 44x44 pixels
 * 6. Page displays correctly on larger mobile screens (414px)
 */

import { test, expect } from '@playwright/test'

// Test viewport sizes for mobile devices
const MOBILE_SMALL = { width: 375, height: 667 } // iPhone SE
const MOBILE_LARGE = { width: 414, height: 896 } // iPhone 11/12/13

test.describe('Responsive Design - Mobile', () => {
  test.describe('375px viewport (iPhone SE)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(MOBILE_SMALL)
      await page.goto('/')
      await page.waitForLoadState('networkidle')
    })

    test('TC1: All content is visible without horizontal scrolling', async ({ page }) => {
      // Check that the page width matches viewport (no horizontal overflow)
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth)
      const viewportWidth = await page.evaluate(() => window.innerWidth)

      // Body should not be wider than viewport (allowing small margin for scrollbar)
      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 1)

      // Verify no horizontal scrollbar is present
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth
      })
      expect(hasHorizontalScroll).toBe(false)

      // Verify main sections are visible
      await expect(page.getByTestId('home-page')).toBeVisible()
      await expect(page.getByTestId('hero-section')).toBeVisible()
      await expect(page.getByTestId('navbar')).toBeVisible()
    })

    test('TC2: Navigation displays hamburger menu icon instead of inline links', async ({ page }) => {
      // Hamburger menu toggle should be visible on mobile
      const mobileMenuToggle = page.getByTestId('mobile-menu-toggle')
      await expect(mobileMenuToggle).toBeVisible()

      // Desktop nav items should be hidden
      const desktopNavFeatures = page.getByTestId('nav-features')
      const desktopNavPricing = page.getByTestId('nav-pricing')
      const desktopNavAbout = page.getByTestId('nav-about')

      await expect(desktopNavFeatures).not.toBeVisible()
      await expect(desktopNavPricing).not.toBeVisible()
      await expect(desktopNavAbout).not.toBeVisible()

      // Desktop auth buttons should be hidden
      await expect(page.getByTestId('nav-login')).not.toBeVisible()
      await expect(page.getByTestId('nav-get-started')).not.toBeVisible()
    })

    test('TC3: Mobile menu expands showing all navigation items when clicked', async ({ page }) => {
      // Click hamburger menu
      const mobileMenuToggle = page.getByTestId('mobile-menu-toggle')
      await mobileMenuToggle.click()

      // Wait for mobile menu to appear
      const mobileMenu = page.getByTestId('mobile-menu')
      await expect(mobileMenu).toBeVisible()

      // Verify all navigation items are visible in mobile menu
      await expect(page.getByTestId('mobile-nav-features')).toBeVisible()
      await expect(page.getByTestId('mobile-nav-pricing')).toBeVisible()
      await expect(page.getByTestId('mobile-nav-about')).toBeVisible()
      await expect(page.getByTestId('mobile-nav-login')).toBeVisible()
      await expect(page.getByTestId('mobile-nav-get-started')).toBeVisible()

      // Verify menu can be closed
      await mobileMenuToggle.click()
      await expect(mobileMenu).not.toBeVisible()
    })

    test('TC4: Feature cards stack vertically in single column', async ({ page }) => {
      // Scroll to features section
      await page.evaluate(() => {
        document.querySelector('[data-testid="features-section"]')?.scrollIntoView()
      })

      // Wait for features section to be visible
      await page.waitForTimeout(300)

      // Get all feature cards
      const featureCards = page.locator('.card')
      const cardCount = await featureCards.count()

      expect(cardCount).toBeGreaterThanOrEqual(3)

      // Check that cards are stacked vertically (each card's left position should be similar)
      const cardPositions: { top: number; left: number }[] = []

      for (let i = 0; i < Math.min(cardCount, 3); i++) {
        const card = featureCards.nth(i)
        const boundingBox = await card.boundingBox()
        if (boundingBox) {
          cardPositions.push({ top: boundingBox.y, left: boundingBox.x })
        }
      }

      // In single column layout, all cards should have similar left position
      if (cardPositions.length >= 2) {
        const leftPositions = cardPositions.map(p => p.left)
        const leftVariance = Math.max(...leftPositions) - Math.min(...leftPositions)
        // Allow small variance for padding differences
        expect(leftVariance).toBeLessThan(50)
      }

      // Cards should be stacked vertically (increasing top positions)
      for (let i = 1; i < cardPositions.length; i++) {
        expect(cardPositions[i].top).toBeGreaterThan(cardPositions[i - 1].top)
      }
    })

    test('TC5: CTA buttons have minimum tap target of 44x44 pixels', async ({ page }) => {
      // Check primary CTA button
      const primaryCta = page.getByTestId('primary-cta')
      await expect(primaryCta).toBeVisible()

      const primaryCtaBox = await primaryCta.boundingBox()
      expect(primaryCtaBox).not.toBeNull()
      if (primaryCtaBox) {
        expect(primaryCtaBox.width).toBeGreaterThanOrEqual(44)
        expect(primaryCtaBox.height).toBeGreaterThanOrEqual(44)
      }

      // Check secondary CTA button
      const secondaryCta = page.getByTestId('secondary-cta')
      await expect(secondaryCta).toBeVisible()

      const secondaryCtaBox = await secondaryCta.boundingBox()
      expect(secondaryCtaBox).not.toBeNull()
      if (secondaryCtaBox) {
        expect(secondaryCtaBox.width).toBeGreaterThanOrEqual(44)
        expect(secondaryCtaBox.height).toBeGreaterThanOrEqual(44)
      }

      // Check hamburger menu button
      const menuToggle = page.getByTestId('mobile-menu-toggle')
      await expect(menuToggle).toBeVisible()

      const menuToggleBox = await menuToggle.boundingBox()
      expect(menuToggleBox).not.toBeNull()
      if (menuToggleBox) {
        expect(menuToggleBox.width).toBeGreaterThanOrEqual(44)
        expect(menuToggleBox.height).toBeGreaterThanOrEqual(44)
      }
    })
  })

  test.describe('414px viewport (iPhone 11/12/13)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(MOBILE_LARGE)
      await page.goto('/')
      await page.waitForLoadState('networkidle')
    })

    test('TC6: Page displays correctly on larger mobile screens', async ({ page }) => {
      // Check that the page width matches viewport (no horizontal overflow)
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth)
      const viewportWidth = await page.evaluate(() => window.innerWidth)

      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 1)

      // Verify no horizontal scrollbar
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth
      })
      expect(hasHorizontalScroll).toBe(false)

      // Verify main sections are visible
      await expect(page.getByTestId('home-page')).toBeVisible()
      await expect(page.getByTestId('hero-section')).toBeVisible()
      await expect(page.getByTestId('navbar')).toBeVisible()

      // Hamburger menu should still be visible at 414px (mobile breakpoint)
      const mobileMenuToggle = page.getByTestId('mobile-menu-toggle')
      await expect(mobileMenuToggle).toBeVisible()

      // Desktop nav should still be hidden
      await expect(page.getByTestId('nav-features')).not.toBeVisible()

      // Click hamburger and verify menu works
      await mobileMenuToggle.click()
      await expect(page.getByTestId('mobile-menu')).toBeVisible()
      await expect(page.getByTestId('mobile-nav-features')).toBeVisible()

      // CTA buttons should have proper tap targets
      const primaryCta = page.getByTestId('primary-cta')
      const primaryCtaBox = await primaryCta.boundingBox()
      expect(primaryCtaBox).not.toBeNull()
      if (primaryCtaBox) {
        expect(primaryCtaBox.width).toBeGreaterThanOrEqual(44)
        expect(primaryCtaBox.height).toBeGreaterThanOrEqual(44)
      }
    })
  })

  test.describe('Mobile interaction tests', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize(MOBILE_SMALL)
      await page.goto('/')
      await page.waitForLoadState('networkidle')
    })

    test('Mobile menu navigation links are clickable', async ({ page }) => {
      // Open mobile menu
      await page.getByTestId('mobile-menu-toggle').click()
      await expect(page.getByTestId('mobile-menu')).toBeVisible()

      // Click a navigation link
      const featuresLink = page.getByTestId('mobile-nav-features')
      await expect(featuresLink).toBeVisible()

      // Verify the link has proper href
      const href = await featuresLink.getAttribute('href')
      expect(href).toBe('/features')
    })

    test('Hero section content is readable on mobile', async ({ page }) => {
      const heroSection = page.getByTestId('hero-section')
      await expect(heroSection).toBeVisible()

      // Check that headline is visible
      const headline = heroSection.locator('h1')
      await expect(headline).toBeVisible()

      // Check that subheading is visible
      const subheading = heroSection.locator('h2')
      await expect(subheading).toBeVisible()

      // Verify text is within viewport bounds
      const headlineBox = await headline.boundingBox()
      expect(headlineBox).not.toBeNull()
      if (headlineBox) {
        expect(headlineBox.x).toBeGreaterThanOrEqual(0)
        expect(headlineBox.x + headlineBox.width).toBeLessThanOrEqual(MOBILE_SMALL.width)
      }
    })

    test('Navbar logo is visible on mobile', async ({ page }) => {
      const logo = page.getByTestId('navbar-logo')
      await expect(logo).toBeVisible()

      // Logo should be clickable
      const logoBox = await logo.boundingBox()
      expect(logoBox).not.toBeNull()
    })
  })
})
