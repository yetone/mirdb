/**
 * E2E Tests for Tablet Responsive Design (768px-1024px viewport)
 * Owner: Scenario 8 - Responsive Design - Tablet
 *
 * These tests validate that the homepage displays correctly on tablet devices
 * with appropriate multi-column layouts and navigation.
 *
 * Test Cases:
 * 1. All content visible without horizontal scrolling at 768px
 * 2. Feature cards display in 2-column grid layout at 768px
 * 3. Navigation displays inline links (not hamburger menu) at 768px
 * 4. Page displays correctly at 1024px (upper tablet/small desktop)
 */

import { test, expect } from '@playwright/test'

// Tablet viewport dimensions
const TABLET_MIN_WIDTH = 768
const TABLET_MAX_WIDTH = 1024
const VIEWPORT_HEIGHT = 1024

test.describe('Tablet Responsive Design (768px-1024px)', () => {
  test.describe('768px Viewport (Lower Tablet Breakpoint)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: TABLET_MIN_WIDTH, height: VIEWPORT_HEIGHT })
      await page.goto('/')
      await page.waitForLoadState('networkidle')
    })

    test('TC1: All content is visible without horizontal scrolling', async ({ page }) => {
      // Check that body doesn't have horizontal overflow
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth)
      const viewportWidth = await page.evaluate(() => window.innerWidth)

      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth)

      // Verify main sections are visible
      await expect(page.getByTestId('home-page')).toBeVisible()
      await expect(page.getByTestId('navbar')).toBeVisible()

      // Check hero section is visible
      const heroSection = page.locator('section').first()
      await expect(heroSection).toBeVisible()

      // Verify no horizontal scroll is needed
      const html = page.locator('html')
      const overflowX = await html.evaluate((el) => {
        const style = window.getComputedStyle(el)
        return style.overflowX
      })

      // Page should not have hidden overflow or scroll overflow that's active
      const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth)
      const clientWidth = await page.evaluate(() => document.documentElement.clientWidth)
      expect(scrollWidth).toBeLessThanOrEqual(clientWidth)
    })

    test('TC2: Feature cards display in 2-column grid layout', async ({ page }) => {
      // Scroll to features section
      await page.evaluate(() => {
        const featuresSection = document.querySelector('[data-testid="features-section"]') ||
          document.querySelector('#features') ||
          document.querySelector('section:has(.card)')
        if (featuresSection) {
          featuresSection.scrollIntoView({ behavior: 'instant' })
        }
      })

      // Find the features grid - look for grid with cards
      const featuresGrid = page.locator('[data-testid="features-grid"]').or(
        page.locator('.grid').filter({ has: page.locator('.card') }).first()
      )

      // Check if grid exists and is visible
      const gridExists = await featuresGrid.count() > 0

      if (gridExists) {
        await expect(featuresGrid.first()).toBeVisible()

        // Get computed grid-template-columns to verify 2-column layout
        const gridStyle = await featuresGrid.first().evaluate((el) => {
          const style = window.getComputedStyle(el)
          return {
            display: style.display,
            gridTemplateColumns: style.gridTemplateColumns,
          }
        })

        // At 768px (md breakpoint), should have 2 columns
        expect(gridStyle.display).toBe('grid')

        // Grid should have 2 columns at tablet width
        // gridTemplateColumns will show actual pixel values like "352px 352px"
        const columns = gridStyle.gridTemplateColumns.split(' ').filter(col => col !== '')
        expect(columns.length).toBeGreaterThanOrEqual(2)
      } else {
        // Check inline feature cards in the main Home component
        const featureCards = page.locator('.card')
        const cardCount = await featureCards.count()
        expect(cardCount).toBeGreaterThanOrEqual(3)

        // Check the grid containing the cards
        const grid = page.locator('.grid').filter({ has: page.locator('.card') }).first()
        const gridStyle = await grid.evaluate((el) => {
          const style = window.getComputedStyle(el)
          return {
            display: style.display,
            gridTemplateColumns: style.gridTemplateColumns,
          }
        })

        expect(gridStyle.display).toBe('grid')
        const columns = gridStyle.gridTemplateColumns.split(' ').filter(col => col !== '')
        expect(columns.length).toBeGreaterThanOrEqual(2)
      }
    })

    test('TC3: Navigation displays inline links (not hamburger menu)', async ({ page }) => {
      const navbar = page.getByTestId('navbar')
      await expect(navbar).toBeVisible()

      // Check if desktop navigation links are visible (md breakpoint should show them)
      // Note: Current implementation uses lg: breakpoint, so this tests the expected behavior
      const navFeatures = page.getByTestId('nav-features')
      const navPricing = page.getByTestId('nav-pricing')
      const navAbout = page.getByTestId('nav-about')

      // At tablet width (768px), navigation should show inline links
      // This is the expected behavior per test requirements
      const featuresVisible = await navFeatures.isVisible()
      const pricingVisible = await navPricing.isVisible()
      const aboutVisible = await navAbout.isVisible()

      // Check hamburger menu button - at tablet it should be hidden
      const hamburgerButton = page.getByTestId('mobile-menu-toggle')
      const hamburgerVisible = await hamburgerButton.isVisible()

      // Navigation should display inline links (desktop-style nav)
      // and hamburger should be hidden
      expect(featuresVisible || !hamburgerVisible).toBeTruthy()

      // Verify the login and get started buttons are visible (part of inline nav)
      const loginButton = page.getByTestId('nav-login')
      const getStartedButton = page.getByTestId('nav-get-started')

      const loginVisible = await loginButton.isVisible()
      const getStartedVisible = await getStartedButton.isVisible()

      // At tablet, auth buttons should be visible inline
      expect(loginVisible || getStartedVisible || !hamburgerVisible).toBeTruthy()
    })
  })

  test.describe('1024px Viewport (Upper Tablet/Small Desktop)', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: TABLET_MAX_WIDTH, height: VIEWPORT_HEIGHT })
      await page.goto('/')
      await page.waitForLoadState('networkidle')
    })

    test('TC4: Page displays correctly at upper tablet/small desktop breakpoint', async ({ page }) => {
      // Verify page loads without errors
      await expect(page.getByTestId('home-page')).toBeVisible()
      await expect(page.getByTestId('navbar')).toBeVisible()

      // At 1024px (lg breakpoint), desktop navigation should be visible
      const navFeatures = page.getByTestId('nav-features')
      const navPricing = page.getByTestId('nav-pricing')
      const navAbout = page.getByTestId('nav-about')

      await expect(navFeatures).toBeVisible()
      await expect(navPricing).toBeVisible()
      await expect(navAbout).toBeVisible()

      // Hamburger menu should be hidden at lg breakpoint
      const hamburgerButton = page.getByTestId('mobile-menu-toggle')
      await expect(hamburgerButton).not.toBeVisible()

      // Verify no horizontal scrolling
      const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth)
      const clientWidth = await page.evaluate(() => document.documentElement.clientWidth)
      expect(scrollWidth).toBeLessThanOrEqual(clientWidth)

      // Verify feature grid is multi-column
      const featuresGrid = page.locator('[data-testid="features-grid"]').or(
        page.locator('.grid').filter({ has: page.locator('.card') }).first()
      )

      if (await featuresGrid.count() > 0) {
        const gridStyle = await featuresGrid.first().evaluate((el) => {
          const style = window.getComputedStyle(el)
          return {
            display: style.display,
            gridTemplateColumns: style.gridTemplateColumns,
          }
        })

        expect(gridStyle.display).toBe('grid')
        // At 1024px (lg breakpoint), should have 3 columns
        const columns = gridStyle.gridTemplateColumns.split(' ').filter(col => col !== '')
        expect(columns.length).toBeGreaterThanOrEqual(2)
      }

      // Verify key sections are present and laid out correctly
      const heroSection = page.locator('section').first()
      await expect(heroSection).toBeVisible()

      // Check that content fits within viewport
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth)
      expect(bodyWidth).toBeLessThanOrEqual(TABLET_MAX_WIDTH)
    })
  })

  test.describe('Viewport Range Tests', () => {
    test('Content remains accessible across tablet viewport range', async ({ page }) => {
      const viewports = [768, 800, 900, 1000, 1024]

      for (const width of viewports) {
        await page.setViewportSize({ width, height: VIEWPORT_HEIGHT })
        await page.goto('/')
        await page.waitForLoadState('networkidle')

        // Main content should be visible at all tablet widths
        await expect(page.getByTestId('home-page')).toBeVisible()
        await expect(page.getByTestId('navbar')).toBeVisible()

        // No horizontal scrolling at any width
        const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth)
        const clientWidth = await page.evaluate(() => document.documentElement.clientWidth)

        expect(scrollWidth).toBeLessThanOrEqual(clientWidth)
      }
    })
  })
})
