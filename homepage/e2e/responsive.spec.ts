/**
 * Responsive design E2E tests for MirDB homepage.
 * Owners:
 * - Scenario 9: Desktop viewport tests
 * - Scenario 10: Tablet viewport tests
 * - Scenario 11: Mobile viewport tests
 *
 * Requirements:
 * - Desktop (1920x1080): 3-column feature grid
 * - Tablet (768x1024): 2-column feature grid
 * - Mobile (375x667): Single-column layout
 * - No horizontal scrolling at any viewport (REQ-9)
 *
 * Test structure:
 * - describe('Desktop viewport')
 * - describe('Tablet viewport')
 * - describe('Mobile viewport')
 */

import { test, expect } from '@playwright/test'

/**
 * Tablet Viewport Tests (Scenario 10)
 * Tests responsive design at tablet viewport size (768x1024)
 */
test.describe('Tablet viewport', () => {
  test.beforeEach(async ({ page }) => {
    // Set tablet viewport (768x1024 - standard iPad dimensions)
    await page.setViewportSize({ width: 768, height: 1024 })
    await page.goto('/')
  })

  test('should render page with tablet-optimized layout', async ({ page }) => {
    // Test case 1: Page renders with tablet-optimized layout
    // Verify page loads successfully
    await expect(page).toHaveTitle(/MirDB/i)

    // Verify main content is visible
    const header = page.locator('header')
    await expect(header).toBeVisible()

    const hero = page.locator('#hero')
    await expect(hero).toBeVisible()

    const features = page.locator('#features')
    await expect(features).toBeVisible()

    // Verify the layout adapts (no desktop-only elements visible incorrectly)
    const mainContent = page.locator('#main-content')
    await expect(mainContent).toBeVisible()
  })

  test('should display features in 2-column grid on tablet', async ({ page }) => {
    // Test case 2: Features display in 2-column grid on tablet
    const featuresGrid = page.locator('[data-testid="features-grid"]')
    await expect(featuresGrid).toBeVisible()

    // Check that the grid has 2 columns at tablet width (768px is md breakpoint)
    // md:grid-cols-2 should be applied
    const gridComputedStyle = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns,
      }
    })

    expect(gridComputedStyle.display).toBe('grid')

    // Parse the grid-template-columns to count columns
    // At 768px (md breakpoint), should have 2 columns
    const columns = gridComputedStyle.gridTemplateColumns.split(' ').filter(
      (col) => col.trim() !== ''
    )
    expect(columns.length).toBe(2)
  })

  test('should have minimum 44px height buttons for touch', async ({ page }) => {
    // Test case 3: Buttons have minimum 44px height for touch
    // Focus on primary CTA buttons in the hero section that users will tap on tablet
    // These are styled with bg-blue-600 or bg-slate-700 and size "lg" (py-3)
    const ctaButtons = page.locator('#hero [class*="bg-blue-600"], #hero [class*="bg-slate-700"]')
    const buttonCount = await ctaButtons.count()

    // Ensure we have CTA buttons to test
    expect(buttonCount).toBeGreaterThan(0)

    // Check each primary CTA button has minimum 44px height (touch target requirement)
    let checkedButtons = 0
    for (let i = 0; i < buttonCount; i++) {
      const button = ctaButtons.nth(i)
      const isVisible = await button.isVisible()

      if (isVisible) {
        const boundingBox = await button.boundingBox()
        if (boundingBox && boundingBox.height > 0) {
          // Primary touch targets should be at least 44px
          expect(boundingBox.height).toBeGreaterThanOrEqual(44)
          checkedButtons++
        }
      }
    }

    // Ensure we actually checked some buttons
    expect(checkedButtons).toBeGreaterThan(0)
  })

  test('should have no horizontal scrolling at tablet size', async ({ page }) => {
    // Test case 4: No horizontal overflow at tablet size
    // Wait for page to fully load
    await page.waitForLoadState('networkidle')

    // Check for horizontal overflow
    const hasHorizontalScroll = await page.evaluate(() => {
      const docWidth = document.documentElement.scrollWidth
      const viewportWidth = window.innerWidth
      return docWidth > viewportWidth
    })

    expect(hasHorizontalScroll).toBe(false)

    // Also verify no element extends beyond viewport
    const overflowingElements = await page.evaluate(() => {
      const viewportWidth = window.innerWidth
      const elements = document.querySelectorAll('*')
      const overflowing: string[] = []

      elements.forEach((el) => {
        const rect = el.getBoundingClientRect()
        if (rect.right > viewportWidth + 1) {
          // +1 for rounding tolerance
          overflowing.push(
            `${el.tagName}.${el.className}: right=${rect.right}, viewport=${viewportWidth}`
          )
        }
      })

      return overflowing
    })

    expect(overflowingElements).toHaveLength(0)
  })

  test('should show header logo and title correctly', async ({ page }) => {
    // Additional test: Verify header displays correctly at tablet size
    const logo = page.locator('header img[alt*="MirDB"]')
    await expect(logo).toBeVisible()

    const title = page.locator('header span', { hasText: 'MirDB' })
    await expect(title).toBeVisible()
  })

  test('should display navigation at tablet size', async ({ page }) => {
    // At 768px (md breakpoint), navigation should be visible
    // The header nav uses "hidden md:flex" so it should be visible at 768px
    const nav = page.locator('nav[aria-label="Main navigation"]')
    await expect(nav).toBeVisible()
  })

  test('should display hero content properly at tablet size', async ({ page }) => {
    // Verify hero section adapts to tablet
    const heroHeadline = page.locator('#hero-headline')
    await expect(heroHeadline).toBeVisible()

    // Check that text is readable (not overflowing)
    const headlineBounding = await heroHeadline.boundingBox()
    expect(headlineBounding).not.toBeNull()

    if (headlineBounding) {
      // Headline should fit within viewport width
      // boundingBox has x, y, width, height - calculate right edge
      const rightEdge = headlineBounding.x + headlineBounding.width
      expect(rightEdge).toBeLessThanOrEqual(768)
    }
  })

  test('should maintain proper spacing at tablet viewport', async ({ page }) => {
    // Verify sections have proper padding/margin
    const sections = page.locator('section')
    const sectionCount = await sections.count()

    for (let i = 0; i < sectionCount; i++) {
      const section = sections.nth(i)
      const isVisible = await section.isVisible()

      if (isVisible) {
        const box = await section.boundingBox()
        if (box) {
          // boundingBox has x, y, width, height - calculate edges
          // Each section should start within the viewport
          expect(box.x).toBeGreaterThanOrEqual(0)
          // Sections should not extend past viewport width
          const rightEdge = box.x + box.width
          expect(rightEdge).toBeLessThanOrEqual(768 + 1) // +1 for rounding
        }
      }
    }
  })
})
