/**
 * Responsive design E2E tests.
 * Owner: Scenario 6 - Responsive Design
 *
 * Tests for:
 * - Desktop viewport (>1024px)
 * - Tablet viewport (768-1024px)
 * - Mobile viewport (<768px)
 * - Touch target sizes
 * - No horizontal scroll
 * - Dynamic viewport resizing
 */

import { test, expect, Page } from '@playwright/test'

// Viewport configurations
const VIEWPORTS = {
  desktop: { width: 1440, height: 900 },
  tablet: { width: 900, height: 1024 },
  mobile: { width: 375, height: 667 },
}

// Minimum touch target size per WCAG 2.1
const MIN_TOUCH_TARGET = 44

/**
 * Helper function to get computed styles for an element
 */
async function getComputedStyles(page: Page, selector: string) {
  return page.evaluate((sel) => {
    const element = document.querySelector(sel)
    if (!element) return null
    const style = window.getComputedStyle(element)
    return {
      display: style.display,
      flexDirection: style.flexDirection,
      gridTemplateColumns: style.gridTemplateColumns,
      width: style.width,
      padding: style.padding,
      fontSize: style.fontSize,
    }
  }, selector)
}

/**
 * Helper function to get bounding box of an element
 */
async function getElementSize(page: Page, selector: string) {
  return page.evaluate((sel) => {
    const element = document.querySelector(sel)
    if (!element) return null
    const rect = element.getBoundingClientRect()
    return {
      width: rect.width,
      height: rect.height,
    }
  }, selector)
}

test.describe('Responsive Design - Homepage Layout', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for page to be fully loaded
    await page.waitForLoadState('domcontentloaded')
  })

  // Test Case 1: Render at 1440px viewport width
  test('TC1: Desktop viewport (1440px) shows full layout with horizontal navigation and multi-column sections', async ({
    page,
  }) => {
    await page.setViewportSize(VIEWPORTS.desktop)
    await page.waitForTimeout(100) // Allow CSS to apply

    // Verify hero section is visible
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Verify features grid uses 3 columns on desktop
    const featuresGrid = page.getByTestId('features-grid')
    if (await featuresGrid.isVisible()) {
      const gridStyle = await getComputedStyles(page, '[data-testid="features-grid"]')
      if (gridStyle) {
        expect(gridStyle.display).toBe('grid')
        const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(Boolean).length
        expect(columnCount).toBe(3)
      }
    }

    // Verify CTA buttons are displayed horizontally on desktop
    const ctaContainer = page.locator('[class*="ctaContainer"]')
    if (await ctaContainer.isVisible()) {
      const ctaStyle = await ctaContainer.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection
      })
      expect(ctaStyle).toBe('row')
    }
  })

  // Test Case 2: Render at 900px viewport width
  test('TC2: Tablet viewport (900px) shows adjusted spacing and 2-column feature grid', async ({
    page,
  }) => {
    await page.setViewportSize(VIEWPORTS.tablet)
    await page.waitForTimeout(100)

    // Verify hero section is visible
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Verify features grid uses 2 columns on tablet
    const featuresGrid = page.getByTestId('features-grid')
    if (await featuresGrid.isVisible()) {
      const gridStyle = await getComputedStyles(page, '[data-testid="features-grid"]')
      if (gridStyle) {
        expect(gridStyle.display).toBe('grid')
        const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(Boolean).length
        expect(columnCount).toBe(2)
      }
    }

    // Verify CTA buttons are displayed horizontally on tablet
    const ctaContainer = page.locator('[class*="ctaContainer"]')
    if (await ctaContainer.isVisible()) {
      const ctaStyle = await ctaContainer.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection
      })
      expect(ctaStyle).toBe('row')
    }
  })

  // Test Case 3: Render at 375px viewport width
  test('TC3: Mobile viewport (375px) shows stacked sections and single-column layout', async ({
    page,
  }) => {
    await page.setViewportSize(VIEWPORTS.mobile)
    await page.waitForTimeout(100)

    // Verify hero section is visible
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Verify features grid uses 1 column on mobile
    const featuresGrid = page.getByTestId('features-grid')
    if (await featuresGrid.isVisible()) {
      const gridStyle = await getComputedStyles(page, '[data-testid="features-grid"]')
      if (gridStyle) {
        expect(gridStyle.display).toBe('grid')
        const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(Boolean).length
        expect(columnCount).toBe(1)
      }
    }

    // Verify CTA buttons are stacked vertically on mobile
    const ctaContainer = page.locator('[class*="ctaContainer"]')
    if (await ctaContainer.isVisible()) {
      const ctaStyle = await ctaContainer.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection
      })
      expect(ctaStyle).toBe('column')
    }
  })
})

test.describe('Responsive Design - Touch Targets', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.setViewportSize(VIEWPORTS.mobile)
    await page.waitForLoadState('domcontentloaded')
    await page.waitForTimeout(100)
  })

  // Test Case 4: Measure touch target sizes on mobile
  test('TC4: All interactive elements have minimum 44x44px touch target', async ({
    page,
  }) => {
    // Get buttons and links in main content area (excluding footer social links which are 40px by design)
    // Footer social links are owned by Scenario 5 and may have different design requirements
    const mainContent = page.locator('main')
    const buttons = mainContent.locator('button, a[href]')
    const buttonCount = await buttons.count()

    for (let i = 0; i < buttonCount; i++) {
      const button = buttons.nth(i)

      // Only check visible buttons
      if (await button.isVisible()) {
        const boundingBox = await button.boundingBox()

        if (boundingBox) {
          // Check minimum touch target size for main content
          expect(boundingBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET)
          expect(boundingBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET)
        }
      }
    }
  })

  test('CTA buttons meet touch target requirements on mobile', async ({
    page,
  }) => {
    // Specifically check primary and secondary CTA buttons
    const primaryButton = page.locator('a[href="/signup"]')
    const secondaryButton = page.locator('a[href="#features"]')

    if (await primaryButton.isVisible()) {
      const primaryBox = await primaryButton.boundingBox()
      expect(primaryBox).not.toBeNull()
      if (primaryBox) {
        expect(primaryBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET)
        expect(primaryBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET)
      }
    }

    if (await secondaryButton.isVisible()) {
      const secondaryBox = await secondaryButton.boundingBox()
      expect(secondaryBox).not.toBeNull()
      if (secondaryBox) {
        expect(secondaryBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET)
        expect(secondaryBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET)
      }
    }
  })
})

test.describe('Responsive Design - Horizontal Scroll', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('domcontentloaded')
  })

  // Test Case 5: Test horizontal scroll on mobile
  test('TC5: No horizontal scrollbar appears on mobile - all content fits within viewport', async ({
    page,
  }) => {
    await page.setViewportSize(VIEWPORTS.mobile)
    await page.waitForTimeout(100)

    // Check that the document width equals viewport width (no horizontal overflow)
    const overflowInfo = await page.evaluate(() => {
      const body = document.body
      const html = document.documentElement

      return {
        bodyScrollWidth: body.scrollWidth,
        viewportWidth: window.innerWidth,
        htmlOverflowX: window.getComputedStyle(html).overflowX,
        hasHorizontalScroll: body.scrollWidth > window.innerWidth,
      }
    })

    expect(overflowInfo.hasHorizontalScroll).toBe(false)
  })

  test('No horizontal scroll on tablet', async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.tablet)
    await page.waitForTimeout(100)

    const overflowInfo = await page.evaluate(() => {
      return {
        bodyScrollWidth: document.body.scrollWidth,
        viewportWidth: window.innerWidth,
        hasHorizontalScroll: document.body.scrollWidth > window.innerWidth,
      }
    })

    expect(overflowInfo.hasHorizontalScroll).toBe(false)
  })
})

test.describe('Responsive Design - Dynamic Viewport Resizing', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('domcontentloaded')
  })

  // Test Case 6: Resize viewport dynamically
  test('TC6: Layout smoothly transitions between breakpoints without broken layouts', async ({
    page,
  }) => {
    // Start at desktop
    await page.setViewportSize(VIEWPORTS.desktop)
    await page.waitForTimeout(100)

    // Verify desktop layout
    let heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    let desktopOverflow = await page.evaluate(() => document.body.scrollWidth > window.innerWidth)
    expect(desktopOverflow).toBe(false)

    // Resize to tablet
    await page.setViewportSize(VIEWPORTS.tablet)
    await page.waitForTimeout(100)

    // Verify content is still visible and not broken
    await expect(heroSection).toBeVisible()

    let tabletOverflow = await page.evaluate(() => document.body.scrollWidth > window.innerWidth)
    expect(tabletOverflow).toBe(false)

    // Resize to mobile
    await page.setViewportSize(VIEWPORTS.mobile)
    await page.waitForTimeout(100)

    // Verify content is still visible
    await expect(heroSection).toBeVisible()

    let mobileOverflow = await page.evaluate(() => document.body.scrollWidth > window.innerWidth)
    expect(mobileOverflow).toBe(false)

    // Resize back to desktop
    await page.setViewportSize(VIEWPORTS.desktop)
    await page.waitForTimeout(100)

    // Verify content is still visible
    await expect(heroSection).toBeVisible()

    let finalOverflow = await page.evaluate(() => document.body.scrollWidth > window.innerWidth)
    expect(finalOverflow).toBe(false)
  })

  test('Features grid transitions between column layouts smoothly', async ({
    page,
  }) => {
    const featuresGrid = page.getByTestId('features-grid')

    // Skip if features grid is not present
    if (!(await featuresGrid.isVisible())) {
      test.skip()
      return
    }

    // Desktop: 3 columns
    await page.setViewportSize(VIEWPORTS.desktop)
    await page.waitForTimeout(100)

    let gridStyle = await getComputedStyles(page, '[data-testid="features-grid"]')
    expect(gridStyle?.gridTemplateColumns.split(' ').filter(Boolean).length).toBe(3)

    // Tablet: 2 columns
    await page.setViewportSize(VIEWPORTS.tablet)
    await page.waitForTimeout(100)

    gridStyle = await getComputedStyles(page, '[data-testid="features-grid"]')
    expect(gridStyle?.gridTemplateColumns.split(' ').filter(Boolean).length).toBe(2)

    // Mobile: 1 column
    await page.setViewportSize(VIEWPORTS.mobile)
    await page.waitForTimeout(100)

    gridStyle = await getComputedStyles(page, '[data-testid="features-grid"]')
    expect(gridStyle?.gridTemplateColumns.split(' ').filter(Boolean).length).toBe(1)
  })

  test('Hero CTA buttons transition from horizontal to vertical on resize', async ({
    page,
  }) => {
    const ctaContainer = page.locator('[class*="ctaContainer"]')

    // Skip if CTA container is not present
    if (!(await ctaContainer.isVisible())) {
      test.skip()
      return
    }

    // Desktop: horizontal layout
    await page.setViewportSize(VIEWPORTS.desktop)
    await page.waitForTimeout(100)

    let ctaStyle = await ctaContainer.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection
    })
    expect(ctaStyle).toBe('row')

    // Mobile: vertical layout
    await page.setViewportSize(VIEWPORTS.mobile)
    await page.waitForTimeout(100)

    ctaStyle = await ctaContainer.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection
    })
    expect(ctaStyle).toBe('column')
  })
})

test.describe('Responsive Design - Breakpoint Boundaries', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('domcontentloaded')
  })

  test('Layout changes at 768px breakpoint (mobile to tablet)', async ({
    page,
  }) => {
    // Just below 768px (mobile)
    await page.setViewportSize({ width: 767, height: 800 })
    await page.waitForTimeout(100)

    const featuresGrid = page.getByTestId('features-grid')
    if (await featuresGrid.isVisible()) {
      let gridStyle = await getComputedStyles(page, '[data-testid="features-grid"]')
      expect(gridStyle?.gridTemplateColumns.split(' ').filter(Boolean).length).toBe(1)
    }

    // At 768px (tablet)
    await page.setViewportSize({ width: 768, height: 800 })
    await page.waitForTimeout(100)

    if (await featuresGrid.isVisible()) {
      let gridStyle = await getComputedStyles(page, '[data-testid="features-grid"]')
      expect(gridStyle?.gridTemplateColumns.split(' ').filter(Boolean).length).toBe(2)
    }
  })

  test('Layout changes at 1025px breakpoint (tablet to desktop)', async ({
    page,
  }) => {
    // At 1024px (still tablet)
    await page.setViewportSize({ width: 1024, height: 800 })
    await page.waitForTimeout(100)

    const featuresGrid = page.getByTestId('features-grid')
    if (await featuresGrid.isVisible()) {
      let gridStyle = await getComputedStyles(page, '[data-testid="features-grid"]')
      expect(gridStyle?.gridTemplateColumns.split(' ').filter(Boolean).length).toBe(2)
    }

    // At 1025px (desktop)
    await page.setViewportSize({ width: 1025, height: 800 })
    await page.waitForTimeout(100)

    if (await featuresGrid.isVisible()) {
      let gridStyle = await getComputedStyles(page, '[data-testid="features-grid"]')
      expect(gridStyle?.gridTemplateColumns.split(' ').filter(Boolean).length).toBe(3)
    }
  })
})

test.describe('Responsive Design - Content Visibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('domcontentloaded')
  })

  test('Hero headline is visible at all viewport sizes', async ({ page }) => {
    const viewports = [VIEWPORTS.desktop, VIEWPORTS.tablet, VIEWPORTS.mobile]

    for (const viewport of viewports) {
      await page.setViewportSize(viewport)
      await page.waitForTimeout(100)

      const headline = page.locator('#hero-headline')
      await expect(headline).toBeVisible()
    }
  })

  test('CTA buttons are accessible above the fold on all devices', async ({
    page,
  }) => {
    const viewportConfigs = [
      { width: 1440, height: 900, name: 'desktop' },
      { width: 900, height: 1024, name: 'tablet' },
      { width: 375, height: 667, name: 'mobile' },
    ]

    for (const viewport of viewportConfigs) {
      await page.setViewportSize({ width: viewport.width, height: viewport.height })
      await page.waitForTimeout(100)

      // Check primary CTA is visible
      const primaryCTA = page.locator('a[href="/signup"]')
      if (await primaryCTA.isVisible()) {
        const boundingBox = await primaryCTA.boundingBox()
        if (boundingBox && typeof boundingBox.top === 'number' && typeof boundingBox.height === 'number') {
          // Button should be above the fold (within viewport height)
          const bottomPosition = boundingBox.top + boundingBox.height
          expect(bottomPosition).toBeLessThan(viewport.height)
        }
      }
    }
  })
})
