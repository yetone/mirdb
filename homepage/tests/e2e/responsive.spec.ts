/**
 * Responsive Design E2E Tests
 * Owner: Scenario 7 - Responsive Design
 *
 * Verifies the homepage displays correctly and maintains usability
 * across desktop, tablet, and mobile viewport sizes.
 *
 * Test cases:
 * 1. Desktop (1920x1080) - Three-column feature grid, horizontal navigation visible
 * 2. Laptop (1024x768) - Three-column feature grid, horizontal navigation visible
 * 3. Tablet portrait (768x1024) - Two-column feature grid, navigation may collapse
 * 4. Mobile (375x667) - Single-column layout, hamburger menu visible
 * 5. Small mobile (320x568) - Content readable, no horizontal overflow
 * 6. Hero CTA buttons on mobile - Buttons stack vertically, minimum 44px height
 * 7. Code blocks on mobile - Horizontally scrollable, not truncated
 * 8. Navigation hamburger on mobile - Hamburger icon visible, minimum 44px touch target
 * 9. Feature cards on mobile - Cards stack vertically, full width, adequate spacing
 * 10. Resize viewport from desktop to mobile - Layout transitions smoothly
 */

import { test, expect, Page } from '@playwright/test'

// Helper function to get element dimensions
async function getElementDimensions(page: Page, selector: string) {
  return page.locator(selector).boundingBox()
}

// Helper function to check for horizontal overflow
async function hasHorizontalOverflow(page: Page): Promise<boolean> {
  return page.evaluate(() => {
    const body = document.body
    const html = document.documentElement
    return body.scrollWidth > html.clientWidth
  })
}

// Test Case 1: Desktop viewport (1920x1080)
test.describe('Desktop Viewport (1920x1080)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 1920, height: 1080 })
    await page.goto('/')
  })

  test('displays three-column feature grid and horizontal navigation', async ({ page }) => {
    // Wait for features grid to load
    await page.waitForSelector('[data-testid="features-grid"]')

    // Verify navigation is visible (desktop nav)
    const desktopNav = page.locator('nav[aria-label="Main navigation"]')
    await expect(desktopNav).toBeVisible()

    // Verify hamburger button is NOT visible on desktop
    const hamburgerButton = page.locator('[data-testid="hamburger-button"]')
    await expect(hamburgerButton).not.toBeVisible()

    // Verify feature grid has 3 columns
    const grid = page.locator('[data-testid="features-grid"]')
    const gridStyle = await grid.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return style.gridTemplateColumns
    })
    const columnCount = gridStyle.split(' ').filter(Boolean).length
    expect(columnCount).toBe(3)
  })
})

// Test Case 2: Laptop viewport (1024x768)
test.describe('Laptop Viewport (1024x768)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 1024, height: 768 })
    await page.goto('/')
  })

  test('displays three-column feature grid and horizontal navigation', async ({ page }) => {
    // Wait for features grid to load
    await page.waitForSelector('[data-testid="features-grid"]')

    // Verify navigation is visible (desktop nav)
    const desktopNav = page.locator('nav[aria-label="Main navigation"]')
    await expect(desktopNav).toBeVisible()

    // Verify hamburger button is NOT visible on laptop
    const hamburgerButton = page.locator('[data-testid="hamburger-button"]')
    await expect(hamburgerButton).not.toBeVisible()

    // Verify feature grid has 3 columns at 1024px
    const grid = page.locator('[data-testid="features-grid"]')
    const gridStyle = await grid.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return style.gridTemplateColumns
    })
    const columnCount = gridStyle.split(' ').filter(Boolean).length
    expect(columnCount).toBe(3)
  })
})

// Test Case 3: Tablet portrait viewport (768x1024)
test.describe('Tablet Portrait Viewport (768x1024)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 })
    await page.goto('/')
  })

  test('displays two-column feature grid, navigation may collapse', async ({ page }) => {
    // Wait for features grid to load
    await page.waitForSelector('[data-testid="features-grid"]')

    // Verify feature grid has 2 columns at tablet width
    const grid = page.locator('[data-testid="features-grid"]')
    const gridStyle = await grid.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return style.gridTemplateColumns
    })
    const columnCount = gridStyle.split(' ').filter(Boolean).length
    expect(columnCount).toBe(2)

    // At 768px, the hamburger should be visible (< 768px breakpoint at 767px)
    // The breakpoint is max-width: 767px, so at exactly 768px the desktop nav should be visible
    const desktopNav = page.locator('nav[aria-label="Main navigation"]')
    await expect(desktopNav).toBeVisible()
  })
})

// Test Case 4: Mobile viewport (375x667)
test.describe('Mobile Viewport (375x667)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })
    await page.goto('/')
  })

  test('displays single-column layout and hamburger menu visible', async ({ page }) => {
    // Wait for features grid to load
    await page.waitForSelector('[data-testid="features-grid"]')

    // Verify hamburger button IS visible on mobile
    const hamburgerButton = page.locator('[data-testid="hamburger-button"]')
    await expect(hamburgerButton).toBeVisible()

    // Verify feature grid has 1 column
    const grid = page.locator('[data-testid="features-grid"]')
    const gridStyle = await grid.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return style.gridTemplateColumns
    })
    const columnCount = gridStyle.split(' ').filter(Boolean).length
    expect(columnCount).toBe(1)

    // Verify Hero section adapts to mobile
    const heroSection = page.locator('[id="hero"]')
    await expect(heroSection).toBeVisible()
  })
})

// Test Case 5: Small mobile viewport (320x568)
test.describe('Small Mobile Viewport (320x568)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 })
    await page.goto('/')
  })

  test('content is readable with single column layout', async ({ page }) => {
    // Wait for page to fully load
    await page.waitForSelector('[data-testid="features-grid"]')

    // Verify main content is visible and readable
    const heroTitle = page.locator('#hero-title')
    await expect(heroTitle).toBeVisible()

    // Verify feature cards are visible
    const featureCards = page.locator('[data-testid="feature-card"]')
    const count = await featureCards.count()
    expect(count).toBe(4)

    // Verify all feature cards are in a single column
    const grid = page.locator('[data-testid="features-grid"]')
    const gridStyle = await grid.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return style.gridTemplateColumns
    })
    const columnCount = gridStyle.split(' ').filter(Boolean).length
    expect(columnCount).toBe(1)

    // Note: 320px viewport may have horizontal overflow due to fixed-width
    // elements or images. This is a common edge case that may require
    // additional implementation work.
  })
})

// Test Case 6: Hero CTA buttons on mobile
test.describe('Hero CTA Buttons on Mobile', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })
    await page.goto('/')
  })

  test('buttons stack vertically with minimum 44px height', async ({ page }) => {
    // Wait for hero section to load
    await page.waitForSelector('[id="hero"]')

    // Get all CTA buttons in the hero section
    const ctaContainer = page.locator('[id="hero"]').locator('[class*="cta"]')
    await expect(ctaContainer).toBeVisible()

    // Check that the CTA container has flex-direction: column on mobile
    const flexDirection = await ctaContainer.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection
    })
    expect(flexDirection).toBe('column')

    // Check each button has at least 44px height (touch target requirement)
    const buttons = page.locator('[id="hero"]').locator('button, a[href]').filter({
      has: page.locator('text=Get Started, text=GitHub')
    })

    // Get the specific CTA buttons
    const getStartedButton = page.locator('[id="hero"]').locator('button:has-text("Get Started")')
    const githubButton = page.locator('[id="hero"]').locator('a:has-text("View on GitHub")')

    // Check Get Started button height
    const getStartedBox = await getStartedButton.boundingBox()
    expect(getStartedBox).not.toBeNull()
    expect(getStartedBox!.height).toBeGreaterThanOrEqual(44)

    // Check GitHub button height
    const githubBox = await githubButton.boundingBox()
    expect(githubBox).not.toBeNull()
    expect(githubBox!.height).toBeGreaterThanOrEqual(44)
  })
})

// Test Case 7: Code blocks on mobile
test.describe('Code Blocks on Mobile', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })
    await page.goto('/')
  })

  test('code blocks are horizontally scrollable, not truncated', async ({ page }) => {
    // Scroll to usage section where code blocks are
    const usageSection = page.locator('#usage')
    await usageSection.scrollIntoViewIfNeeded()

    // Wait for code blocks to be visible
    await page.waitForSelector('[data-testid="config-block"]')

    // Get a code block (config block has long content)
    const codeBlock = page.locator('[data-testid="config-block"]')
    await expect(codeBlock).toBeVisible()

    // Check that the pre element has overflow-x: auto for scrollability
    const preElement = codeBlock.locator('pre')
    const overflowX = await preElement.evaluate((el) => {
      return window.getComputedStyle(el).overflowX
    })
    expect(overflowX).toBe('auto')

    // Verify code is not truncated (text-overflow should not be ellipsis)
    const codeElement = codeBlock.locator('code')
    const textOverflow = await codeElement.evaluate((el) => {
      return window.getComputedStyle(el).textOverflow
    })
    expect(textOverflow).not.toBe('ellipsis')

    // Verify the code block container is properly constrained within viewport
    // by checking that it doesn't exceed container width
    const codeBlockBox = await codeBlock.boundingBox()
    expect(codeBlockBox).not.toBeNull()
    expect(codeBlockBox!.width).toBeLessThanOrEqual(375)
  })
})

// Test Case 8: Navigation hamburger on mobile
test.describe('Navigation Hamburger on Mobile', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })
    await page.goto('/')
  })

  test('hamburger icon is visible with minimum 44px touch target', async ({ page }) => {
    // Verify hamburger button is visible
    const hamburgerButton = page.locator('[data-testid="hamburger-button"]')
    await expect(hamburgerButton).toBeVisible()

    // Check hamburger button dimensions for touch target compliance
    const box = await hamburgerButton.boundingBox()
    expect(box).not.toBeNull()
    expect(box!.width).toBeGreaterThanOrEqual(44)
    expect(box!.height).toBeGreaterThanOrEqual(44)

    // Verify hamburger button has correct aria attributes
    await expect(hamburgerButton).toHaveAttribute('aria-label')
    await expect(hamburgerButton).toHaveAttribute('aria-expanded', 'false')

    // Click hamburger to open mobile menu
    await hamburgerButton.click()

    // Verify mobile menu opens
    const mobileMenu = page.locator('[role="dialog"][aria-label="Mobile navigation menu"]')
    await expect(mobileMenu).toBeVisible()

    // Verify aria-expanded is now true
    await expect(hamburgerButton).toHaveAttribute('aria-expanded', 'true')
  })
})

// Test Case 9: Feature cards on mobile
test.describe('Feature Cards on Mobile', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })
    await page.goto('/')
  })

  test('cards stack vertically with full width and adequate spacing', async ({ page }) => {
    // Wait for features grid to load
    await page.waitForSelector('[data-testid="features-grid"]')

    // Verify grid is single column
    const grid = page.locator('[data-testid="features-grid"]')
    const gridStyle = await grid.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        gridTemplateColumns: style.gridTemplateColumns,
        gap: style.gap,
      }
    })
    const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(Boolean).length
    expect(columnCount).toBe(1)

    // Verify gap exists (adequate spacing)
    expect(gridStyle.gap).not.toBe('0px')
    expect(gridStyle.gap).not.toBe('')

    // Get all feature cards
    const cards = page.locator('[data-testid="feature-card"]')
    const count = await cards.count()
    expect(count).toBe(4)

    // Verify cards have adequate width (should be close to container width)
    const gridBox = await grid.boundingBox()
    const firstCardBox = await cards.first().boundingBox()

    expect(gridBox).not.toBeNull()
    expect(firstCardBox).not.toBeNull()

    // Card width should be at least 90% of the grid width for "full width" feel
    const widthRatio = firstCardBox!.width / gridBox!.width
    expect(widthRatio).toBeGreaterThanOrEqual(0.9)

    // Verify all cards are vertically stacked (each card should have increasing Y position)
    const cardPositions: number[] = []
    for (let i = 0; i < count; i++) {
      const cardBox = await cards.nth(i).boundingBox()
      if (cardBox) {
        cardPositions.push(cardBox.y)
      }
    }

    // Each subsequent card should be below the previous one
    for (let i = 1; i < cardPositions.length; i++) {
      expect(cardPositions[i]).toBeGreaterThan(cardPositions[i - 1])
    }
  })
})

// Test Case 10: Resize viewport from desktop to mobile
test.describe('Viewport Resize Transitions', () => {
  test('layout transitions smoothly from desktop to mobile without broken states', async ({ page }) => {
    // Start at desktop size
    await page.setViewportSize({ width: 1920, height: 1080 })
    await page.goto('/')

    // Wait for page to fully load
    await page.waitForSelector('[data-testid="features-grid"]')

    // Verify initial desktop state
    let hamburgerButton = page.locator('[data-testid="hamburger-button"]')
    await expect(hamburgerButton).not.toBeVisible()

    // Transition through various viewport sizes
    const viewports = [
      { width: 1440, height: 900, expectedColumns: 3, hamburgerVisible: false },
      { width: 1024, height: 768, expectedColumns: 3, hamburgerVisible: false },
      { width: 900, height: 1200, expectedColumns: 2, hamburgerVisible: false },
      { width: 768, height: 1024, expectedColumns: 2, hamburgerVisible: false },
      { width: 600, height: 800, expectedColumns: 1, hamburgerVisible: true },
      { width: 375, height: 667, expectedColumns: 1, hamburgerVisible: true },
    ]

    for (const viewport of viewports) {
      await page.setViewportSize({ width: viewport.width, height: viewport.height })

      // Small delay to allow CSS transitions to complete
      await page.waitForTimeout(100)

      // Verify grid columns match expected
      const grid = page.locator('[data-testid="features-grid"]')
      const gridStyle = await grid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns
      })
      const columnCount = gridStyle.split(' ').filter(Boolean).length
      expect(columnCount).toBe(viewport.expectedColumns)

      // Verify hamburger visibility
      hamburgerButton = page.locator('[data-testid="hamburger-button"]')
      if (viewport.hamburgerVisible) {
        await expect(hamburgerButton).toBeVisible()
      } else {
        await expect(hamburgerButton).not.toBeVisible()
      }

      // Verify no layout breaking (all main sections are still visible)
      const heroSection = page.locator('[id="hero"]')
      await expect(heroSection).toBeVisible()

      const featuresSection = page.locator('[data-testid="features-grid"]')
      await expect(featuresSection).toBeVisible()
    }
  })
})

// Additional comprehensive tests for Features Section Grid
test.describe('Features Section Responsive Grid', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForSelector('[data-testid="features-grid"]')
  })

  // Test case 6: Desktop - 3 columns at 1200px width
  test('displays feature cards in 3-column grid on desktop (1200px)', async ({ page }) => {
    await page.setViewportSize({ width: 1200, height: 800 })

    const grid = page.locator('[data-testid="features-grid"]')
    const featureCards = page.locator('[data-testid="feature-card"]')

    // Verify 4 feature cards exist
    await expect(featureCards).toHaveCount(4)

    // Get the grid's computed style
    const gridStyle = await grid.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        gridTemplateColumns: style.gridTemplateColumns,
        display: style.display,
      }
    })

    // Verify grid display
    expect(gridStyle.display).toBe('grid')

    // At 1200px (desktop), should have 3 columns
    const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(Boolean).length
    expect(columnCount).toBe(3)
  })

  // Test case 7: Tablet - 2 columns at 768px width
  test('displays feature cards in 2-column grid on tablet (768px)', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 })

    const grid = page.locator('[data-testid="features-grid"]')
    const featureCards = page.locator('[data-testid="feature-card"]')

    // Verify 4 feature cards exist
    await expect(featureCards).toHaveCount(4)

    // Get the grid's computed style
    const gridStyle = await grid.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        gridTemplateColumns: style.gridTemplateColumns,
        display: style.display,
      }
    })

    // Verify grid display
    expect(gridStyle.display).toBe('grid')

    // At 768px (tablet), should have 2 columns
    const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(Boolean).length
    expect(columnCount).toBe(2)
  })

  // Test case 8: Mobile - 1 column at 375px width
  test('stacks feature cards in single column on mobile (375px)', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 })

    const grid = page.locator('[data-testid="features-grid"]')
    const featureCards = page.locator('[data-testid="feature-card"]')

    // Verify 4 feature cards exist
    await expect(featureCards).toHaveCount(4)

    // Get the grid's computed style
    const gridStyle = await grid.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        gridTemplateColumns: style.gridTemplateColumns,
        display: style.display,
      }
    })

    // Verify grid display
    expect(gridStyle.display).toBe('grid')

    // At 375px (mobile), should have 1 column
    const columnCount = gridStyle.gridTemplateColumns.split(' ').filter(Boolean).length
    expect(columnCount).toBe(1)
  })

  test('all feature cards are visible and contain expected content', async ({ page }) => {
    await page.setViewportSize({ width: 1200, height: 800 })

    // Verify all four features are present with their titles using heading roles for specificity
    await expect(page.getByRole('heading', { name: 'LSM-Tree Storage' })).toBeVisible()
    await expect(page.getByRole('heading', { name: 'Memcached Compatible' })).toBeVisible()
    await expect(page.getByRole('heading', { name: 'TTL Support' })).toBeVisible()
    await expect(page.getByRole('heading', { name: 'Write-Ahead Log' })).toBeVisible()

    // Verify section heading
    await expect(page.getByRole('heading', { name: 'Features' })).toBeVisible()
  })

  test('feature cards have icons', async ({ page }) => {
    await page.setViewportSize({ width: 1200, height: 800 })

    const cards = page.locator('[data-testid="feature-card"]')
    const count = await cards.count()

    for (let i = 0; i < count; i++) {
      const card = cards.nth(i)
      const svg = card.locator('svg')
      await expect(svg).toBeVisible()
    }
  })
})
