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
 * Desktop Viewport Tests (Scenario 9)
 */
test.describe('Desktop viewport', () => {
  test.beforeEach(async ({ page }) => {
    // Set desktop viewport (1920x1080)
    await page.setViewportSize({ width: 1920, height: 1080 })
    await page.goto('/')
  })

  // Test Case 1: Render page at 1920px width
  test('page renders with desktop layout and features in grid', async ({ page }) => {
    // Verify the page has loaded properly
    await expect(page.locator('header')).toBeVisible()
    await expect(page.locator('#hero')).toBeVisible()
    await expect(page.locator('#features')).toBeVisible()

    // Verify features section is visible with grid layout
    const featuresGrid = page.locator('[data-testid="features-grid"]')
    await expect(featuresGrid).toBeVisible()

    // Verify grid display is active
    const display = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display
    })
    expect(display).toBe('grid')

    // Verify desktop layout - content should be centered with proper max-width
    const heroSection = page.locator('#hero')
    const heroBox = await heroSection.boundingBox()
    expect(heroBox).toBeTruthy()
    // Hero should span reasonable width on desktop
    expect(heroBox!.width).toBeGreaterThanOrEqual(1000)
  })

  // Test Case 2: Check feature grid columns
  test('features display in 3-column grid on desktop', async ({ page }) => {
    const featuresGrid = page.locator('[data-testid="features-grid"]')
    await expect(featuresGrid).toBeVisible()

    // Get the computed grid-template-columns
    const gridColumns = await featuresGrid.evaluate((el) => {
      const computed = window.getComputedStyle(el)
      return computed.gridTemplateColumns
    })

    // Grid should have 3 columns - gridTemplateColumns will show actual pixel widths
    // e.g., "300px 300px 300px" for 3 columns
    const columnCount = gridColumns.split(' ').filter((col) => col.trim()).length
    expect(columnCount).toBe(3)

    // Verify feature cards exist and are arranged in columns
    const featureCards = page.locator('[data-testid="features-grid"] > article')
    const cardCount = await featureCards.count()
    expect(cardCount).toBeGreaterThanOrEqual(3)

    // Verify first row has 3 items side by side (similar y coordinates)
    if (cardCount >= 3) {
      const boxes = await Promise.all([
        featureCards.nth(0).boundingBox(),
        featureCards.nth(1).boundingBox(),
        featureCards.nth(2).boundingBox(),
      ])

      // All three cards should have similar top positions (same row)
      expect(boxes[0]).toBeTruthy()
      expect(boxes[1]).toBeTruthy()
      expect(boxes[2]).toBeTruthy()

      const tolerance = 10 // pixels
      expect(Math.abs(boxes[0]!.y - boxes[1]!.y)).toBeLessThan(tolerance)
      expect(Math.abs(boxes[1]!.y - boxes[2]!.y)).toBeLessThan(tolerance)

      // Cards should be in different horizontal positions (different columns)
      expect(boxes[1]!.x).toBeGreaterThan(boxes[0]!.x)
      expect(boxes[2]!.x).toBeGreaterThan(boxes[1]!.x)
    }
  })

  // Test Case 3: Check text line length
  test('content max-width prevents excessive line length', async ({ page }) => {
    // Check hero content max-width
    const heroContent = page.locator('#hero .max-w-4xl')
    await expect(heroContent).toBeVisible()

    const heroContentBox = await heroContent.boundingBox()
    expect(heroContentBox).toBeTruthy()
    // max-w-4xl is 896px, so content width should be around or less than that
    expect(heroContentBox!.width).toBeLessThanOrEqual(920) // Allow some margin

    // Check features section max-width
    const featuresContainer = page.locator('#features .max-w-7xl')
    await expect(featuresContainer).toBeVisible()

    const featuresBox = await featuresContainer.boundingBox()
    expect(featuresBox).toBeTruthy()
    // max-w-7xl is 1280px
    expect(featuresBox!.width).toBeLessThanOrEqual(1300) // Allow some margin

    // Check that text paragraphs have reasonable line length
    const paragraph = page.locator('#hero p').first()
    if (await paragraph.isVisible()) {
      const paragraphBox = await paragraph.boundingBox()
      expect(paragraphBox).toBeTruthy()
      // Lines should not exceed ~800px for comfortable reading
      expect(paragraphBox!.width).toBeLessThanOrEqual(850)
    }

    // Check getting started section max-width
    const gettingStartedContainer = page.locator('#getting-started .max-w-4xl')
    if ((await gettingStartedContainer.count()) > 0) {
      const gsBox = await gettingStartedContainer.boundingBox()
      if (gsBox) {
        expect(gsBox.width).toBeLessThanOrEqual(920) // max-w-4xl is 896px
      }
    }
  })

  // Test Case 4: Verify no horizontal scrolling
  test('no horizontal overflow at desktop size', async ({ page }) => {
    // Check that page doesn't have horizontal overflow
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth
    })
    expect(hasHorizontalScroll).toBe(false)

    // Verify body doesn't have horizontal scrollbar
    const bodyOverflow = await page.evaluate(() => {
      const body = document.body
      return body.scrollWidth > body.clientWidth
    })
    expect(bodyOverflow).toBe(false)

    // Verify no element overflows the viewport
    const overflowingElements = await page.evaluate(() => {
      const viewportWidth = window.innerWidth
      const elements = document.querySelectorAll('*')
      const overflowing: string[] = []

      elements.forEach((el) => {
        const rect = el.getBoundingClientRect()
        if (rect.right > viewportWidth + 1) {
          // +1 for rounding tolerance
          overflowing.push(el.tagName + (el.className ? '.' + el.className.split(' ')[0] : ''))
        }
      })

      return overflowing
    })

    expect(overflowingElements).toHaveLength(0)
  })

  // Additional desktop layout verification tests
  test('header spans full width on desktop', async ({ page }) => {
    const header = page.locator('header')
    await expect(header).toBeVisible()

    const headerBox = await header.boundingBox()
    expect(headerBox).toBeTruthy()
    // Header should span at least 90% of viewport width
    expect(headerBox!.width).toBeGreaterThanOrEqual(1728) // 90% of 1920
  })

  test('sections have proper padding on desktop', async ({ page }) => {
    // Verify features section has desktop padding
    const featuresSection = page.locator('#features')
    await expect(featuresSection).toBeVisible()

    const paddingLeft = await featuresSection.evaluate((el) => {
      return window.getComputedStyle(el).paddingLeft
    })
    const paddingRight = await featuresSection.evaluate((el) => {
      return window.getComputedStyle(el).paddingRight
    })

    // On desktop (lg), padding should be lg:px-8 = 32px
    const leftPx = parseFloat(paddingLeft)
    const rightPx = parseFloat(paddingRight)
    expect(leftPx).toBeGreaterThanOrEqual(32)
    expect(rightPx).toBeGreaterThanOrEqual(32)
  })

  test('CTA buttons display side by side on desktop', async ({ page }) => {
    // In hero section, buttons should be in a row on desktop
    const buttonContainer = page.locator('#hero .flex.flex-col.sm\\:flex-row')
    await expect(buttonContainer).toBeVisible()

    const flexDirection = await buttonContainer.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection
    })
    expect(flexDirection).toBe('row')

    // Verify buttons are positioned horizontally
    const buttons = buttonContainer.locator('a')
    const count = await buttons.count()

    if (count >= 2) {
      const box1 = await buttons.nth(0).boundingBox()
      const box2 = await buttons.nth(1).boundingBox()

      expect(box1).toBeTruthy()
      expect(box2).toBeTruthy()

      // Buttons should have similar y positions (same row)
      expect(Math.abs(box1!.y - box2!.y)).toBeLessThan(10)
      // Second button should be to the right of first
      expect(box2!.x).toBeGreaterThan(box1!.x)
    }
  })
})

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
