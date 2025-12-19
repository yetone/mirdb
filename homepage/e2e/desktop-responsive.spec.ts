import { test, expect } from '@playwright/test'

test.describe('Responsive Design - Desktop View', () => {
  test.beforeEach(async ({ page }) => {
    // Set desktop viewport size (1280px width as specified in scenario)
    await page.setViewportSize({ width: 1280, height: 800 })
    await page.goto('/')
  })

  test('TC1: page renders with full desktop layout at 1280px viewport width', async ({ page }) => {
    // Set explicit desktop viewport
    await page.setViewportSize({ width: 1280, height: 800 })

    // Wait for the page to load
    await expect(page.locator('.hero')).toBeVisible()

    // Check that page renders properly without horizontal overflow
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth
    })
    expect(hasHorizontalScroll).toBe(false)

    // Verify the main content sections are visible
    await expect(page.locator('h1')).toBeVisible()
    await expect(page.locator('.tagline')).toBeVisible()
    await expect(page.locator('.description')).toBeVisible()

    // Verify features section is visible
    await expect(page.locator('.features-section')).toBeVisible()

    // Verify the app has proper layout structure
    const appElement = page.locator('.app')
    await expect(appElement).toBeVisible()
  })

  test('TC2: full navigation links visible without hamburger menu on desktop', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 })

    // Check that sticky navigation is visible
    const stickyNav = page.locator('.sticky-nav, [data-testid="sticky-nav"]')
    await expect(stickyNav).toBeVisible()

    // Check for GitHub link in navigation (primary navigation element)
    const navGitHubLink = page.locator('.sticky-nav a[href*="github.com"], nav a[href*="github.com"]')
    await expect(navGitHubLink).toBeVisible()

    // Verify there is no hamburger menu visible on desktop
    const hamburgerButton = page.locator('[data-testid="mobile-menu-toggle"], .hamburger-menu, .mobile-nav-toggle, button[aria-label*="menu" i]')
    const hamburgerCount = await hamburgerButton.count()

    // On desktop, hamburger menu should either not exist or be hidden
    if (hamburgerCount > 0) {
      await expect(hamburgerButton.first()).not.toBeVisible()
    }

    // Verify navigation links are directly accessible (not collapsed)
    const githubLink = page.locator('.github-link')
    await expect(githubLink.first()).toBeVisible()
  })

  test('TC3: feature cards display in 3-4 column grid on desktop', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 })

    // Scroll to features section
    const featuresSection = page.locator('.features-section')
    await featuresSection.scrollIntoViewIfNeeded()
    await expect(featuresSection).toBeVisible()

    // Get the features grid
    const featuresGrid = page.locator('.features-grid')
    await expect(featuresGrid).toBeVisible()

    // Get all feature cards
    const featureCards = page.locator('.feature-card')
    const cardCount = await featureCards.count()
    expect(cardCount).toBeGreaterThanOrEqual(3)

    // Get bounding boxes of feature cards to verify multi-column layout
    const cardPositions: { x: number; y: number; width: number }[] = []

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i)
      await expect(card).toBeVisible()
      const boundingBox = await card.boundingBox()
      expect(boundingBox).not.toBeNull()
      if (boundingBox) {
        cardPositions.push({ x: boundingBox.x, y: boundingBox.y, width: boundingBox.width })
      }
    }

    // Verify cards are displayed in a multi-column layout
    // Cards should have different x positions if they're in multiple columns
    const uniqueXPositions = new Set(cardPositions.map(pos => Math.round(pos.x / 10) * 10))
    const uniqueYPositions = new Set(cardPositions.map(pos => Math.round(pos.y / 10) * 10))

    // On desktop at 1280px width, we expect at least 2-3 columns
    // Either multiple different x positions (columns) or all same row (single row with multiple columns)
    if (uniqueYPositions.size === 1) {
      // All cards on same row - good, means horizontal layout
      expect(uniqueXPositions.size).toBeGreaterThanOrEqual(2)
    } else {
      // Multiple rows - also fine, but should still have multiple columns
      expect(uniqueXPositions.size).toBeGreaterThanOrEqual(2)
    }

    // Verify grid CSS properties
    const gridDisplay = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return style.display
    })
    expect(gridDisplay).toBe('grid')
  })

  test('TC4: content has max-width constraint and is centered on wide screens', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 })

    // Check the container has max-width
    const container = page.locator('.container').first()
    await expect(container).toBeVisible()

    const containerStyle = await container.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        maxWidth: style.maxWidth,
        marginLeft: style.marginLeft,
        marginRight: style.marginRight
      }
    })

    // Verify max-width is set (should be 1200px based on CSS)
    expect(containerStyle.maxWidth).not.toBe('none')
    const maxWidthValue = parseInt(containerStyle.maxWidth, 10)
    expect(maxWidthValue).toBeGreaterThan(0)
    expect(maxWidthValue).toBeLessThanOrEqual(1200)

    // Verify centering (margin: 0 auto means marginLeft and marginRight should be 'auto' or computed equal values)
    const containerBox = await container.boundingBox()
    expect(containerBox).not.toBeNull()

    if (containerBox) {
      // Container should be centered in the viewport
      const viewportWidth = 1280
      const leftMargin = containerBox.x
      const rightMargin = viewportWidth - (containerBox.x + containerBox.width)

      // Left and right margins should be approximately equal (within 50px tolerance)
      expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(50)
    }

    // Test at wider viewport to ensure content doesn't stretch
    await page.setViewportSize({ width: 1920, height: 1080 })

    const wideContainerBox = await container.boundingBox()
    expect(wideContainerBox).not.toBeNull()

    if (wideContainerBox) {
      // Container width should still be constrained to max-width
      expect(wideContainerBox.width).toBeLessThanOrEqual(1200 + 40) // +40 for padding
    }
  })
})
