/**
 * E2E tests for Features section.
 * Owner: Scenario 3 - Features Section Implementation
 *
 * Test cases:
 * - Hover over feature card - Card displays hover effect
 * - Test grid at tablet viewport (768px) - Grid reflows to 2 columns
 * - Test grid at mobile viewport (375px) - Features stack vertically
 */

import { test, expect } from '@playwright/test'

test.describe('Features Section E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  // Basic rendering tests
  test('Features section renders correctly', async ({ page }) => {
    const featuresSection = page.getByTestId('features-section')
    await expect(featuresSection).toBeVisible()

    const heading = page.getByTestId('features-heading')
    await expect(heading).toBeVisible()
    await expect(heading).toHaveText(/Powerful Features/)

    const description = page.getByTestId('features-description')
    await expect(description).toBeVisible()
  })

  test('Features section contains 3-5 feature cards', async ({ page }) => {
    const featureCards = page.getByTestId('feature-card')
    const count = await featureCards.count()

    expect(count).toBeGreaterThanOrEqual(3)
    expect(count).toBeLessThanOrEqual(5)
  })

  test('Each feature card has icon, title, and description', async ({ page }) => {
    const featureCards = page.getByTestId('feature-card')
    const firstCard = featureCards.first()

    await expect(firstCard.getByTestId('feature-icon')).toBeVisible()
    await expect(firstCard.getByTestId('feature-title')).toBeVisible()
    await expect(firstCard.getByTestId('feature-description')).toBeVisible()
  })

  test('Feature icons contain SVG elements', async ({ page }) => {
    const iconContainers = page.getByTestId('feature-icon')
    const firstIcon = iconContainers.first()

    await expect(firstIcon).toBeVisible()
    const svg = firstIcon.locator('svg')
    await expect(svg).toBeVisible()
  })

  // Test Case 4: Hover over feature card - Card displays hover effect
  test('Test Case 4: Feature card displays hover effect', async ({ page }) => {
    const featureCard = page.getByTestId('feature-card').first()
    await expect(featureCard).toBeVisible()

    // Get initial styles
    const initialStyles = await featureCard.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        transform: styles.transform,
        boxShadow: styles.boxShadow,
        borderColor: styles.borderColor,
      }
    })

    // Hover over the card
    await featureCard.hover()

    // Wait for transition to complete
    await page.waitForTimeout(350)

    // Get hover styles
    const hoverStyles = await featureCard.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        transform: styles.transform,
        boxShadow: styles.boxShadow,
        borderColor: styles.borderColor,
      }
    })

    // Verify at least one visual property changed on hover
    const hasVisibleChange =
      initialStyles.transform !== hoverStyles.transform ||
      initialStyles.boxShadow !== hoverStyles.boxShadow ||
      initialStyles.borderColor !== hoverStyles.borderColor

    expect(hasVisibleChange).toBeTruthy()
  })

  test('Feature card hover shows scale effect', async ({ page }) => {
    const featureCard = page.getByTestId('feature-card').first()
    await expect(featureCard).toBeVisible()

    // Hover over the card
    await featureCard.hover()
    await page.waitForTimeout(350)

    // Check transform includes scale
    const transform = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).transform
    })

    // The transform should be a matrix (from scale) or contain scale
    expect(transform).not.toBe('none')
  })

  test('Feature card hover shows enhanced shadow', async ({ page }) => {
    const featureCard = page.getByTestId('feature-card').first()
    await expect(featureCard).toBeVisible()

    // Get initial shadow
    const initialShadow = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow
    })

    // Hover over the card
    await featureCard.hover()
    await page.waitForTimeout(350)

    // Get hover shadow
    const hoverShadow = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow
    })

    // Shadow should change on hover (from shadow-sm to shadow-lg)
    expect(hoverShadow).not.toBe(initialShadow)
  })

  // Test Case 5: Test grid at tablet viewport (768px)
  test('Test Case 5: Grid displays 2 columns at tablet viewport (768px)', async ({ page }) => {
    // Set viewport to tablet
    await page.setViewportSize({ width: 768, height: 1024 })
    await page.goto('/')

    const featuresGrid = page.getByTestId('features-grid')
    await expect(featuresGrid).toBeVisible()

    // Check grid layout
    const gridStyles = await featuresGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns,
      }
    })

    expect(gridStyles.display).toBe('grid')

    // At 768px (md breakpoint), should have 2 columns
    // Grid template columns should show 2 tracks
    const columnCount = gridStyles.gridTemplateColumns.split(' ').filter((col) => col !== '').length
    expect(columnCount).toBe(2)
  })

  // Test Case 6: Test grid at mobile viewport (375px)
  test('Test Case 6: Features stack vertically at mobile viewport (375px)', async ({ page }) => {
    // Set viewport to mobile
    await page.setViewportSize({ width: 375, height: 812 })
    await page.goto('/')

    const featuresGrid = page.getByTestId('features-grid')
    await expect(featuresGrid).toBeVisible()

    // Check grid layout
    const gridStyles = await featuresGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns,
      }
    })

    expect(gridStyles.display).toBe('grid')

    // At 375px (mobile), should have 1 column
    const columnCount = gridStyles.gridTemplateColumns.split(' ').filter((col) => col !== '').length
    expect(columnCount).toBe(1)
  })

  // Desktop viewport test
  test('Grid displays 3 columns at desktop viewport (1024px+)', async ({ page }) => {
    // Set viewport to desktop
    await page.setViewportSize({ width: 1280, height: 800 })
    await page.goto('/')

    const featuresGrid = page.getByTestId('features-grid')
    await expect(featuresGrid).toBeVisible()

    // Check grid layout
    const gridStyles = await featuresGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns,
      }
    })

    expect(gridStyles.display).toBe('grid')

    // At 1024px+ (lg breakpoint), should have 3 columns
    const columnCount = gridStyles.gridTemplateColumns.split(' ').filter((col) => col !== '').length
    expect(columnCount).toBe(3)
  })

  // Accessibility tests
  test('Features section has proper accessibility attributes', async ({ page }) => {
    const featuresSection = page.getByTestId('features-section')
    await expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-heading')
    await expect(featuresSection).toHaveAttribute('id', 'features')
  })

  test('Feature cards are keyboard focusable', async ({ page }) => {
    const firstCard = page.getByTestId('feature-card').first()

    // Navigate to features section
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()

    // Tab to first feature card
    await firstCard.focus()
    await expect(firstCard).toBeFocused()
  })

  test('Feature card shows focus indicator', async ({ page }) => {
    const firstCard = page.getByTestId('feature-card').first()

    // Focus the card
    await firstCard.focus()
    await page.waitForTimeout(100)

    // Check for focus indicator (ring/outline/shadow)
    const focusStyles = await firstCard.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        outline: styles.outline,
        boxShadow: styles.boxShadow,
      }
    })

    // Should have some focus indicator
    const hasFocusIndicator =
      focusStyles.boxShadow !== 'none' || focusStyles.outline !== 'none'

    expect(hasFocusIndicator).toBeTruthy()
  })

  test('Features heading is h2 element', async ({ page }) => {
    const heading = page.getByTestId('features-heading')
    await expect(heading).toBeVisible()

    const tagName = await heading.evaluate((el) => el.tagName)
    expect(tagName).toBe('H2')
  })

  test('Feature titles are h3 elements', async ({ page }) => {
    const titles = page.getByTestId('feature-title')
    const firstTitle = titles.first()
    await expect(firstTitle).toBeVisible()

    const tagName = await firstTitle.evaluate((el) => el.tagName)
    expect(tagName).toBe('H3')
  })

  // Navigation test
  test('Features section is navigable via anchor link', async ({ page }) => {
    // Navigate directly to #features
    await page.goto('/#features')

    const featuresSection = page.getByTestId('features-section')
    await expect(featuresSection).toBeInViewport()
  })
})
