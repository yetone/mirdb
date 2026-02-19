/**
 * Responsive Design E2E Tests
 * Owner: Scenario 7 (responsive tests), Scenario 3 (features grid tests)
 *
 * Test cases:
 * 6. Set viewport to 1200px width (desktop) - Feature cards display in 3-column grid
 * 7. Set viewport to 768px width (tablet) - Feature cards display in 2-column grid
 * 8. Set viewport to 375px width (mobile) - Feature cards stack in single column
 */

import { test, expect } from '@playwright/test'

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
    // The gridTemplateColumns will show actual pixel values like "350px 350px 350px"
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
