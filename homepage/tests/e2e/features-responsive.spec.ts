/**
 * E2E tests for Features section responsive layout.
 * Owner: Scenario 3 - Features Section
 *
 * Test cases covered:
 * - TC4: Desktop viewport (>1024px) - 3-column grid layout
 * - TC5: Tablet viewport (768-1024px) - 2-column grid layout
 * - TC6: Mobile viewport (<768px) - single column stacked layout
 */

import { test, expect } from '@playwright/test'

test.describe('Features Section Responsive Layout', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  // TC4: Render at desktop viewport (>1024px) - Features display in 3-column grid layout
  test('TC4: displays 3-column grid layout on desktop (>1024px)', async ({
    page,
  }) => {
    await page.setViewportSize({ width: 1280, height: 800 })
    await page.waitForTimeout(100)

    const grid = page.getByTestId('features-grid')
    await expect(grid).toBeVisible()

    const computedStyle = await grid.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns,
      }
    })

    expect(computedStyle.display).toBe('grid')
    const columnCount =
      computedStyle.gridTemplateColumns.split(' ').filter(Boolean).length
    expect(columnCount).toBe(3)
  })

  // TC5: Render at tablet viewport (768-1024px) - Features display in 2-column grid layout
  test('TC5: displays 2-column grid layout on tablet (768-1024px)', async ({
    page,
  }) => {
    await page.setViewportSize({ width: 900, height: 800 })
    await page.waitForTimeout(100)

    const grid = page.getByTestId('features-grid')
    await expect(grid).toBeVisible()

    const computedStyle = await grid.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns,
      }
    })

    expect(computedStyle.display).toBe('grid')
    const columnCount =
      computedStyle.gridTemplateColumns.split(' ').filter(Boolean).length
    expect(columnCount).toBe(2)
  })

  // TC6: Render at mobile viewport (<768px) - Features display in single column stacked layout
  test('TC6: displays single column layout on mobile (<768px)', async ({
    page,
  }) => {
    await page.setViewportSize({ width: 375, height: 667 })
    await page.waitForTimeout(100)

    const grid = page.getByTestId('features-grid')
    await expect(grid).toBeVisible()

    const computedStyle = await grid.evaluate((el) => {
      const style = window.getComputedStyle(el)
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns,
      }
    })

    expect(computedStyle.display).toBe('grid')
    const columnCount =
      computedStyle.gridTemplateColumns.split(' ').filter(Boolean).length
    expect(columnCount).toBe(1)
  })

  test('feature cards are visible at all viewport sizes', async ({ page }) => {
    const viewports = [
      { width: 1280, height: 800, name: 'desktop' },
      { width: 900, height: 800, name: 'tablet' },
      { width: 375, height: 667, name: 'mobile' },
    ]

    for (const viewport of viewports) {
      await page.setViewportSize({
        width: viewport.width,
        height: viewport.height,
      })
      await page.waitForTimeout(100)

      const cards = page.getByTestId('feature-card')
      const count = await cards.count()

      expect(count).toBeGreaterThanOrEqual(3)

      for (let i = 0; i < Math.min(count, 3); i++) {
        await expect(cards.nth(i)).toBeVisible()
      }
    }
  })

  test('features section has accessible heading', async ({ page }) => {
    const heading = page.getByRole('heading', { name: /features/i })
    await expect(heading).toBeVisible()
  })

  test('features section is accessible with proper ARIA attributes', async ({
    page,
  }) => {
    const section = page.locator('#features')
    await expect(section).toHaveAttribute('aria-labelledby', 'features-heading')

    const heading = page.locator('#features-heading')
    await expect(heading).toBeVisible()
  })
})
