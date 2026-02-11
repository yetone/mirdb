/**
 * E2E tests for Demo component.
 * Owner: Scenario 3 - Usage Demo GIF Display
 *
 * Test cases:
 * 4. Critical content renders before GIF fully loads (non-blocking)
 * 5. Page remains usable while GIF loads, shows loading state or placeholder
 */

import { test, expect } from '@playwright/test'

test.describe('Demo GIF Display E2E Tests', () => {
  // Test case 4: Critical content renders before GIF fully loads (non-blocking)
  test('critical content renders before GIF fully loads', async ({ page }) => {
    // Intercept the GIF request to simulate slow loading
    await page.route('**/usage.gif', async (route) => {
      // Delay the response by 2 seconds to simulate slow loading
      await new Promise((resolve) => setTimeout(resolve, 2000))
      await route.continue()
    })

    // Navigate to the page
    await page.goto('/')

    // Check that critical content (demo section heading) is visible immediately
    const heading = page.getByRole('heading', { name: 'See MirDB in Action' })
    await expect(heading).toBeVisible({ timeout: 1000 })

    // Check that the section is rendered
    const demoSection = page.locator('section.demo')
    await expect(demoSection).toBeVisible({ timeout: 1000 })

    // Check that demo description text is visible
    const description = page.locator('.demo__description')
    await expect(description).toBeVisible({ timeout: 1000 })
  })

  // Test case 5: Page remains usable while GIF loads, shows loading state
  test('page shows loading state while GIF loads on slow connection', async ({
    page,
  }) => {
    // Intercept the GIF request to simulate very slow 3G-like loading
    await page.route('**/usage.gif', async (route) => {
      // Hold the request to simulate ongoing loading
      await new Promise((resolve) => setTimeout(resolve, 3000))
      await route.continue()
    })

    // Navigate to the page
    await page.goto('/')

    // Check that loading placeholder is shown
    const loadingPlaceholder = page.locator('.demo__placeholder')
    await expect(loadingPlaceholder).toBeVisible({ timeout: 1000 })

    // Check that loading text is present
    const loadingText = page.getByText(/loading demo/i)
    await expect(loadingText).toBeVisible({ timeout: 1000 })

    // Check that spinner is visible
    const spinner = page.locator('.demo__spinner')
    await expect(spinner).toBeVisible({ timeout: 1000 })
  })

  test('GIF becomes visible after loading completes', async ({ page }) => {
    // Navigate to the page
    await page.goto('/')

    // Wait for the image to load
    const image = page.locator('img[src*="usage.gif"]')
    await expect(image).toBeVisible()

    // Check that the image has the loaded class after loading
    await expect(image).toHaveClass(/demo__image--loaded/)
  })

  test('demo section has proper lazy loading attribute', async ({ page }) => {
    await page.goto('/')

    // Check that the image has lazy loading attribute
    const image = page.locator('img[src*="usage.gif"]')
    await expect(image).toHaveAttribute('loading', 'lazy')
  })

  test('demo image has alt text for accessibility', async ({ page }) => {
    await page.goto('/')

    // Check that the image has alt text
    const image = page.locator('img[src*="usage.gif"]')
    const altText = await image.getAttribute('alt')

    expect(altText).toBeTruthy()
    expect(altText?.toLowerCase()).toContain('mirdb')
  })

  test('page is usable and responsive during GIF loading', async ({ page }) => {
    // Intercept the GIF to delay loading
    let resolveGif: () => void
    const gifLoadPromise = new Promise<void>((resolve) => {
      resolveGif = resolve
    })

    await page.route('**/usage.gif', async (route) => {
      await gifLoadPromise
      await route.continue()
    })

    // Navigate to the page
    await page.goto('/')

    // Page should be interactive - check that main content is accessible
    const mainContent = page.locator('main')
    await expect(mainContent).toBeVisible()

    // Demo section heading should be visible and interactive
    const heading = page.getByRole('heading', { name: 'See MirDB in Action' })
    await expect(heading).toBeVisible()

    // Now let the GIF load
    resolveGif!()

    // Image should eventually become visible
    const image = page.locator('img[src*="usage.gif"]')
    await expect(image).toHaveClass(/demo__image--loaded/, { timeout: 5000 })
  })
})
