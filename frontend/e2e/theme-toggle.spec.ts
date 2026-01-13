import { test, expect } from '@playwright/test'

/**
 * E2E tests for Theme Toggle Functionality
 * Validates REQ-7: Support theme switching consistent with existing dark/light mode system
 * Validates US-6: Toggle theme with immediate visual feedback and persistence
 */

test.describe('Theme Toggle E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage before each test
    await page.goto('/')
    await page.evaluate(() => localStorage.clear())
    await page.reload()
  })

  /**
   * Test Case 4 (E2E): Toggle theme and reload page
   * Input: Toggle theme and reload page
   * Expected: Theme preference is persisted in localStorage and restored on reload
   */
  test('should persist theme preference after page reload', async ({ page }) => {
    await page.goto('/')

    // Wait for page to fully load
    await page.waitForSelector('[data-testid="theme-toggle"]')

    // Get initial theme (should be dark by default)
    const initialTheme = await page.evaluate(() =>
      document.documentElement.getAttribute('data-theme')
    )
    expect(initialTheme).toBe('dark')

    // Click theme toggle to switch to light mode
    await page.click('[data-testid="theme-toggle"]')

    // Verify theme changed to light
    const themeAfterClick = await page.evaluate(() =>
      document.documentElement.getAttribute('data-theme')
    )
    expect(themeAfterClick).toBe('light')

    // Verify localStorage was updated
    const storedTheme = await page.evaluate(() => localStorage.getItem('theme'))
    expect(storedTheme).toBe('light')

    // Reload the page
    await page.reload()

    // Wait for page to load
    await page.waitForSelector('[data-testid="theme-toggle"]')

    // Verify theme is restored from localStorage
    const themeAfterReload = await page.evaluate(() =>
      document.documentElement.getAttribute('data-theme')
    )
    expect(themeAfterReload).toBe('light')

    // Verify sun icon is displayed (light mode indicator)
    await expect(page.locator('[data-testid="sun-icon"]')).toBeVisible()
  })

  test('should toggle theme immediately on click', async ({ page }) => {
    await page.goto('/')

    // Wait for page to fully load
    await page.waitForSelector('[data-testid="theme-toggle"]')

    // Initial state: dark mode with moon icon
    await expect(page.locator('[data-testid="moon-icon"]')).toBeVisible()

    // Click toggle
    await page.click('[data-testid="theme-toggle"]')

    // Immediate change: light mode with sun icon
    await expect(page.locator('[data-testid="sun-icon"]')).toBeVisible()

    // Click toggle again
    await page.click('[data-testid="theme-toggle"]')

    // Immediate change back: dark mode with moon icon
    await expect(page.locator('[data-testid="moon-icon"]')).toBeVisible()
  })

  test('should display theme toggle in navigation area', async ({ page }) => {
    await page.goto('/')

    // Wait for navigation to be visible
    await page.waitForSelector('nav')

    // Theme toggle should be within the navigation
    const nav = page.locator('nav')
    const themeToggle = nav.locator('[data-testid="theme-toggle"]')

    await expect(themeToggle).toBeVisible()
  })

  test('should maintain theme preference across multiple page navigations', async ({ page }) => {
    await page.goto('/')

    // Click theme toggle to switch to light mode
    await page.click('[data-testid="theme-toggle"]')

    // Navigate to login page
    await page.click('[data-testid="login-link"]')
    await page.waitForSelector('[data-testid="login-page"]')

    // Theme should still be light
    const themeOnLogin = await page.evaluate(() =>
      document.documentElement.getAttribute('data-theme')
    )
    expect(themeOnLogin).toBe('light')

    // Navigate back to homepage
    await page.goto('/')
    await page.waitForSelector('[data-testid="theme-toggle"]')

    // Theme should still be light
    const themeOnHome = await page.evaluate(() =>
      document.documentElement.getAttribute('data-theme')
    )
    expect(themeOnHome).toBe('light')
  })
})
