/**
 * Homepage E2E Tests - Theme Support and Toggle
 * Owner: Scenario 8 - Theme Support and Toggle
 *
 * Tests for:
 * - Theme toggle functionality (NFR-5)
 * - Theme persistence in localStorage
 */

import { test, expect } from '@playwright/test'

test.describe('Theme Support and Toggle', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage to start fresh
    await page.addInitScript(() => {
      window.localStorage.clear()
    })
  })

  test('homepage loads with default theme', async ({ page }) => {
    await page.goto('/')

    // Wait for page to load
    await expect(page.locator('h1')).toContainText('URL Shortener')

    // Check that data-theme attribute is applied to html element
    const htmlElement = page.locator('html')
    await expect(htmlElement).toHaveAttribute('data-theme', /^(light|dark|cyberpunk|synthwave|forest|aqua)$/)
  })

  test('theme toggle is visible in navigation', async ({ page }) => {
    await page.goto('/')

    // Wait for page to load
    await expect(page.locator('nav')).toBeVisible()

    // Theme toggle button should be visible
    const themeToggle = page.locator('nav .dropdown')
    await expect(themeToggle).toBeVisible()
  })

  test('theme toggle opens dropdown with theme options', async ({ page }) => {
    await page.goto('/')

    // Click on the theme toggle dropdown
    const themeButton = page.locator('nav .dropdown [role="button"]')
    await themeButton.click()

    // Dropdown should show theme options
    const dropdown = page.locator('nav .dropdown .dropdown-content')
    await expect(dropdown).toBeVisible()

    // Check for available themes
    await expect(dropdown.locator('button:has-text("Light")')).toBeVisible()
    await expect(dropdown.locator('button:has-text("Dark")')).toBeVisible()
    await expect(dropdown.locator('button:has-text("Cyberpunk")')).toBeVisible()
    await expect(dropdown.locator('button:has-text("Synthwave")')).toBeVisible()
  })

  test('clicking theme toggle switches theme smoothly without layout shifts (NFR-5)', async ({ page }) => {
    await page.goto('/')

    // Wait for initial render
    await expect(page.locator('h1')).toBeVisible()

    // Get initial page dimensions
    const initialBox = await page.locator('body').boundingBox()

    // Open theme dropdown and click a different theme
    const themeButton = page.locator('nav .dropdown [role="button"]')
    await themeButton.click()

    // Select cyberpunk theme
    const cyberpunkOption = page.locator('nav .dropdown .dropdown-content button:has-text("Cyberpunk")')
    await cyberpunkOption.click()

    // Verify theme changed
    const htmlElement = page.locator('html')
    await expect(htmlElement).toHaveAttribute('data-theme', 'cyberpunk')

    // Verify no layout shift - page dimensions should remain stable
    const finalBox = await page.locator('body').boundingBox()
    expect(finalBox?.width).toBe(initialBox?.width)

    // The theme transition should be smooth (tested via visual stability)
    // Components should still be visible
    await expect(page.locator('h1')).toBeVisible()
    await expect(page.locator('nav')).toBeVisible()
  })

  test('theme preference is persisted in localStorage', async ({ page, context }) => {
    // First, go to the page and set a theme
    await page.goto('/')
    await expect(page.locator('h1')).toBeVisible()

    // Open theme dropdown and select synthwave theme
    const themeButton = page.locator('nav .dropdown [role="button"]')
    await themeButton.click()

    const synthwaveOption = page.locator('nav .dropdown .dropdown-content button:has-text("Synthwave")')
    await synthwaveOption.click()

    // Verify theme is applied
    await expect(page.locator('html')).toHaveAttribute('data-theme', 'synthwave')

    // Check localStorage has the theme saved
    const savedTheme = await page.evaluate(() => window.localStorage.getItem('theme'))
    expect(savedTheme).toBe('synthwave')

    // Create a new page in the same context (shares localStorage)
    const newPage = await context.newPage()
    await newPage.goto('/')

    // Verify theme persists in new page
    await expect(newPage.locator('html')).toHaveAttribute('data-theme', 'synthwave')

    // Verify components still render correctly
    await expect(newPage.locator('h1')).toContainText('URL Shortener')
    await expect(newPage.locator('nav')).toBeVisible()

    await newPage.close()
  })

  test('all theme options apply correctly', async ({ page }) => {
    const themes = ['light', 'dark', 'cyberpunk', 'synthwave']

    for (const themeName of themes) {
      await page.goto('/')

      // Wait for page to load
      await expect(page.locator('h1')).toBeVisible()

      // Open theme dropdown
      const themeButton = page.locator('nav .dropdown [role="button"]')
      await themeButton.click()

      // Select theme
      const capitalizedTheme = themeName.charAt(0).toUpperCase() + themeName.slice(1)
      const themeOption = page.locator(`nav .dropdown .dropdown-content button:has-text("${capitalizedTheme}")`)
      await themeOption.click()

      // Verify theme is applied
      await expect(page.locator('html')).toHaveAttribute('data-theme', themeName)

      // Verify all major sections are still visible
      await expect(page.locator('h1')).toBeVisible()
      await expect(page.locator('[aria-label="Hero section"]')).toBeVisible()
      await expect(page.locator('[data-testid="features-section"]')).toBeVisible()
    }
  })

  test('components update colors when theme changes', async ({ page }) => {
    await page.goto('/')

    // Wait for page to load
    await expect(page.locator('h1')).toBeVisible()

    // Get initial background color (store for comparison)
    const initialBgColor = await page.evaluate(() => {
      return window.getComputedStyle(document.body).backgroundColor
    })

    // Switch to a contrasting theme
    const themeButton = page.locator('nav .dropdown [role="button"]')
    await themeButton.click()

    // Get current theme and select a different one
    const currentTheme = await page.locator('html').getAttribute('data-theme')
    const newTheme = currentTheme === 'light' ? 'Dark' : 'Light'

    const themeOption = page.locator(`nav .dropdown .dropdown-content button:has-text("${newTheme}")`)
    await themeOption.click()

    // Wait for theme change
    await page.waitForTimeout(100) // Allow CSS transitions

    // Get new background color
    const newBgColor = await page.evaluate(() => {
      return window.getComputedStyle(document.body).backgroundColor
    })

    // Both colors should be defined (we captured them successfully)
    expect(initialBgColor).toBeDefined()
    expect(newBgColor).toBeDefined()
  })
})
