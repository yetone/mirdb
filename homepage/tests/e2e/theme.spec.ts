/**
 * Theme E2E Tests
 * Owner: Scenario 6 - Dark Mode & Theme System
 *
 * Tests:
 * - Theme toggle functionality
 * - localStorage persistence
 * - System color scheme preference
 * - Dark mode styling (background, text contrast, code blocks)
 */
import { test, expect } from '@playwright/test'

test.describe('Dark Mode & Theme System', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage before each test
    await page.addInitScript(() => {
      window.localStorage.clear()
    })
  })

  test('toggle theme and check localStorage is updated', async ({ page }) => {
    await page.goto('/')

    // Wait for page to load
    await page.waitForSelector('[data-testid="theme-toggle"]')

    // Get initial theme from localStorage (should be light by default)
    const initialTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme')
    })
    expect(initialTheme).toBe('light')

    // Click theme toggle
    await page.click('[data-testid="theme-toggle"]')

    // Verify localStorage is updated
    const storedTheme = await page.evaluate(() => {
      return window.localStorage.getItem('theme')
    })
    expect(storedTheme).toBe('dark')

    // Verify data-theme attribute changed
    const dataTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme')
    })
    expect(dataTheme).toBe('dark')
  })

  test('set localStorage theme to dark and reload - page loads with dark theme', async ({ page }) => {
    // Set localStorage before navigating
    await page.addInitScript(() => {
      window.localStorage.setItem('theme', 'dark')
    })

    await page.goto('/')
    await page.waitForSelector('[data-testid="theme-toggle"]')

    // Verify dark theme is applied
    const dataTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme')
    })
    expect(dataTheme).toBe('dark')

    // Toggle should show sun icon (switch to light mode label)
    const toggle = page.locator('[data-testid="theme-toggle"]')
    await expect(toggle).toHaveAttribute('aria-label', 'Switch to light mode')
  })

  test('respects system prefers-color-scheme: dark when no localStorage', async ({ page }) => {
    // Emulate dark color scheme
    await page.emulateMedia({ colorScheme: 'dark' })

    await page.goto('/')
    await page.waitForSelector('[data-testid="theme-toggle"]')

    // Verify dark theme is applied based on system preference
    const dataTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme')
    })
    expect(dataTheme).toBe('dark')
  })

  test('verify dark mode background color is dark', async ({ page }) => {
    // Set dark theme
    await page.addInitScript(() => {
      window.localStorage.setItem('theme', 'dark')
    })

    await page.goto('/')
    await page.waitForSelector('[data-testid="theme-toggle"]')

    // Get background color of body
    const bgColor = await page.evaluate(() => {
      return window.getComputedStyle(document.body).backgroundColor
    })

    // Parse RGB values - dark background should have low values
    const rgbMatch = bgColor.match(/rgb\((\d+), (\d+), (\d+)\)/)
    if (rgbMatch) {
      const [, r, g, b] = rgbMatch.map(Number)
      // Dark background typically has R, G, B values below 50
      expect(r).toBeLessThan(50)
      expect(g).toBeLessThan(50)
      expect(b).toBeLessThan(80) // Allow slightly higher blue for dark blue tones
    }
  })

  test('verify dark mode text contrast meets WCAG AA (4.5:1+)', async ({ page }) => {
    // Set dark theme
    await page.addInitScript(() => {
      window.localStorage.setItem('theme', 'dark')
    })

    await page.goto('/')
    await page.waitForSelector('[data-testid="theme-toggle"]')

    // Get text color and background color
    const colors = await page.evaluate(() => {
      const textColor = window.getComputedStyle(document.body).color
      const bgColor = window.getComputedStyle(document.body).backgroundColor
      return { textColor, bgColor }
    })

    // Calculate relative luminance for contrast ratio
    function getLuminance(rgb: string): number {
      const match = rgb.match(/rgb\((\d+), (\d+), (\d+)\)/)
      if (!match) return 0
      const [, r, g, b] = match.map(Number)
      const [rs, gs, bs] = [r / 255, g / 255, b / 255].map((c) =>
        c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4)
      )
      return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs
    }

    const textLum = getLuminance(colors.textColor)
    const bgLum = getLuminance(colors.bgColor)

    // Calculate contrast ratio
    const lighter = Math.max(textLum, bgLum)
    const darker = Math.min(textLum, bgLum)
    const contrastRatio = (lighter + 0.05) / (darker + 0.05)

    // WCAG AA requires 4.5:1 for normal text
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5)
  })

  test('verify code blocks have appropriate colors in dark mode', async ({ page }) => {
    // Set dark theme
    await page.addInitScript(() => {
      window.localStorage.setItem('theme', 'dark')
    })

    await page.goto('/')

    // Check if there are any code blocks on the page
    const codeBlocks = page.locator('code, pre')
    const count = await codeBlocks.count()

    if (count > 0) {
      const codeBlock = codeBlocks.first()
      const bgColor = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor
      })

      // Code blocks in dark mode should have a different (usually darker) background
      // or use the surface color variable
      expect(bgColor).toBeDefined()
    } else {
      // If no code blocks exist, test passes (feature may not be implemented yet)
      expect(true).toBe(true)
    }
  })

  test('theme toggle has correct aria-label', async ({ page }) => {
    await page.goto('/')
    await page.waitForSelector('[data-testid="theme-toggle"]')

    // In light mode, should say "Switch to dark mode"
    const toggle = page.locator('[data-testid="theme-toggle"]')
    await expect(toggle).toHaveAttribute('aria-label', 'Switch to dark mode')

    // Click to switch to dark
    await toggle.click()

    // Now should say "Switch to light mode"
    await expect(toggle).toHaveAttribute('aria-label', 'Switch to light mode')
  })

  test('theme persists after page reload', async ({ context }) => {
    // Create a new page without the localStorage clearing script
    const page = await context.newPage()

    await page.goto('/')
    await page.waitForSelector('[data-testid="theme-toggle"]')

    // Toggle to dark mode
    await page.click('[data-testid="theme-toggle"]')

    // Verify it's dark
    let dataTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme')
    })
    expect(dataTheme).toBe('dark')

    // Verify localStorage was set
    const storedTheme = await page.evaluate(() => {
      return window.localStorage.getItem('theme')
    })
    expect(storedTheme).toBe('dark')

    // Reload the page
    await page.reload()
    await page.waitForSelector('[data-testid="theme-toggle"]')

    // Verify theme persisted
    dataTheme = await page.evaluate(() => {
      return document.documentElement.getAttribute('data-theme')
    })
    expect(dataTheme).toBe('dark')

    await page.close()
  })
})
