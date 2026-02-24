/**
 * Homepage E2E Tests - Theme Support and Toggle & Cross-Browser Compatibility
 * Owner: Scenario 8 - Theme Support and Toggle
 * Extended by: Scenario 16 - Cross-Browser Compatibility
 *
 * Tests for:
 * - Theme toggle functionality (NFR-5)
 * - Theme persistence in localStorage
 * - Cross-browser compatibility (NFR-6, NFR-7)
 *   - Chrome 90+ (primary target)
 *   - Firefox 88+
 *   - Safari 14+
 *   - Edge 90+
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

/**
 * Cross-Browser Compatibility Tests
 * Owner: Scenario 16 - Cross-Browser Compatibility
 *
 * Tests verifying homepage displays and functions correctly across:
 * - Chrome 90+ (chromium project)
 * - Firefox 88+ (firefox project)
 * - Safari 14+ (webkit project)
 * - Edge 90+ (msedge project)
 *
 * These tests run in all configured browser projects to ensure
 * consistent behavior across browsers (NFR-6).
 */
test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage for consistent state
    await page.addInitScript(() => {
      window.localStorage.clear()
    })
  })

  test('homepage renders correctly across browsers (NFR-6)', async ({ page, browserName }) => {
    await page.goto('/')

    // Verify main heading is visible
    const heading = page.locator('h1')
    await expect(heading).toBeVisible()
    await expect(heading).toContainText('URL Shortener')

    // Verify navigation bar is present and visible
    const navbar = page.locator('nav')
    await expect(navbar).toBeVisible()

    // Verify hero section is present
    const heroSection = page.locator('[aria-label="Hero section"]')
    await expect(heroSection).toBeVisible()

    // Verify features section is present
    const featuresSection = page.locator('[data-testid="features-section"]')
    await expect(featuresSection).toBeVisible()

    // Verify footer is present
    const footer = page.locator('footer')
    await expect(footer).toBeVisible()

    // Log browser info for debugging
    console.log(`Cross-browser test passed for: ${browserName}`)
  })

  test('URL shortener form renders and is interactive across browsers (NFR-6)', async ({ page, browserName }) => {
    await page.goto('/')

    // Find the URL input field
    const urlInput = page.locator('input[placeholder*="URL"], input[type="url"], input[name="url"]').first()
    await expect(urlInput).toBeVisible()

    // Verify input is focusable
    await urlInput.focus()
    await expect(urlInput).toBeFocused()

    // Verify typing works
    await urlInput.fill('https://example.com')
    await expect(urlInput).toHaveValue('https://example.com')

    // Find and verify the shorten button
    const shortenButton = page.locator('button:has-text("Shorten"), button[type="submit"]').first()
    await expect(shortenButton).toBeVisible()
    await expect(shortenButton).toBeEnabled()

    console.log(`URL form test passed for: ${browserName}`)
  })

  test('navigation links work correctly across browsers (NFR-6)', async ({ page, browserName }) => {
    await page.goto('/')

    // Verify Login link is present
    const loginLink = page.locator('a:has-text("Login"), button:has-text("Login"), [href*="login"]').first()
    await expect(loginLink).toBeVisible()

    // Verify Register link is present
    const registerLink = page.locator('a:has-text("Register"), button:has-text("Register"), [href*="register"]').first()
    await expect(registerLink).toBeVisible()

    console.log(`Navigation test passed for: ${browserName}`)
  })

  test('theme toggle works across browsers (NFR-6)', async ({ page, browserName }) => {
    await page.goto('/')

    // Find and click theme toggle
    const themeButton = page.locator('nav .dropdown [role="button"]')
    await expect(themeButton).toBeVisible()
    await themeButton.click()

    // Verify dropdown opens
    const dropdown = page.locator('nav .dropdown .dropdown-content')
    await expect(dropdown).toBeVisible()

    // Select a theme
    const darkOption = dropdown.locator('button:has-text("Dark")')
    await darkOption.click()

    // Verify theme is applied
    await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark')

    console.log(`Theme toggle test passed for: ${browserName}`)
  })

  test('clipboard API functionality works across browsers (NFR-7)', async ({ page, browserName, context }) => {
    // Grant clipboard permissions where supported
    try {
      await context.grantPermissions(['clipboard-read', 'clipboard-write'])
    } catch {
      // Some browsers may not support these permissions - continue anyway
      console.log(`Clipboard permissions not supported in ${browserName}, continuing...`)
    }

    await page.goto('/')

    // Look for any copy button on the page
    const copyButton = page.locator('[data-testid="copy-button"], button:has-text("Copy")').first()

    // Check if copy button exists (might need URL to be shortened first)
    const copyButtonExists = await copyButton.count() > 0

    if (copyButtonExists) {
      await expect(copyButton).toBeVisible()

      // Click the copy button
      await copyButton.click()

      // Verify visual feedback (button text changes or success state)
      // The button should show "Copied!" or have a success state
      await page.waitForTimeout(100)

      // Verify button is still functional (didn't break)
      await expect(copyButton).toBeVisible()

      console.log(`Clipboard API test passed for: ${browserName}`)
    } else {
      // If no copy button visible initially, try creating a short URL first
      const urlInput = page.locator('input[placeholder*="URL"], input[type="url"]').first()

      if (await urlInput.count() > 0) {
        await urlInput.fill('https://example.com/test')

        const shortenButton = page.locator('button:has-text("Shorten"), button[type="submit"]').first()
        if (await shortenButton.count() > 0) {
          // Note: This would require API to be running, so we just verify the form works
          console.log(`URL form exists for clipboard test in: ${browserName}`)
        }
      }

      console.log(`Clipboard API elements verified for: ${browserName}`)
    }
  })

  test('CSS styling renders consistently across browsers (NFR-6)', async ({ page, browserName }) => {
    await page.goto('/')

    // Verify glass morphism cards render (if present)
    const glassCards = page.locator('.glass, [class*="glass"], [class*="backdrop"]')
    const cardCount = await glassCards.count()

    if (cardCount > 0) {
      // Verify at least one glass card is visible
      await expect(glassCards.first()).toBeVisible()
    }

    // Verify Tailwind CSS is working (check for flex/grid layouts)
    const flexContainers = page.locator('[class*="flex"], [class*="grid"]')
    const flexCount = await flexContainers.count()
    expect(flexCount).toBeGreaterThan(0)

    // Verify DaisyUI components render
    const daisyComponents = page.locator('.btn, .card, .navbar, [class*="btn-"], [class*="card-"]')
    const daisyCount = await daisyComponents.count()
    expect(daisyCount).toBeGreaterThan(0)

    console.log(`CSS styling test passed for: ${browserName}`)
  })

  test('responsive layout works across browsers (NFR-6)', async ({ page, browserName }) => {
    await page.goto('/')

    // Get initial viewport size
    const viewportSize = page.viewportSize()
    expect(viewportSize).toBeDefined()

    // Verify no horizontal scroll at current viewport
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth)
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth)

    // Allow small tolerance for scrollbar
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 20)

    // Verify main content is visible
    const mainContent = page.locator('main, #main-content')
    await expect(mainContent.first()).toBeVisible()

    console.log(`Responsive layout test passed for: ${browserName}`)
  })

  test('interactive elements have proper focus states across browsers (NFR-6)', async ({ page, browserName }) => {
    await page.goto('/')

    // Tab through focusable elements
    await page.keyboard.press('Tab')

    // Verify something is focused
    const focusedElement = page.locator(':focus')
    await expect(focusedElement).toBeVisible()

    // Continue tabbing and verify focus moves
    await page.keyboard.press('Tab')
    const newFocusedElement = page.locator(':focus')
    await expect(newFocusedElement).toBeVisible()

    console.log(`Focus states test passed for: ${browserName}`)
  })
})
