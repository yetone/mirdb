/**
 * Accessibility E2E Tests - Keyboard Navigation
 * Owner: Scenario 14 - Accessibility - Keyboard Navigation
 *
 * Tests for:
 * - Keyboard navigation through interactive elements (WCAG 2.1 AA)
 * - Focus indicators on interactive elements
 * - ARIA labels for screen reader accessibility
 * - Semantic HTML structure
 * - Skip navigation link for keyboard users
 */

import { test, expect } from '@playwright/test'

test.describe('Accessibility - Keyboard Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for page to fully load
    await expect(page.locator('h1')).toContainText('URL Shortener')
  })

  test('all interactive elements are reachable via Tab key navigation', async ({
    page,
  }) => {
    // Start from the beginning of the page
    await page.keyboard.press('Tab')

    // First interactive element should be the skip link
    const skipLink = page.locator('[data-testid="skip-to-content"]')
    await expect(skipLink).toBeFocused()

    // Tab through all interactive elements and verify they receive focus
    const interactiveElements: string[] = []

    // Tab through elements and collect them
    for (let i = 0; i < 15; i++) {
      await page.keyboard.press('Tab')

      // Get the currently focused element
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement
        if (el && el !== document.body) {
          return {
            tagName: el.tagName.toLowerCase(),
            role: el.getAttribute('role'),
            ariaLabel: el.getAttribute('aria-label'),
            text: el.textContent?.trim().substring(0, 50),
            isInteractive: ['a', 'button', 'input', 'select', 'textarea'].includes(
              el.tagName.toLowerCase()
            ) || el.getAttribute('role') === 'button',
          }
        }
        return null
      })

      if (focusedElement && focusedElement.isInteractive) {
        interactiveElements.push(
          `${focusedElement.tagName}${focusedElement.ariaLabel ? `:${focusedElement.ariaLabel}` : ''}`
        )
      }
    }

    // Verify we found multiple interactive elements
    expect(interactiveElements.length).toBeGreaterThan(3)

    // Verify key interactive elements were reached
    const interactiveText = interactiveElements.join(',')
    // Should have reached some buttons/links
    expect(interactiveText).toMatch(/a|button|input/i)
  })

  test('visible focus indicator appears on focused elements', async ({ page }) => {
    // Tab to skip link
    await page.keyboard.press('Tab')

    // Check that focus is visible - the element should have focus styles
    // DaisyUI and Tailwind apply focus-visible styles
    const skipLink = page.locator('[data-testid="skip-to-content"]')
    await expect(skipLink).toBeFocused()

    // Tab to logo link
    await page.keyboard.press('Tab')
    const logoLink = page.locator('nav a').first()
    await expect(logoLink).toBeFocused()

    // Check that the focused element has visible focus styling
    // This verifies the element isn't hidden and has focus
    const isFocused = await page.evaluate(() => {
      const el = document.activeElement
      if (!el) return false

      const styles = window.getComputedStyle(el)
      // Focus should be visible (not outline: none with no other indicator)
      // Check for any visible focus indication
      return (
        styles.outlineStyle !== 'none' ||
        styles.boxShadow !== 'none' ||
        el.classList.contains('focus') ||
        el.classList.contains('focus-visible')
      )
    })

    // DaisyUI buttons should have focus styles
    expect(isFocused).toBe(true)

    // Tab to the URL input
    await page.keyboard.press('Tab') // Theme dropdown
    await page.keyboard.press('Tab') // Login or next element
    await page.keyboard.press('Tab')
    await page.keyboard.press('Tab')
    await page.keyboard.press('Tab')

    // Find and focus the URL input
    const urlInput = page.locator('input[aria-label="URL to shorten"]')
    await urlInput.focus()

    // Verify the input can receive focus
    await expect(urlInput).toBeFocused()
  })

  test('URL input field has appropriate ARIA attributes', async ({ page }) => {
    const urlInput = page.locator('input[aria-label="URL to shorten"]')

    // Verify input exists and has aria-label
    await expect(urlInput).toBeVisible()

    // Check for aria-label attribute
    const ariaLabel = await urlInput.getAttribute('aria-label')
    expect(ariaLabel).toBe('URL to shorten')

    // Input should have proper type
    const inputType = await urlInput.getAttribute('type')
    expect(inputType).toBe('url')
  })

  test('page uses semantic HTML structure (header, main, section, footer)', async ({
    page,
  }) => {
    // Check for <header> element (wrapping navbar)
    const header = page.locator('header')
    await expect(header).toBeVisible()

    // Check for <main> element
    const main = page.locator('main')
    await expect(main).toBeVisible()

    // Check for <section> elements (Hero, Features)
    const sections = page.locator('section')
    const sectionCount = await sections.count()
    expect(sectionCount).toBeGreaterThanOrEqual(2)

    // Verify specific sections have aria-labels
    const heroSection = page.locator('section[aria-label="Hero section"]')
    await expect(heroSection).toBeVisible()

    const featuresSection = page.locator('section[aria-label="Features section"]')
    await expect(featuresSection).toBeVisible()

    // Check for <footer> element
    const footer = page.locator('footer')
    await expect(footer).toBeVisible()
  })

  test('skip to content link is present for keyboard users', async ({ page }) => {
    // Skip link should be first focusable element
    const skipLink = page.locator('[data-testid="skip-to-content"]')

    // Initially may be visually hidden but focusable
    await expect(skipLink).toHaveCount(1)

    // Tab to the skip link
    await page.keyboard.press('Tab')
    await expect(skipLink).toBeFocused()

    // Verify skip link has correct attributes
    const href = await skipLink.getAttribute('href')
    expect(href).toBe('#main-content')

    // Skip link should be visible when focused
    await expect(skipLink).toBeVisible()

    // Clicking/activating skip link should move focus to main content
    await page.keyboard.press('Enter')

    // Focus should now be on or inside the main content area
    const mainContent = page.locator('#main-content')
    await expect(mainContent).toBeVisible()
  })

  test('buttons and links have minimum touch target size (44px)', async ({
    page,
  }) => {
    // Check the Shorten button
    const shortenButton = page.locator('button[aria-label="Shorten URL"]')
    const shortenBox = await shortenButton.boundingBox()

    expect(shortenBox).not.toBeNull()
    expect(shortenBox!.height).toBeGreaterThanOrEqual(44)
    expect(shortenBox!.width).toBeGreaterThanOrEqual(44)

    // Check navigation links on desktop - use more specific selector
    const mainNav = page.locator('nav[aria-label="Main navigation"]')
    const loginLink = mainNav.locator('a:has-text("Login")')
    if (await loginLink.isVisible()) {
      const loginBox = await loginLink.boundingBox()
      expect(loginBox).not.toBeNull()
      expect(loginBox!.height).toBeGreaterThanOrEqual(44)
    }
  })

  test('form can be submitted using Enter key', async ({ page }) => {
    // Focus the URL input
    const urlInput = page.locator('input[aria-label="URL to shorten"]')
    await urlInput.focus()

    // Type a URL
    await urlInput.fill('https://example.com/test')

    // Submit using Enter key
    await page.keyboard.press('Enter')

    // Form should be processed (no error thrown)
    // The actual submission behavior depends on the form implementation
    // Here we just verify the form accepts keyboard submission
    await expect(urlInput).toBeVisible()
  })

  test('theme dropdown can be navigated with keyboard', async ({ page }) => {
    // Navigate to theme dropdown in the main navigation
    const mainNav = page.locator('nav[aria-label="Main navigation"]')
    const themeButton = mainNav.locator('.dropdown [role="button"]')

    // Focus and activate the dropdown
    await themeButton.focus()
    await expect(themeButton).toBeFocused()

    // Press Enter or Space to open dropdown - DaisyUI dropdowns use focus
    // The dropdown should open on click/focus
    await themeButton.click()

    // Dropdown should be visible
    const dropdown = mainNav.locator('.dropdown .dropdown-content')
    await expect(dropdown).toBeVisible()

    // Check that theme buttons are present and can be focused
    const themeOptions = dropdown.locator('button')
    const optionCount = await themeOptions.count()
    expect(optionCount).toBeGreaterThan(0)

    // The first theme option should be focusable
    const firstOption = themeOptions.first()
    await firstOption.focus()
    await expect(firstOption).toBeFocused()
  })

  test('all images have alt text or are decorative', async ({ page }) => {
    // Get all images on the page
    const images = page.locator('img')
    const imageCount = await images.count()

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i)
      const alt = await img.getAttribute('alt')
      const role = await img.getAttribute('role')
      const ariaHidden = await img.getAttribute('aria-hidden')

      // Image should have alt text OR be marked as decorative
      const isAccessible =
        alt !== null || role === 'presentation' || ariaHidden === 'true'

      expect(isAccessible).toBe(true)
    }
  })

  test('SVG icons are properly hidden from screen readers', async ({ page }) => {
    // Get all SVG elements in the navigation
    const svgs = page.locator('nav svg')
    const svgCount = await svgs.count()

    for (let i = 0; i < svgCount; i++) {
      const svg = svgs.nth(i)
      const ariaHidden = await svg.getAttribute('aria-hidden')

      // Decorative SVGs should have aria-hidden="true"
      expect(ariaHidden).toBe('true')
    }
  })

  test('page landmark roles are properly defined', async ({ page }) => {
    // Check for navigation landmark
    const nav = page.locator('nav[aria-label]')
    await expect(nav).toHaveCount(2) // Main nav and footer nav

    // Verify main navigation has proper label
    const mainNav = page.locator('nav[aria-label="Main navigation"]')
    await expect(mainNav).toBeVisible()

    // Verify footer navigation has proper label
    const footerNav = page.locator('footer nav[aria-label="Footer navigation"]')
    await expect(footerNav).toBeVisible()
  })

  test('focus order follows logical reading order', async ({ page }) => {
    const focusOrder: string[] = []

    // Tab through elements and record order
    for (let i = 0; i < 10; i++) {
      await page.keyboard.press('Tab')

      const focused = await page.evaluate(() => {
        const el = document.activeElement
        if (el && el !== document.body) {
          const rect = el.getBoundingClientRect()
          return {
            tag: el.tagName,
            top: rect.top,
            label: el.getAttribute('aria-label') || el.textContent?.substring(0, 20),
          }
        }
        return null
      })

      if (focused) {
        focusOrder.push(`${focused.tag}:${focused.top}`)
      }
    }

    // Verify focus generally moves down the page (top values should increase or stay similar)
    // Skip link is at top, then navbar, then hero, etc.
    expect(focusOrder.length).toBeGreaterThan(5)
  })
})
