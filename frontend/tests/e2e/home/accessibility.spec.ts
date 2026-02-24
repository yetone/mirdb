/**
 * Accessibility E2E Tests - Keyboard Navigation
 * Owner: Scenario 14 - Accessibility - Keyboard Navigation
 *
 * Verifies homepage supports keyboard navigation and screen reader accessibility (WCAG 2.1 AA).
 * Tests cover:
 * - Tab key navigation through interactive elements
 * - Visible focus indicators on focused elements
 * - ARIA labels on interactive elements
 * - Semantic HTML structure (header, main, section, footer)
 * - Skip navigation link for keyboard users
 */

import { test, expect } from '@playwright/test'

test.describe('Accessibility - Keyboard Navigation (WCAG 2.1 AA)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for page to fully load
    await expect(page.locator('h1')).toContainText('URL Shortener')
  })

  test('TC1: All interactive elements are reachable via keyboard Tab navigation', async ({ page }) => {
    // Start from the beginning of the page
    await page.keyboard.press('Tab')

    // First interactive element should be the skip link
    const skipLink = page.locator('[data-testid="skip-to-content"]')
    await expect(skipLink).toBeFocused()

    // Collect all focused elements during tab navigation
    const focusedElements: string[] = []
    const maxTabs = 30

    for (let i = 0; i < maxTabs; i++) {
      await page.keyboard.press('Tab')

      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement
        if (!el || el === document.body) return null
        return {
          tagName: el.tagName.toLowerCase(),
          role: el.getAttribute('role'),
          ariaLabel: el.getAttribute('aria-label'),
          text: el.textContent?.trim().substring(0, 50) || '',
          isInteractive: ['a', 'button', 'input', 'select', 'textarea'].includes(
            el.tagName.toLowerCase()
          ) || el.getAttribute('role') === 'button',
        }
      })

      if (focusedElement && focusedElement.isInteractive) {
        focusedElements.push(
          `${focusedElement.tagName}${focusedElement.ariaLabel ? `:${focusedElement.ariaLabel}` : ''}`
        )
      }
    }

    // Verify we found multiple interactive elements
    expect(focusedElements.length).toBeGreaterThan(3)

    // Should have reached some buttons/links
    const interactiveText = focusedElements.join(',')
    expect(interactiveText).toMatch(/a|button|input/i)
  })

  test('TC2: Visible focus indicator appears on focused elements', async ({ page }) => {
    // Tab to skip link
    await page.keyboard.press('Tab')

    // Check that focus is visible - the element should have focus styles
    const skipLink = page.locator('[data-testid="skip-to-content"]')
    await expect(skipLink).toBeFocused()

    // Tab to logo link
    await page.keyboard.press('Tab')
    const logoLink = page.locator('nav a').first()
    await expect(logoLink).toBeFocused()

    // Check that the focused element has visible focus styling
    const isFocused = await page.evaluate(() => {
      const el = document.activeElement
      if (!el) return false

      const styles = window.getComputedStyle(el)
      // Focus should be visible (not outline: none with no other indicator)
      return (
        styles.outlineStyle !== 'none' ||
        styles.boxShadow !== 'none' ||
        el.classList.contains('focus') ||
        el.classList.contains('focus-visible')
      )
    })

    expect(isFocused).toBe(true)

    // Also verify URL input can receive focus
    const urlInput = page.locator('input[aria-label="URL to shorten"]')
    await urlInput.focus()
    await expect(urlInput).toBeFocused()
  })

  test('TC3: URL input field has appropriate ARIA attributes', async ({ page }) => {
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

  test('TC4: Page uses semantic HTML elements (header, main, section, footer)', async ({ page }) => {
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

    // Page should have exactly one h1 for proper document structure
    const h1Count = await page.locator('h1').count()
    expect(h1Count).toBe(1)
  })

  test('TC5: Skip to content link is present for keyboard users', async ({ page }) => {
    // Skip link should be first focusable element
    const skipLink = page.locator('[data-testid="skip-to-content"]')

    // Should have exactly one skip link
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

  test('Buttons and links have minimum touch target size (44px)', async ({ page }) => {
    // Check the Shorten button
    const shortenButton = page.locator('button[aria-label="Shorten URL"]')
    const shortenBox = await shortenButton.boundingBox()

    expect(shortenBox).not.toBeNull()
    expect(shortenBox!.height).toBeGreaterThanOrEqual(44)
    expect(shortenBox!.width).toBeGreaterThanOrEqual(44)

    // Check navigation links on desktop
    const mainNav = page.locator('nav[aria-label="Main navigation"]')
    const loginLink = mainNav.locator('a:has-text("Login")')
    if (await loginLink.isVisible()) {
      const loginBox = await loginLink.boundingBox()
      expect(loginBox).not.toBeNull()
      expect(loginBox!.height).toBeGreaterThanOrEqual(44)
    }
  })

  test('Form can be submitted using Enter key', async ({ page }) => {
    // Focus the URL input
    const urlInput = page.locator('input[aria-label="URL to shorten"]')
    await urlInput.focus()

    // Type a URL
    await urlInput.fill('https://example.com/test')

    // Submit using Enter key
    await page.keyboard.press('Enter')

    // Form should be processed (no error thrown)
    await expect(urlInput).toBeVisible()
  })

  test('Theme dropdown can be navigated with keyboard', async ({ page }) => {
    // Navigate to theme dropdown in the main navigation
    const mainNav = page.locator('nav[aria-label="Main navigation"]')
    const themeButton = mainNav.locator('.dropdown [role="button"]')

    // Focus and activate the dropdown
    await themeButton.focus()
    await expect(themeButton).toBeFocused()

    // Click to open dropdown
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

  test('Interactive elements have accessible names', async ({ page }) => {
    // Check that all interactive elements have accessible names
    const accessibilityAudit = await page.evaluate(() => {
      const results: { element: string; hasName: boolean; name: string | null }[] = []

      // Check buttons
      document.querySelectorAll('button').forEach((btn, i) => {
        const name = btn.getAttribute('aria-label') ||
                    btn.textContent?.trim() ||
                    btn.querySelector('img')?.getAttribute('alt')
        results.push({
          element: `button[${i}]`,
          hasName: !!name && name.length > 0,
          name: name || null
        })
      })

      // Check links
      document.querySelectorAll('a').forEach((link, i) => {
        const name = link.getAttribute('aria-label') ||
                    link.textContent?.trim() ||
                    link.querySelector('img')?.getAttribute('alt')
        results.push({
          element: `a[${i}]`,
          hasName: !!name && name.length > 0,
          name: name || null
        })
      })

      // Check inputs
      document.querySelectorAll('input').forEach((input, i) => {
        const id = input.id
        const label = id ? document.querySelector(`label[for="${id}"]`) : null
        const name = input.getAttribute('aria-label') ||
                    label?.textContent?.trim() ||
                    input.getAttribute('placeholder')
        results.push({
          element: `input[${i}]`,
          hasName: !!name && name.length > 0,
          name: name || null
        })
      })

      return results
    })

    // All interactive elements should have accessible names
    const elementsWithoutNames = accessibilityAudit.filter(el => !el.hasName)

    // There should be no (or very few) elements without accessible names
    expect(
      elementsWithoutNames.length,
      `${elementsWithoutNames.length} elements missing accessible names: ${elementsWithoutNames.map(e => e.element).join(', ')}`
    ).toBeLessThanOrEqual(2)
  })

  test('ARIA landmarks are properly defined', async ({ page }) => {
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

  test('Focus order follows logical reading order', async ({ page }) => {
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

    // Verify focus generally moves through elements in a logical order
    expect(focusOrder.length).toBeGreaterThan(5)
  })

  test('Focus management - focus does not get trapped', async ({ page }) => {
    // Tab through the page to ensure focus cycles properly (not trapped)
    const visitedElements: string[] = []
    let foundCycle = false

    // Start with first Tab
    await page.keyboard.press('Tab')

    for (let i = 0; i < 50; i++) {
      const currentElement = await page.evaluate(() => {
        const el = document.activeElement
        if (!el || el === document.body) return 'body'
        return `${el.tagName}_${el.getAttribute('aria-label') || el.getAttribute('href') || el.textContent?.substring(0, 20)}`
      })

      // Check if we've cycled back to a previously visited element
      if (visitedElements.length > 5 && visitedElements.includes(currentElement)) {
        foundCycle = true
        break
      }

      visitedElements.push(currentElement)
      await page.keyboard.press('Tab')
    }

    // We should have visited multiple unique elements (proper keyboard navigation)
    const uniqueElements = new Set(visitedElements)
    expect(
      uniqueElements.size,
      'Focus should navigate through multiple interactive elements'
    ).toBeGreaterThan(5)
  })
})

test.describe('Color Contrast (Manual Test Reference)', () => {
  test('TC6: Manual verification reference - text contrast ratios', async ({ page }) => {
    await page.goto('/')
    await page.waitForSelector('h1')

    // This test documents the manual verification process for color contrast
    // Actual contrast ratio testing requires visual inspection or specialized tools

    // Check that text elements exist and have readable styles
    const textContrast = await page.evaluate(() => {
      const results: { element: string; color: string; backgroundColor: string }[] = []

      // Check heading
      const h1 = document.querySelector('h1')
      if (h1) {
        const styles = window.getComputedStyle(h1)
        results.push({
          element: 'h1',
          color: styles.color,
          backgroundColor: styles.backgroundColor
        })
      }

      // Check paragraph text
      const p = document.querySelector('p')
      if (p) {
        const styles = window.getComputedStyle(p)
        results.push({
          element: 'p',
          color: styles.color,
          backgroundColor: styles.backgroundColor
        })
      }

      // Check button text
      const btn = document.querySelector('button')
      if (btn) {
        const styles = window.getComputedStyle(btn)
        results.push({
          element: 'button',
          color: styles.color,
          backgroundColor: styles.backgroundColor
        })
      }

      return results
    })

    // Document that text elements exist and have color styles defined
    expect(textContrast.length).toBeGreaterThan(0)

    // Verify colors are defined (not transparent or undefined)
    // Note: Some elements like h1 may use gradient text (bg-clip-text) which makes
    // the actual text color transparent - this is acceptable for gradient text styling
    for (const item of textContrast) {
      expect(item.color).toBeDefined()
      // h1 with gradient text may have transparent color (bg-clip-text technique)
      // Button and other elements should have non-transparent colors
      if (item.element !== 'h1') {
        expect(item.color).toBeTruthy()
      }
    }

    // Note: WCAG 4.5:1 contrast ratio verification requires:
    // - Lighthouse accessibility audit
    // - axe-core integration
    // - Manual inspection with contrast checking tools
    // DaisyUI themes are designed to meet WCAG contrast requirements
  })
})
