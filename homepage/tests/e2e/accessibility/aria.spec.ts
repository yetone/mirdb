/**
 * E2E Tests for ARIA Labels
 * Owner: Scenario 11 - Accessibility Compliance
 *
 * Test Case 8: Icon-only buttons have aria-label attributes
 */

import { test, expect } from '@playwright/test'

test.describe('Test Case 8: Button ARIA Labels', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')
  })

  test('icon-only buttons have aria-label attributes', async ({ page }) => {
    // Find all buttons
    const buttons = await page.$$('button')

    for (const button of buttons) {
      const isVisible = await button.isVisible()
      if (!isVisible) continue

      // Get button text content (excluding hidden elements)
      const visibleText = await button.evaluate((el) => {
        const clone = el.cloneNode(true) as HTMLElement
        // Remove hidden elements from clone
        clone.querySelectorAll('[aria-hidden="true"]').forEach((hidden) => hidden.remove())
        return clone.textContent?.trim() || ''
      })

      // If button has no visible text, it should have aria-label
      if (visibleText === '' || visibleText.length < 2) {
        const ariaLabel = await button.getAttribute('aria-label')
        const ariaLabelledBy = await button.getAttribute('aria-labelledby')
        const title = await button.getAttribute('title')

        const hasAccessibleName = (ariaLabel && ariaLabel.trim() !== '') || ariaLabelledBy !== null || (title && title.trim() !== '')

        const buttonInfo = await button.evaluate((el) => ({
          className: el.className,
          innerHTML: el.innerHTML.substring(0, 100),
        }))

        expect(
          hasAccessibleName,
          `Icon-only button (class: ${buttonInfo.className}) should have aria-label, aria-labelledby, or title attribute`
        ).toBe(true)
      }
    }
  })

  test('theme toggle button has aria-label', async ({ page }) => {
    const themeToggle = await page.$('.theme-toggle')

    if (themeToggle) {
      const ariaLabel = await themeToggle.getAttribute('aria-label')
      expect(ariaLabel).not.toBeNull()
      expect(ariaLabel).not.toBe('')
      expect(ariaLabel?.toLowerCase()).toMatch(/theme|mode|dark|light/i)
    }
  })

  test('hamburger menu button has aria-label', async ({ page }) => {
    const hamburgerButton = await page.$('.hamburger-button')

    if (hamburgerButton) {
      const ariaLabel = await hamburgerButton.getAttribute('aria-label')
      expect(ariaLabel).not.toBeNull()
      expect(ariaLabel).not.toBe('')
      expect(ariaLabel?.toLowerCase()).toMatch(/menu|navigation/i)
    }
  })

  test('hamburger menu button has aria-expanded', async ({ page }) => {
    const hamburgerButton = await page.$('.hamburger-button')

    if (hamburgerButton) {
      const ariaExpanded = await hamburgerButton.getAttribute('aria-expanded')
      expect(ariaExpanded).not.toBeNull()
      expect(['true', 'false']).toContain(ariaExpanded)
    }
  })

  test('copy buttons have aria-label', async ({ page }) => {
    const copyButtons = await page.$$('.code-block__copy')

    for (const button of copyButtons) {
      const ariaLabel = await button.getAttribute('aria-label')
      expect(ariaLabel).not.toBeNull()
      expect(ariaLabel).not.toBe('')
      expect(ariaLabel?.toLowerCase()).toMatch(/copy|clipboard/i)
    }
  })

  test('buttons with only SVG icons have accessible names', async ({ page }) => {
    const buttons = await page.$$('button')

    for (const button of buttons) {
      const isVisible = await button.isVisible()
      if (!isVisible) continue

      // Check if button contains only SVG or icon elements
      const hasOnlyIconContent = await button.evaluate((el) => {
        const clone = el.cloneNode(true) as HTMLElement
        // Remove SVGs and icon elements
        clone.querySelectorAll('svg, [class*="icon"], [aria-hidden="true"]').forEach((icon) => icon.remove())
        return clone.textContent?.trim() === ''
      })

      if (hasOnlyIconContent) {
        const ariaLabel = await button.getAttribute('aria-label')
        const ariaLabelledBy = await button.getAttribute('aria-labelledby')
        const title = await button.getAttribute('title')

        const hasAccessibleName = (ariaLabel && ariaLabel.trim() !== '') || ariaLabelledBy !== null || (title && title.trim() !== '')

        expect(hasAccessibleName, 'Button with only icon content should have accessible name').toBe(true)
      }
    }
  })
})
