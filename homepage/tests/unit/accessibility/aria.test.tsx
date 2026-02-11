/**
 * Unit Tests for ARIA Labels
 * Owner: Scenario 11 - Accessibility Compliance
 *
 * Test Case 8: Icon-only buttons have aria-label attributes
 */

import { render, screen } from '@testing-library/react'
import { describe, it, expect, vi, beforeEach } from 'vitest'
import App from '../../../src/App'

// Mock CSS imports
vi.mock('../../../src/styles/globals.css', () => ({}))
vi.mock('../../../src/components/layout/Header.css', () => ({}))
vi.mock('../../../src/components/layout/Footer.css', () => ({}))
vi.mock('../../../src/components/layout/Navigation.css', () => ({}))
vi.mock('../../../src/components/layout/MobileMenu.css', () => ({}))
vi.mock('../../../src/components/sections/Hero.css', () => ({}))
vi.mock('../../../src/components/sections/Demo.css', () => ({}))
vi.mock('../../../src/components/sections/Features.css', () => ({}))
vi.mock('../../../src/components/sections/QuickStart.css', () => ({}))
vi.mock('../../../src/components/ui/Button.css', () => ({}))
vi.mock('../../../src/components/ui/Badge.css', () => ({}))
vi.mock('../../../src/components/ui/FeatureCard.css', () => ({}))
vi.mock('../../../src/components/ui/CodeBlock.css', () => ({}))
vi.mock('../../../src/components/ui/ThemeToggle.css', () => ({}))

describe('Test Case 8: Button ARIA Labels', () => {
  beforeEach(() => {
    render(<App />)
  })

  it('icon-only buttons have aria-label attributes', () => {
    const buttons = document.querySelectorAll('button')

    buttons.forEach((button) => {
      // Clone to check visible text content
      const clone = button.cloneNode(true) as HTMLElement
      // Remove hidden elements from clone
      clone.querySelectorAll('[aria-hidden="true"]').forEach((hidden) => hidden.remove())
      const visibleText = clone.textContent?.trim() || ''

      // If button has no visible text or very short text, it should have aria-label
      if (visibleText === '' || visibleText.length < 2) {
        const ariaLabel = button.getAttribute('aria-label')
        const ariaLabelledBy = button.getAttribute('aria-labelledby')
        const title = button.getAttribute('title')

        const hasAccessibleName =
          (ariaLabel && ariaLabel.trim() !== '') || ariaLabelledBy !== null || (title && title.trim() !== '')

        expect(
          hasAccessibleName,
          `Icon-only button (class: ${button.className}) should have aria-label, aria-labelledby, or title attribute`
        ).toBe(true)
      }
    })
  })

  it('theme toggle button has aria-label', () => {
    const themeToggle = document.querySelector('.theme-toggle')

    if (themeToggle) {
      const ariaLabel = themeToggle.getAttribute('aria-label')
      expect(ariaLabel).not.toBeNull()
      expect(ariaLabel).not.toBe('')
      expect(ariaLabel?.toLowerCase()).toMatch(/theme|mode|dark|light/i)
    }
  })

  it('hamburger menu button has aria-label', () => {
    const hamburgerButton = document.querySelector('.hamburger-button')

    if (hamburgerButton) {
      const ariaLabel = hamburgerButton.getAttribute('aria-label')
      expect(ariaLabel).not.toBeNull()
      expect(ariaLabel).not.toBe('')
      expect(ariaLabel?.toLowerCase()).toMatch(/menu|navigation/i)
    }
  })

  it('hamburger menu button has aria-expanded', () => {
    const hamburgerButton = document.querySelector('.hamburger-button')

    if (hamburgerButton) {
      const ariaExpanded = hamburgerButton.getAttribute('aria-expanded')
      expect(ariaExpanded).not.toBeNull()
      expect(['true', 'false']).toContain(ariaExpanded)
    }
  })

  it('copy buttons have aria-label', () => {
    const copyButtons = document.querySelectorAll('.code-block__copy')

    copyButtons.forEach((button) => {
      const ariaLabel = button.getAttribute('aria-label')
      expect(ariaLabel).not.toBeNull()
      expect(ariaLabel).not.toBe('')
      expect(ariaLabel?.toLowerCase()).toMatch(/copy|clipboard/i)
    })
  })

  it('buttons with only SVG icons have accessible names', () => {
    const buttons = document.querySelectorAll('button')

    buttons.forEach((button) => {
      // Check if button contains only SVG or icon elements
      const clone = button.cloneNode(true) as HTMLElement
      // Remove SVGs and icon elements
      clone.querySelectorAll('svg, [class*="icon"], [aria-hidden="true"]').forEach((icon) => icon.remove())
      const hasOnlyIconContent = clone.textContent?.trim() === ''

      if (hasOnlyIconContent) {
        const ariaLabel = button.getAttribute('aria-label')
        const ariaLabelledBy = button.getAttribute('aria-labelledby')
        const title = button.getAttribute('title')

        const hasAccessibleName =
          (ariaLabel && ariaLabel.trim() !== '') || ariaLabelledBy !== null || (title && title.trim() !== '')

        expect(hasAccessibleName, 'Button with only icon content should have accessible name').toBe(true)
      }
    })
  })
})
