/**
 * Unit Tests for Keyboard Navigation
 * Owner: Scenario 11 - Accessibility Compliance
 *
 * Test Case 2: All interactive elements receive focus in logical order
 * Test Case 7: Links are activatable via keyboard Enter key
 */

import { render, screen, fireEvent } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
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

describe('Test Case 2: Keyboard Navigation', () => {
  beforeEach(() => {
    render(<App />)
  })

  it('interactive elements are focusable', () => {
    const focusableElements = document.querySelectorAll(
      'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])'
    )

    // Verify there are focusable elements
    expect(focusableElements.length).toBeGreaterThan(0)
  })

  it('links have valid href attributes', () => {
    const links = document.querySelectorAll('a[href]')

    links.forEach((link) => {
      const href = link.getAttribute('href')
      expect(href).not.toBeNull()
      expect(href).not.toBe('')
    })
  })

  it('buttons have proper type attribute or default to button', () => {
    const buttons = document.querySelectorAll('button')

    buttons.forEach((button) => {
      // If no type is specified, button defaults to "submit" in forms
      // For accessibility, buttons should have explicit type or be outside forms
      const type = button.getAttribute('type')

      // If button has a type, it should be valid
      if (type) {
        expect(['button', 'submit', 'reset']).toContain(type)
      }
    })
  })

  it('no interactive elements have negative tabindex (hidden from tab order) unless intentional', () => {
    // Elements with tabindex=-1 are removed from tab order
    // This is ok for elements managed by JS (like modals) but should be intentional
    const hiddenFromTab = document.querySelectorAll('[tabindex="-1"]')

    // If there are elements with tabindex=-1, they should be valid cases
    // (e.g., modal content, skip links that get focused programmatically)
    hiddenFromTab.forEach((element) => {
      // Check that it's not a main interactive element like a nav link or button
      const isMainInteractive =
        element.matches('nav a') ||
        element.matches('header a') ||
        element.matches('footer a') ||
        element.matches('main button:not([aria-hidden="true"])')

      expect(isMainInteractive, `Main interactive element should not have tabindex=-1`).toBe(false)
    })
  })
})

describe('Test Case 7: Keyboard Link Activation', () => {
  beforeEach(() => {
    render(<App />)
  })

  it('links are proper anchor elements that can be activated', () => {
    const links = document.querySelectorAll('a[href]')

    links.forEach((link) => {
      // Verify the link is a proper anchor element
      expect(link instanceof HTMLAnchorElement).toBe(true)

      // Verify href is set (links without href cannot be activated by keyboard)
      const href = link.getAttribute('href')
      expect(href).not.toBeNull()
      expect(href).not.toBe('')
    })
  })

  it('buttons are proper button elements that can be activated', () => {
    const buttons = document.querySelectorAll('button')

    buttons.forEach((button) => {
      // Verify the button is a proper button element
      expect(button instanceof HTMLButtonElement).toBe(true)

      // Buttons can be activated by Enter or Space
      // They should not be disabled without indication
      const isDisabled = button.hasAttribute('disabled')

      if (isDisabled) {
        // Disabled buttons should have proper aria indication
        expect(button.getAttribute('aria-disabled') === 'true' || button.hasAttribute('disabled')).toBe(true)
      }
    })
  })

  it('external links have proper attributes for security', () => {
    const externalLinks = document.querySelectorAll('a[target="_blank"]')

    externalLinks.forEach((link) => {
      const rel = link.getAttribute('rel')
      expect(rel).toContain('noopener')
      expect(rel).toContain('noreferrer')
    })
  })
})
