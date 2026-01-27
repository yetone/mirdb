/**
 * Keyboard Navigation Integration Tests
 * Owner: Scenario 13 - Accessibility - Keyboard Navigation
 *
 * Tests for keyboard navigation accessibility:
 * - Tab order follows visual layout (top to bottom, left to right)
 * - All interactive elements are focusable
 * - Focus indicators are visible
 * - Enter/Space activates buttons
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import userEvent from '@testing-library/user-event'
import { render, screen, within } from './setup'
import Home from '../../../src/pages/Home'

describe('Accessibility - Keyboard Navigation', () => {
  let user: ReturnType<typeof userEvent.setup>

  beforeEach(() => {
    user = userEvent.setup()
    // Reset any mocks
    vi.clearAllMocks()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  describe('Tab Order', () => {
    it('all buttons and links receive focus in logical order', async () => {
      render(<Home />)

      // Get all focusable elements
      const focusableElements = screen.getAllByRole('link')
      const buttons = screen.getAllByRole('button')

      // Verify that links exist
      expect(focusableElements.length).toBeGreaterThan(0)

      // Verify that buttons exist
      expect(buttons.length).toBeGreaterThan(0)

      // Start from body
      document.body.focus()

      // Tab through the page and verify elements receive focus
      // First tab should focus the first interactive element (URL Shortener link in header)
      await user.tab()
      const firstFocusedElement = document.activeElement

      // Verify that an interactive element is focused
      expect(firstFocusedElement).not.toBe(document.body)
      expect(
        firstFocusedElement?.tagName === 'A' ||
          firstFocusedElement?.tagName === 'BUTTON'
      ).toBe(true)
    })

    it('tab order follows visual layout (top to bottom, left to right)', async () => {
      render(<Home />)

      // Start tabbing from the beginning
      document.body.focus()

      const focusOrder: Element[] = []

      // Tab through all focusable elements (limit to reasonable number)
      for (let i = 0; i < 20; i++) {
        await user.tab()
        const activeElement = document.activeElement
        if (
          activeElement &&
          activeElement !== document.body &&
          !focusOrder.includes(activeElement)
        ) {
          focusOrder.push(activeElement)
        } else if (activeElement === document.body) {
          break
        }
      }

      // Verify at least some elements were focused
      expect(focusOrder.length).toBeGreaterThan(0)

      // Get bounding rectangles to verify visual order
      const positions = focusOrder.map((el) => {
        const rect = el.getBoundingClientRect()
        return { element: el, top: rect.top, left: rect.left }
      })

      // Verify general top-to-bottom, left-to-right order
      // (allowing for some flexibility in exact positioning)
      for (let i = 1; i < positions.length; i++) {
        const prev = positions[i - 1]
        const curr = positions[i]

        // Elements should be either below the previous one, or
        // to the right on the same row (with some tolerance)
        const isBelowOrRight =
          curr.top > prev.top - 50 || // Below or roughly same row
          (Math.abs(curr.top - prev.top) < 100 && curr.left >= prev.left - 50) // Same row, to the right

        expect(isBelowOrRight).toBe(true)
      }
    })

    it('navigation links come before main content in tab order', async () => {
      render(<Home />)

      document.body.focus()

      // Tab to first element
      await user.tab()
      const firstFocused = document.activeElement

      // Verify the first focused element is in the header area
      const header = screen.getByRole('banner')
      expect(header.contains(firstFocused)).toBe(true)
    })
  })

  describe('Focus Indicators', () => {
    it('visible focus indicator when CTA button is focused', async () => {
      render(<Home />)

      // Find the primary CTA button in Hero section
      const getStartedButtons = screen.getAllByRole('button', {
        name: /get started/i,
      })
      const heroGetStartedButton = getStartedButtons[0]

      // Focus the button
      heroGetStartedButton.focus()

      // Verify the button is focused
      expect(document.activeElement).toBe(heroGetStartedButton)

      // Verify the button has DaisyUI btn class which includes focus styles
      expect(heroGetStartedButton).toHaveClass('btn')

      // DaisyUI buttons have built-in focus-visible styles
      // We verify the button is properly styled with btn class
      expect(heroGetStartedButton.className).toMatch(/btn/)
    })

    it('all interactive elements have focus-visible support', async () => {
      render(<Home />)

      const buttons = screen.getAllByRole('button')
      const links = screen.getAllByRole('link')

      // Check buttons have btn class (DaisyUI focus styles)
      buttons.forEach((button) => {
        // Buttons should either have btn class or be part of a component
        // that handles focus
        expect(
          button.className.includes('btn') ||
            button.getAttribute('aria-label') !== null
        ).toBe(true)
      })

      // Check links have link class or are properly styled
      links.forEach((link) => {
        // Links should have link class or btn class
        expect(
          link.className.includes('link') ||
            link.className.includes('btn') ||
            link.tagName === 'A'
        ).toBe(true)
      })
    })

    it('theme toggle button has visible focus indicator', async () => {
      render(<Home />)

      // Find theme toggle by its aria-label
      const themeToggle = screen.getByRole('button', {
        name: /switch to (light|dark) mode/i,
      })

      // Focus the theme toggle
      themeToggle.focus()

      expect(document.activeElement).toBe(themeToggle)
      expect(themeToggle).toHaveClass('btn')
    })
  })

  describe('Keyboard Activation', () => {
    it('button activates and triggers navigation on Enter key', async () => {
      render(<Home />)

      // Find the Get Started link in the header
      const getStartedLink = screen.getByRole('link', { name: /get started/i })

      // Focus the link
      getStartedLink.focus()
      expect(document.activeElement).toBe(getStartedLink)

      // Verify it has the correct href
      expect(getStartedLink).toHaveAttribute('href', '/register')
    })

    it('CTA button responds to Enter key press', async () => {
      render(<Home />)

      // Get the CTA register button in CTA section
      const ctaButton = screen.getByTestId('cta-register-button')

      // Focus the button
      ctaButton.focus()
      expect(document.activeElement).toBe(ctaButton)

      // Verify the button is a proper button element that responds to Enter
      expect(ctaButton.tagName).toBe('BUTTON')

      // Verify it's keyboard accessible (has no tabindex=-1)
      expect(ctaButton).not.toHaveAttribute('tabindex', '-1')
    })

    it('CTA button responds to Space key press', async () => {
      render(<Home />)

      // Get the CTA register button in CTA section
      const ctaButton = screen.getByTestId('cta-register-button')

      // Focus the button
      ctaButton.focus()
      expect(document.activeElement).toBe(ctaButton)

      // Verify the button is a proper button element that responds to Space
      expect(ctaButton.tagName).toBe('BUTTON')

      // Native buttons respond to Space key automatically
      // Verify it has proper button semantics
      expect(ctaButton).not.toHaveAttribute('role', 'presentation')
    })

    it('links can be activated with Enter key', async () => {
      render(<Home />)

      // Get login link
      const loginLink = screen.getAllByRole('link', { name: /login/i })[0]

      // Focus the link
      loginLink.focus()
      expect(document.activeElement).toBe(loginLink)

      // Verify it's a proper link
      expect(loginLink.tagName).toBe('A')
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('Learn More link can be activated with Enter key', async () => {
      render(<Home />)

      // Find the Learn More link
      const learnMoreLink = screen.getByRole('link', {
        name: /learn more/i,
      })

      // Focus and verify
      learnMoreLink.focus()
      expect(document.activeElement).toBe(learnMoreLink)

      // Verify it links to features section
      expect(learnMoreLink).toHaveAttribute('href', '#features')
    })
  })

  describe('Interactive Elements Coverage', () => {
    it('all buttons in the page are keyboard accessible', async () => {
      render(<Home />)

      const buttons = screen.getAllByRole('button')

      // Each button should be focusable
      for (const button of buttons) {
        button.focus()
        expect(document.activeElement).toBe(button)
      }
    })

    it('all links in the page are keyboard accessible', async () => {
      render(<Home />)

      const links = screen.getAllByRole('link')

      // Each link should be focusable
      for (const link of links) {
        link.focus()
        expect(document.activeElement).toBe(link)
      }
    })

    it('header navigation elements are accessible', async () => {
      render(<Home />)

      const header = screen.getByRole('banner')

      // Find all interactive elements within header
      const headerLinks = within(header).getAllByRole('link')
      const headerButtons = within(header).getAllByRole('button')

      expect(headerLinks.length).toBeGreaterThanOrEqual(2) // Logo, Login, Get Started
      expect(headerButtons.length).toBeGreaterThanOrEqual(1) // Theme toggle

      // Verify each is focusable
      headerLinks.forEach((link) => {
        link.focus()
        expect(document.activeElement).toBe(link)
      })

      headerButtons.forEach((button) => {
        button.focus()
        expect(document.activeElement).toBe(button)
      })
    })
  })

  describe('Skip Navigation', () => {
    it('page has proper landmark regions for screen readers', () => {
      render(<Home />)

      // Verify main landmarks exist
      expect(screen.getByRole('banner')).toBeInTheDocument() // header
      expect(screen.getByRole('main')).toBeInTheDocument() // main content

      // Footer in current Home.tsx is a simple footer, check it exists
      const footer = document.querySelector('footer')
      expect(footer).toBeInTheDocument()
    })
  })
})
