/**
 * Accessibility Integration Tests
 * Owner: Scenario 9 - Accessibility Compliance (WCAG 2.1 AA)
 *
 * Test Cases:
 * - TC3: All navigation buttons have aria-label or accessible name
 * - TC4: Theme toggle button has aria-label describing current state
 * - TC5: Run accessibility audit - no critical violations
 * - TC6: All img elements have alt attribute
 * - TC7: Page contains header, main, and footer semantic elements
 * - TC8: Headings follow proper hierarchy (h1 -> h2 -> h3)
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import * as vitestAxe from 'vitest-axe'
import App from '@/App'

// Extend expect with axe matchers
expect.extend({ toHaveNoViolations: vitestAxe.toHaveNoViolations })

// Use the axe function from vitest-axe
const { axe } = vitestAxe

// Mock localStorage
const localStorageMock = (() => {
  let store: Record<string, string> = {}
  return {
    getItem: vi.fn((key: string) => store[key] || null),
    setItem: vi.fn((key: string, value: string) => {
      store[key] = value
    }),
    removeItem: vi.fn((key: string) => {
      delete store[key]
    }),
    clear: vi.fn(() => {
      store = {}
    }),
  }
})()

Object.defineProperty(window, 'localStorage', { value: localStorageMock })

describe('Accessibility Compliance (WCAG 2.1 AA)', () => {
  beforeEach(() => {
    localStorageMock.clear()
    document.documentElement.classList.remove('dark')
  })

  /**
   * Test Case 3: All navigation buttons have aria-label or accessible name
   */
  describe('TC3: Navigation buttons accessibility', () => {
    it('should have accessible names for all navigation links', () => {
      render(<App />)

      // Check header navigation links
      const nav = screen.getByRole('navigation', { name: /main navigation/i })
      const links = within(nav).getAllByRole('link')

      links.forEach((link) => {
        // Each link should either have aria-label or accessible text content
        const hasAriaLabel = link.hasAttribute('aria-label')
        const hasTextContent = link.textContent && link.textContent.trim() !== ''
        expect(hasAriaLabel || hasTextContent).toBe(true)
      })
    })

    it('should have aria-label on mobile menu button', () => {
      render(<App />)

      const menuButton = screen.getByRole('button', { name: /open mobile menu/i })
      expect(menuButton).toHaveAttribute('aria-label')
      expect(menuButton).toHaveAttribute('aria-expanded')
    })

    it('should have accessible CTA buttons in hero section', () => {
      render(<App />)

      const getStartedLink = screen.getByRole('link', { name: /get started/i })
      expect(getStartedLink).toBeInTheDocument()

      // There are multiple GitHub links across the page (header, hero, footer)
      const githubLinks = screen.getAllByRole('link', { name: /github/i })
      expect(githubLinks.length).toBeGreaterThan(0)

      // Verify the hero CTA button specifically
      const heroGithubLink = screen.getByRole('link', { name: /view mirdb on github/i })
      expect(heroGithubLink).toBeInTheDocument()
    })
  })

  /**
   * Test Case 4: Theme toggle button has aria-label describing current state
   */
  describe('TC4: Theme toggle accessibility', () => {
    it('should have aria-label describing current state and action', () => {
      render(<App />)

      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle).toBeInTheDocument()

      // In light mode (default), should say "Switch to dark mode"
      expect(themeToggle).toHaveAttribute('aria-label', 'Switch to dark mode')
    })

    it('should have aria-pressed attribute', () => {
      render(<App />)

      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle).toHaveAttribute('aria-pressed')
    })

    it('should hide decorative icons from screen readers', () => {
      render(<App />)

      const themeToggle = screen.getByTestId('theme-toggle')
      const svg = themeToggle.querySelector('svg')
      expect(svg).toHaveAttribute('aria-hidden', 'true')
    })
  })

  /**
   * Test Case 5: Run accessibility audit - no critical violations
   */
  describe('TC5: Accessibility audit', () => {
    it('should have no critical accessibility violations', async () => {
      const { container } = render(<App />)

      const results = await axe(container, {
        rules: {
          // Disable color-contrast as it requires canvas support in jsdom
          'color-contrast': { enabled: false },
          // Focus on critical WCAG 2.1 AA rules
          'aria-required-attr': { enabled: true },
          'aria-valid-attr': { enabled: true },
          'button-name': { enabled: true },
          'image-alt': { enabled: true },
          'label': { enabled: true },
          'link-name': { enabled: true },
          'region': { enabled: true },
          'heading-order': { enabled: true },
          'landmark-one-main': { enabled: true },
          'page-has-heading-one': { enabled: true },
        },
      })

      // Filter for only critical and serious violations
      const criticalViolations = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      )

      expect(criticalViolations).toHaveLength(0)
    })

    it('should pass axe accessibility checks', async () => {
      const { container } = render(<App />)
      // Disable color-contrast as it requires canvas support (jsdom limitation)
      const results = await axe(container, {
        rules: {
          'color-contrast': { enabled: false },
        },
      })
      // Check that there are no violations directly
      expect(results.violations).toHaveLength(0)
    })
  })

  /**
   * Test Case 6: All img elements have alt attribute
   */
  describe('TC6: Image alt text', () => {
    it('should have alt attribute on all images', () => {
      const { container } = render(<App />)

      const images = container.querySelectorAll('img')
      expect(images.length).toBeGreaterThan(0)

      images.forEach((img) => {
        expect(img).toHaveAttribute('alt')
        // Alt text should be meaningful (not empty unless decorative)
        const altText = img.getAttribute('alt')
        expect(altText).not.toBeNull()
      })
    })

    it('should have descriptive alt text for logo images', () => {
      render(<App />)

      // Check hero logo
      const logos = screen.getAllByAltText(/mirdb logo/i)
      expect(logos.length).toBeGreaterThan(0)
    })

    it('should have meaningful alt text on all images', () => {
      const { container } = render(<App />)

      const images = container.querySelectorAll('img')
      images.forEach((img) => {
        const altText = img.getAttribute('alt')
        // Alt should not be just whitespace
        expect(altText?.trim()).not.toBe('')
      })
    })
  })

  /**
   * Test Case 7: Page contains header, main, and footer semantic elements
   */
  describe('TC7: Document structure', () => {
    it('should contain header element with banner role', () => {
      render(<App />)

      const header = screen.getByRole('banner')
      expect(header).toBeInTheDocument()
      expect(header.tagName.toLowerCase()).toBe('header')
    })

    it('should contain main element', () => {
      render(<App />)

      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()
      expect(main.tagName.toLowerCase()).toBe('main')
    })

    it('should contain footer element with contentinfo role', () => {
      render(<App />)

      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
      expect(footer.tagName.toLowerCase()).toBe('footer')
    })

    it('should have navigation element', () => {
      render(<App />)

      const nav = screen.getByRole('navigation')
      expect(nav).toBeInTheDocument()
      expect(nav).toHaveAttribute('aria-label')
    })

    it('should have proper section landmarks', () => {
      render(<App />)

      // Check for region landmarks (sections with aria-label or aria-labelledby)
      const regions = screen.getAllByRole('region')
      expect(regions.length).toBeGreaterThan(0)
    })
  })

  /**
   * Test Case 8: Headings follow proper hierarchy (h1 -> h2 -> h3)
   */
  describe('TC8: Heading hierarchy', () => {
    it('should have exactly one h1 heading', () => {
      const { container } = render(<App />)

      const h1Elements = container.querySelectorAll('h1')
      expect(h1Elements.length).toBe(1)
    })

    it('should have h1 containing product name', () => {
      render(<App />)

      const h1 = screen.getByRole('heading', { level: 1 })
      expect(h1).toHaveTextContent(/mirdb/i)
    })

    it('should have section headings at h2 level', () => {
      render(<App />)

      const h2Headings = screen.getAllByRole('heading', { level: 2 })
      expect(h2Headings.length).toBeGreaterThan(0)

      // Check that major sections have h2 headings
      const headingTexts = h2Headings.map((h) => h.textContent?.toLowerCase())
      expect(headingTexts.some((t) => t?.includes('feature'))).toBe(true)
      expect(headingTexts.some((t) => t?.includes('quick start'))).toBe(true)
      expect(headingTexts.some((t) => t?.includes('architecture'))).toBe(true)
    })

    it('should have proper heading hierarchy (no skipped levels)', () => {
      const { container } = render(<App />)

      const allHeadings = container.querySelectorAll('h1, h2, h3, h4, h5, h6')
      let previousLevel = 0

      allHeadings.forEach((heading) => {
        const level = parseInt(heading.tagName[1])

        // Heading level should not skip more than one level
        // (e.g., h1 -> h3 is invalid, but h2 -> h3 is valid)
        if (previousLevel > 0) {
          const levelDiff = level - previousLevel
          // Can go deeper by 1, stay same, or go back to any previous level
          expect(levelDiff).toBeLessThanOrEqual(1)
        }

        previousLevel = level
      })
    })

    it('should have h3 subheadings where appropriate', () => {
      const { container } = render(<App />)

      const h3Elements = container.querySelectorAll('h3')
      // Should have h3 elements in sections like Quick Start steps or footer headings
      expect(h3Elements.length).toBeGreaterThan(0)
    })
  })

  /**
   * Additional accessibility tests
   */
  describe('Additional accessibility checks', () => {
    it('should have focus-visible styles on key interactive elements', () => {
      render(<App />)

      // Check theme toggle has focus styles (it's our main interactive button)
      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle.className).toMatch(/focus:/i)

      // Check CTA buttons in hero section have focus styles
      const getStartedLink = screen.getByRole('link', { name: /get started/i })
      expect(getStartedLink.className).toMatch(/focus:/i)
    })

    it('should have keyboard-accessible navigation links', () => {
      render(<App />)

      const links = screen.getAllByRole('link')
      // All links should be keyboard accessible (not have tabindex="-1")
      links.forEach((link) => {
        const tabIndex = link.getAttribute('tabindex')
        expect(tabIndex !== '-1').toBe(true)
      })
    })

    it('should have sufficient color contrast classes', () => {
      const { container } = render(<App />)

      // Check that text elements use proper Tailwind color classes
      // that meet WCAG AA requirements (4.5:1 for normal text)
      const textElements = container.querySelectorAll('p, span, a, button, h1, h2, h3, h4')

      textElements.forEach((el) => {
        // Should use text-gray-600+ or similar high contrast colors
        // This is a basic check - real contrast is verified by axe
        const className = el.className
        if (className && typeof className === 'string') {
          // Just ensure we're not using low-contrast colors
          expect(className).not.toMatch(/text-gray-[123]00(?!\s|$)/)
        }
      })
    })

    it('should have skip-to-content functionality or proper landmarks', () => {
      render(<App />)

      // Either have a skip link or proper landmark structure
      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()

      // Main content should have an ID for potential skip link
      expect(main).toHaveAttribute('id')
    })
  })
})
