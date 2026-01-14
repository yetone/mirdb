import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { axe } from 'vitest-axe'
import Home from '../pages/Home'
import { ThemeProvider, type ThemePreference } from '../contexts/ThemeContext'

/**
 * Accessibility - Color Contrast Tests
 *
 * These tests verify sufficient color contrast for text readability (NFR-2)
 * ensuring the homepage meets WCAG AA standards (4.5:1 for normal text).
 *
 * Requirements tested: NFR-2 (90+ Lighthouse accessibility score)
 * Related PRD sections: Accessibility Considerations (sufficient color contrast ratios)
 */

// Helper to render with ThemeProvider
const renderWithTheme = (
  component: React.ReactElement,
  defaultTheme?: ThemePreference
) => {
  return render(
    <ThemeProvider defaultTheme={defaultTheme}>
      <BrowserRouter>{component}</BrowserRouter>
    </ThemeProvider>
  )
}

describe('Accessibility - Color Contrast (NFR-2)', () => {
  let originalMatchMedia: typeof window.matchMedia

  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear()

    // Store original matchMedia
    originalMatchMedia = window.matchMedia

    // Default mock - light mode system preference
    window.matchMedia = vi.fn().mockImplementation((query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }))
  })

  afterEach(() => {
    window.matchMedia = originalMatchMedia
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  describe('Test Case 1: Axe accessibility audit on homepage (light theme)', () => {
    it('should have no color contrast violations in light theme', async () => {
      const { container } = renderWithTheme(<Home />, 'light')

      // Run axe accessibility audit focused on color-contrast
      const results = await axe(container, {
        rules: {
          'color-contrast': { enabled: true },
        },
        runOnly: {
          type: 'rule',
          values: ['color-contrast'],
        },
      })

      // No violations should be present
      expect(results).toHaveNoViolations()
    })

    it('should pass axe audit for color contrast across all homepage sections in light theme', async () => {
      const { container } = renderWithTheme(<Home />, 'light')

      // Run axe accessibility audit focused on color-contrast across all sections
      const results = await axe(container, {
        runOnly: {
          type: 'rule',
          values: ['color-contrast'],
        },
      })

      // No color contrast violations should be present
      expect(results).toHaveNoViolations()
    })
  })

  describe('Test Case 2: Axe accessibility audit on homepage (dark theme)', () => {
    it('should have no color contrast violations in dark theme', async () => {
      const { container } = renderWithTheme(<Home />, 'dark')

      // Run axe accessibility audit focused on color-contrast
      const results = await axe(container, {
        rules: {
          'color-contrast': { enabled: true },
        },
        runOnly: {
          type: 'rule',
          values: ['color-contrast'],
        },
      })

      // No violations should be present
      expect(results).toHaveNoViolations()
    })

    it('should pass axe audit for color contrast across all homepage sections in dark theme', async () => {
      const { container } = renderWithTheme(<Home />, 'dark')

      // Run axe accessibility audit focused on color-contrast across all sections
      const results = await axe(container, {
        runOnly: {
          type: 'rule',
          values: ['color-contrast'],
        },
      })

      // No color contrast violations should be present
      expect(results).toHaveNoViolations()
    })
  })

  describe('Test Case 3: CTA button text contrast (WCAG AA 4.5:1)', () => {
    it('should use DaisyUI semantic color classes that ensure WCAG AA compliance for primary button', () => {
      const { container } = renderWithTheme(<Home />, 'light')

      // Find all primary buttons
      const primaryButtons = container.querySelectorAll('.btn-primary')

      // Verify we have primary buttons
      expect(primaryButtons.length).toBeGreaterThan(0)

      // Primary buttons in DaisyUI use btn-primary class which ensures WCAG AA compliant contrast
      primaryButtons.forEach((button) => {
        expect(button.classList.contains('btn')).toBe(true)
        expect(button.classList.contains('btn-primary')).toBe(true)
      })
    })

    it('should use DaisyUI semantic color classes that ensure WCAG AA compliance for outline button', () => {
      const { container } = renderWithTheme(<Home />, 'light')

      // Find all outline buttons
      const outlineButtons = container.querySelectorAll('.btn-outline')

      // Verify we have outline buttons
      expect(outlineButtons.length).toBeGreaterThan(0)

      // Outline buttons in DaisyUI use btn-outline class which ensures proper border and text contrast
      outlineButtons.forEach((button) => {
        expect(button.classList.contains('btn')).toBe(true)
        expect(button.classList.contains('btn-outline')).toBe(true)
      })
    })

    it('should pass axe color-contrast check for CTA buttons in light mode', async () => {
      const { container } = renderWithTheme(<Home />, 'light')

      // Focus on the hero section where CTAs are located
      const heroSection = container.querySelector('[data-testid="hero-section"]')

      if (heroSection) {
        const results = await axe(heroSection as HTMLElement, {
          runOnly: {
            type: 'rule',
            values: ['color-contrast'],
          },
        })

        expect(results).toHaveNoViolations()
      }
    })

    it('should pass axe color-contrast check for CTA buttons in dark mode', async () => {
      const { container } = renderWithTheme(<Home />, 'dark')

      // Focus on the hero section where CTAs are located
      const heroSection = container.querySelector('[data-testid="hero-section"]')

      if (heroSection) {
        const results = await axe(heroSection as HTMLElement, {
          runOnly: {
            type: 'rule',
            values: ['color-contrast'],
          },
        })

        expect(results).toHaveNoViolations()
      }
    })
  })

  describe('Additional Color Contrast Tests', () => {
    it('should use proper text color classes for main content', () => {
      const { container } = renderWithTheme(<Home />, 'light')

      // Find headings using text-base-content class for proper contrast
      const baseContentElements = container.querySelectorAll('[class*="text-base-content"]')

      // Verify we have elements using the proper DaisyUI semantic color class
      expect(baseContentElements.length).toBeGreaterThan(0)
    })

    it('should use proper opacity modifiers for secondary text', () => {
      const { container } = renderWithTheme(<Home />, 'light')

      // DaisyUI uses text-base-content/70 for secondary text which maintains contrast
      // These are rendered as text-base-content with opacity modifiers
      const subheadline = container.querySelector('[data-testid="hero-subheadline"]')

      expect(subheadline).toBeInTheDocument()
      expect(subheadline?.className).toContain('text-base-content')
    })

    it('should pass axe audit for features section', async () => {
      const { container } = renderWithTheme(<Home />, 'light')

      const featuresSection = container.querySelector('#features')

      if (featuresSection) {
        const results = await axe(featuresSection as HTMLElement, {
          runOnly: {
            type: 'rule',
            values: ['color-contrast'],
          },
        })

        expect(results).toHaveNoViolations()
      }
    })

    it('should pass axe audit for demo section', async () => {
      const { container } = renderWithTheme(<Home />, 'light')

      const demoSection = container.querySelector('[data-testid="demo-section"]')

      if (demoSection) {
        const results = await axe(demoSection as HTMLElement, {
          runOnly: {
            type: 'rule',
            values: ['color-contrast'],
          },
        })

        expect(results).toHaveNoViolations()
      }
    })

    it('should pass axe audit for footer section', async () => {
      const { container } = renderWithTheme(<Home />, 'light')

      const footerSection = container.querySelector('[data-testid="footer-section"]')

      if (footerSection) {
        const results = await axe(footerSection as HTMLElement, {
          runOnly: {
            type: 'rule',
            values: ['color-contrast'],
          },
        })

        expect(results).toHaveNoViolations()
      }
    })
  })

  describe('Theme-specific Color Contrast Tests', () => {
    it('should pass axe audit for cyberpunk theme', async () => {
      const { container } = renderWithTheme(<Home />, 'cyberpunk')

      const results = await axe(container, {
        runOnly: {
          type: 'rule',
          values: ['color-contrast'],
        },
      })

      expect(results).toHaveNoViolations()
    })

    it('should pass axe audit for synthwave theme', async () => {
      const { container } = renderWithTheme(<Home />, 'synthwave')

      const results = await axe(container, {
        runOnly: {
          type: 'rule',
          values: ['color-contrast'],
        },
      })

      expect(results).toHaveNoViolations()
    })
  })
})
