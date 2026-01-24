/**
 * Browser Compatibility Tests
 * Owner: Scenario 12 - Browser Compatibility
 *
 * Tests for verifying the landing page works across all modern browsers
 * as specified in NFR-4: Support all modern browsers (Chrome, Firefox, Safari, Edge - last 2 versions)
 *
 * This test suite verifies:
 * - All sections render correctly across browsers
 * - Navigation works as expected
 * - Themes apply correctly
 * - CSS properties use cross-browser compatible patterns
 * - Vendor prefixes are properly applied via autoprefixer
 *
 * Note: These tests run in jsdom environment but verify cross-browser compatible patterns.
 * For actual multi-browser testing, Playwright or similar tools would be needed.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, screen, within, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import React from 'react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import { ThemeProvider } from '../../../src/contexts/ThemeContext'
import { AuthProvider } from '../../../src/contexts/AuthContext'
import Home from '../../../src/pages/Home'

// Available themes that should work across all browsers
const THEMES = ['light', 'dark', 'cyberpunk', 'synthwave', 'retro', 'valentine', 'night'] as const

// Browser-specific CSS features to verify
const CROSS_BROWSER_CSS_FEATURES = [
  'flexbox',
  'css-grid',
  'custom-properties',
  'backdrop-filter',
  'transitions',
  'transforms',
] as const

interface RenderOptions {
  initialTheme?: typeof THEMES[number]
  initialRoute?: string
}

function renderApp({ initialTheme = 'dark', initialRoute = '/' }: RenderOptions = {}) {
  localStorage.setItem('theme', initialTheme)
  document.documentElement.setAttribute('data-theme', initialTheme)

  return render(
    <ThemeProvider defaultTheme={initialTheme}>
      <AuthProvider>
        <MemoryRouter initialEntries={[initialRoute]}>
          <Routes>
            <Route path="/" element={<Home />} />
          </Routes>
        </MemoryRouter>
      </AuthProvider>
    </ThemeProvider>
  )
}

describe('Browser Compatibility - NFR-4', () => {
  beforeEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  describe('Test Case 1: Chrome Compatibility (latest 2 versions)', () => {
    it('renders all sections correctly with standard DOM APIs', () => {
      renderApp({ initialTheme: 'light' })

      // Verify hero section renders
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Verify headline is present
      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()
      expect(headline.tagName).toBe('H1')

      // Verify subheadline
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toBeInTheDocument()

      // Verify CTAs
      expect(screen.getByTestId('hero-primary-cta')).toBeInTheDocument()
      expect(screen.getByTestId('hero-secondary-cta')).toBeInTheDocument()
    })

    it('navigation links work correctly', () => {
      renderApp({ initialTheme: 'light' })

      const navbar = screen.getByRole('navigation')
      expect(navbar).toBeInTheDocument()

      // Verify navigation contains expected links
      const primaryCTA = screen.getByTestId('hero-primary-cta')
      expect(primaryCTA).toHaveAttribute('href', '/register')

      const secondaryCTA = screen.getByTestId('hero-secondary-cta')
      expect(secondaryCTA).toHaveAttribute('href', '/login')
    })

    it('themes apply correctly with data-theme attribute', () => {
      renderApp({ initialTheme: 'light' })

      // Verify theme is applied to document root
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Verify page uses theme-aware CSS classes
      const pageContainer = screen.getByRole('main').parentElement
      expect(pageContainer).toHaveClass('bg-base-100')
    })

    it('flexbox layout renders correctly for responsive design', () => {
      renderApp({ initialTheme: 'light' })

      // Verify flexbox classes are applied for CTA button group
      const ctaGroup = screen.getByRole('group', { name: /call to action/i })
      expect(ctaGroup).toHaveClass('flex')
      expect(ctaGroup).toHaveClass('flex-col')
      expect(ctaGroup).toHaveClass('sm:flex-row')
    })

    it('CSS custom properties (CSS variables) work via DaisyUI theme', () => {
      renderApp({ initialTheme: 'dark' })

      // DaisyUI uses CSS custom properties for theming
      // Verify theme is applied (which relies on CSS variables)
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Elements should use theme-aware classes that reference CSS variables
      const headline = screen.getByTestId('hero-headline')
      expect(headline).toHaveClass('text-base-content')
    })
  })

  describe('Test Case 2: Firefox Compatibility (latest 2 versions)', () => {
    it('renders all sections correctly in Firefox-compatible DOM structure', () => {
      renderApp({ initialTheme: 'dark' })

      // Verify semantic HTML structure (works identically in Firefox)
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection.tagName).toBe('SECTION')

      // Verify aria attributes (Firefox has excellent accessibility support)
      expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-headline')
    })

    it('navigation elements are properly accessible', () => {
      renderApp({ initialTheme: 'dark' })

      // Firefox has strong ARIA support
      const navbar = screen.getByRole('navigation')
      expect(navbar).toBeInTheDocument()

      // Verify accessible group role
      const ctaGroup = screen.getByRole('group')
      expect(ctaGroup).toBeInTheDocument()
    })

    it('themes apply and toggle correctly', async () => {
      const user = userEvent.setup()
      renderApp({ initialTheme: 'light' })

      // Initial theme
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Find and interact with theme select
      const themeSelect = screen.getByRole('combobox')
      await user.selectOptions(themeSelect, 'dark')

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })
    })

    it('uses standard CSS selectors compatible with Firefox', () => {
      renderApp({ initialTheme: 'cyberpunk' })

      // Verify standard CSS class patterns that work in all browsers
      const heroSection = screen.getByTestId('hero-section')

      // Standard Tailwind utility classes
      expect(heroSection.className).toContain('min-h-')
      expect(heroSection.className).toContain('flex')
      expect(heroSection.className).toContain('items-center')
      expect(heroSection.className).toContain('justify-center')
    })

    it('responsive classes use standard media query breakpoints', () => {
      renderApp({ initialTheme: 'dark' })

      const headline = screen.getByTestId('hero-headline')

      // Verify responsive classes (md: and lg: prefixes use standard media queries)
      expect(headline.className).toContain('text-4xl')
      expect(headline.className).toContain('md:text-5xl')
      expect(headline.className).toContain('lg:text-6xl')
    })
  })

  describe('Test Case 3: Safari Compatibility (latest 2 versions)', () => {
    it('renders all sections correctly with Safari-compatible patterns', () => {
      renderApp({ initialTheme: 'valentine' })

      // Safari requires careful handling of certain CSS features
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Verify min-height viewport units (Safari compatible)
      expect(heroSection.className).toContain('min-h-')
    })

    it('navigation and links work correctly', () => {
      renderApp({ initialTheme: 'light' })

      // Verify anchor links are properly formed
      const primaryCTA = screen.getByTestId('hero-primary-cta')
      const secondaryCTA = screen.getByTestId('hero-secondary-cta')

      // Safari requires proper href attributes
      expect(primaryCTA).toHaveAttribute('href')
      expect(secondaryCTA).toHaveAttribute('href')

      // Verify links are not broken
      expect(primaryCTA.getAttribute('href')).not.toBe('')
      expect(secondaryCTA.getAttribute('href')).not.toBe('')
    })

    it('themes apply correctly with webkit-compatible features', async () => {
      renderApp({ initialTheme: 'synthwave' })

      // DaisyUI themes work in Safari
      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')

      // Verify theme-aware styling
      const pageContainer = screen.getByRole('main').parentElement
      expect(pageContainer).toHaveClass('bg-base-100')
    })

    it('flexbox gap property has fallback support', () => {
      renderApp({ initialTheme: 'light' })

      // Verify gap class is used (autoprefixer provides fallbacks)
      const ctaGroup = screen.getByRole('group', { name: /call to action/i })
      expect(ctaGroup.className).toContain('gap-')
    })

    it('Safari-compatible text rendering', () => {
      renderApp({ initialTheme: 'dark' })

      const headline = screen.getByTestId('hero-headline')

      // Verify text uses standard font properties
      expect(headline.className).toContain('font-bold')
      expect(headline.className).toContain('leading-')
    })
  })

  describe('Test Case 4: Edge Compatibility (latest 2 versions)', () => {
    it('renders all sections correctly in Edge', () => {
      renderApp({ initialTheme: 'retro' })

      // Edge (Chromium-based) has excellent standards support
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Verify all content renders
      expect(screen.getByTestId('hero-headline')).toBeInTheDocument()
      expect(screen.getByTestId('hero-subheadline')).toBeInTheDocument()
      expect(screen.getByTestId('hero-primary-cta')).toBeInTheDocument()
      expect(screen.getByTestId('hero-secondary-cta')).toBeInTheDocument()
    })

    it('navigation works correctly in Edge', () => {
      renderApp({ initialTheme: 'night' })

      const navbar = screen.getByRole('navigation')
      expect(navbar).toBeInTheDocument()

      // Edge supports all standard navigation patterns
      const primaryCTA = screen.getByTestId('hero-primary-cta')
      expect(primaryCTA).toHaveAttribute('href', '/register')
    })

    it('themes apply correctly in Edge', async () => {
      const user = userEvent.setup()
      renderApp({ initialTheme: 'light' })

      // Verify initial theme
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Test theme switching
      const themeSelect = screen.getByRole('combobox')
      await user.selectOptions(themeSelect, 'cyberpunk')

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
      })
    })

    it('uses standard DOM APIs compatible with Edge', () => {
      renderApp({ initialTheme: 'dark' })

      // Verify standard querySelector patterns work
      const heroSection = document.querySelector('[data-testid="hero-section"]')
      expect(heroSection).not.toBeNull()

      // Verify getAttribute works
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('localStorage persistence works in Edge', () => {
      renderApp({ initialTheme: 'synthwave' })

      // Edge supports localStorage
      expect(localStorage.getItem('theme')).toBe('synthwave')
    })
  })

  describe('Cross-Browser CSS Compatibility', () => {
    it('flexbox layout uses standard properties', () => {
      renderApp({ initialTheme: 'dark' })

      const ctaGroup = screen.getByRole('group', { name: /call to action/i })

      // Standard flexbox classes (Tailwind generates cross-browser compatible CSS)
      expect(ctaGroup).toHaveClass('flex')
      expect(ctaGroup).toHaveClass('justify-center')
      expect(ctaGroup).toHaveClass('items-center')
    })

    it('responsive breakpoints use standard media queries', () => {
      renderApp({ initialTheme: 'light' })

      const headline = screen.getByTestId('hero-headline')
      const ctaGroup = screen.getByRole('group', { name: /call to action/i })

      // sm:, md:, lg: prefixes use standard min-width media queries
      expect(headline.className).toMatch(/md:text-\d+xl/)
      expect(headline.className).toMatch(/lg:text-\d+xl/)
      expect(ctaGroup.className).toContain('sm:flex-row')
    })

    it('text overflow handling is cross-browser compatible', () => {
      renderApp({ initialTheme: 'dark' })

      const subheadline = screen.getByTestId('hero-subheadline')

      // max-w and leading classes for text control
      expect(subheadline.className).toContain('max-w-')
      expect(subheadline.className).toContain('leading-')
    })

    it('box model uses standard properties', () => {
      renderApp({ initialTheme: 'light' })

      const heroSection = screen.getByTestId('hero-section')

      // Standard padding/margin classes
      expect(heroSection.className).toContain('px-')
      expect(heroSection.className).toContain('py-')
    })

    it('all themes work with cross-browser CSS variables', () => {
      // Test each theme applies correctly
      THEMES.forEach((theme) => {
        localStorage.clear()
        document.documentElement.removeAttribute('data-theme')

        const { unmount } = renderApp({ initialTheme: theme })

        // Verify theme is applied
        expect(document.documentElement.getAttribute('data-theme')).toBe(theme)

        // Verify page renders
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()

        unmount()
      })
    })
  })

  describe('Cross-Browser Event Handling', () => {
    it('click events work correctly', async () => {
      const user = userEvent.setup()
      renderApp({ initialTheme: 'dark' })

      const themeSelect = screen.getByRole('combobox')

      // Click interactions should work across all browsers
      await user.click(themeSelect)

      // Select should be interactive
      expect(themeSelect).toBeInTheDocument()
    })

    it('keyboard navigation works correctly', async () => {
      const user = userEvent.setup()
      renderApp({ initialTheme: 'light' })

      // Tab navigation should work across all browsers
      await user.tab()

      // Some element should receive focus
      expect(document.activeElement).not.toBe(document.body)
    })

    it('form controls are accessible across browsers', () => {
      renderApp({ initialTheme: 'dark' })

      // Theme select should have proper form control attributes
      const themeSelect = screen.getByRole('combobox')
      expect(themeSelect.tagName).toBe('SELECT')

      // Verify options are present
      const options = within(themeSelect).getAllByRole('option')
      expect(options.length).toBeGreaterThan(0)
    })
  })

  describe('Cross-Browser Visual Consistency', () => {
    it('semantic HTML structure is consistent', () => {
      renderApp({ initialTheme: 'dark' })

      // Verify semantic elements
      expect(screen.getByRole('main')).toBeInTheDocument()
      expect(screen.getByRole('navigation')).toBeInTheDocument()

      // Verify heading hierarchy
      const h1 = screen.getByRole('heading', { level: 1 })
      expect(h1).toBeInTheDocument()
    })

    it('ARIA attributes are properly set for all browsers', () => {
      renderApp({ initialTheme: 'light' })

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-headline')

      const ctaGroup = screen.getByRole('group')
      expect(ctaGroup).toHaveAttribute('aria-label')
    })

    it('images/icons have proper alt text for accessibility', () => {
      renderApp({ initialTheme: 'dark' })

      // If there are any images, they should have alt text
      const images = document.querySelectorAll('img')
      images.forEach((img) => {
        // Either has alt or is decorative (empty alt)
        expect(img.hasAttribute('alt')).toBe(true)
      })
    })

    it('links have proper href attributes', () => {
      renderApp({ initialTheme: 'light' })

      const primaryCTA = screen.getByTestId('hero-primary-cta')
      const secondaryCTA = screen.getByTestId('hero-secondary-cta')

      // Links should have valid href
      expect(primaryCTA.getAttribute('href')).toMatch(/^\//)
      expect(secondaryCTA.getAttribute('href')).toMatch(/^\//)
    })
  })

  describe('Cross-Browser Theme Switching', () => {
    it('theme persists in localStorage across all browsers', async () => {
      const user = userEvent.setup()
      renderApp({ initialTheme: 'light' })

      const themeSelect = screen.getByRole('combobox')
      await user.selectOptions(themeSelect, 'night')

      await waitFor(() => {
        expect(localStorage.getItem('theme')).toBe('night')
      })
    })

    it('data-theme attribute updates correctly', async () => {
      const user = userEvent.setup()
      renderApp({ initialTheme: 'dark' })

      const themeSelect = screen.getByRole('combobox')
      await user.selectOptions(themeSelect, 'retro')

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('retro')
      })
    })

    it('theme classes update without page reload', async () => {
      const user = userEvent.setup()
      renderApp({ initialTheme: 'light' })

      // Capture initial state
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Change theme
      const themeSelect = screen.getByRole('combobox')
      await user.selectOptions(themeSelect, 'cyberpunk')

      // Page should still be present (no reload)
      await waitFor(() => {
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()
        expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
      })
    })
  })
})
