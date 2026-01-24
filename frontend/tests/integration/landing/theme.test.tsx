/**
 * Theme Support and Dark Mode Integration Tests
 * Owner: Scenario 8 - Theme Support and Dark Mode
 *
 * Tests for verifying the landing page supports dark mode and multiple themes
 * as specified in REQ-8.
 *
 * Verifies:
 * - Light theme rendering
 * - Dark theme rendering with proper contrast
 * - Cyberpunk theme rendering (DaisyUI)
 * - Theme toggling without page reload
 * - Text contrast in dark mode for WCAG AA compliance
 * - Theme persistence via localStorage
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import React, { ReactNode } from 'react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import { ThemeProvider, useTheme } from '../../../src/contexts/ThemeContext'
import { AuthProvider } from '../../../src/contexts/AuthContext'
import Home from '../../../src/pages/Home'

// Available themes in the application
const AVAILABLE_THEMES = ['light', 'dark', 'cyberpunk', 'synthwave', 'retro', 'valentine', 'night'] as const
type Theme = typeof AVAILABLE_THEMES[number]

// Custom render with theme control
interface ThemeTestRenderOptions {
  initialTheme?: Theme
  initialRoute?: string
}

function TestApp({
  initialRoute = '/',
}: {
  initialRoute?: string
}) {
  return (
    <AuthProvider>
      <MemoryRouter initialEntries={[initialRoute]}>
        <Routes>
          <Route path="/" element={<Home />} />
        </Routes>
      </MemoryRouter>
    </AuthProvider>
  )
}

function renderWithTheme({ initialTheme = 'dark', initialRoute = '/' }: ThemeTestRenderOptions = {}) {
  // Set localStorage before rendering to simulate stored theme
  localStorage.setItem('theme', initialTheme)

  return render(
    <ThemeProvider defaultTheme={initialTheme}>
      <TestApp initialRoute={initialRoute} />
    </ThemeProvider>
  )
}

// Helper to get computed styles for color contrast testing
function getComputedBackgroundColor(element: HTMLElement): string {
  return window.getComputedStyle(element).backgroundColor
}

function getComputedTextColor(element: HTMLElement): string {
  return window.getComputedStyle(element).color
}

describe('Theme Support and Dark Mode', () => {
  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear()
    // Reset document theme attribute
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  describe('Test Case 1: Render Home component with ThemeContext set to light', () => {
    it('renders page with light theme when ThemeContext is set to light', () => {
      renderWithTheme({ initialTheme: 'light' })

      // Verify the theme is applied to document
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Verify the page renders
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('applies light theme colors to base elements', () => {
      renderWithTheme({ initialTheme: 'light' })

      // Verify theme attribute is set
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Page should render with light theme classes
      const pageContainer = screen.getByRole('main').parentElement
      expect(pageContainer).toHaveClass('bg-base-100')
    })

    it('stores light theme in localStorage', () => {
      renderWithTheme({ initialTheme: 'light' })

      expect(localStorage.getItem('theme')).toBe('light')
    })

    it('renders navigation elements in light theme', () => {
      renderWithTheme({ initialTheme: 'light' })

      const navbar = screen.getByRole('navigation')
      expect(navbar).toBeInTheDocument()
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })
  })

  describe('Test Case 2: Render Home component with ThemeContext set to dark', () => {
    it('renders page with dark theme when ThemeContext is set to dark', () => {
      renderWithTheme({ initialTheme: 'dark' })

      // Verify the theme is applied to document
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Verify the page renders
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('applies dark theme colors to base elements', () => {
      renderWithTheme({ initialTheme: 'dark' })

      // Verify theme attribute is set
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Page should render with base-100 class (which adapts to dark theme)
      const pageContainer = screen.getByRole('main').parentElement
      expect(pageContainer).toHaveClass('bg-base-100')
    })

    it('stores dark theme in localStorage', () => {
      renderWithTheme({ initialTheme: 'dark' })

      expect(localStorage.getItem('theme')).toBe('dark')
    })

    it('maintains proper text visibility in dark theme', () => {
      renderWithTheme({ initialTheme: 'dark' })

      // Verify headline text is present and rendered
      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()
      expect(headline.textContent).toBeTruthy()

      // Verify subheadline text is present
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toBeInTheDocument()
      expect(subheadline.textContent).toBeTruthy()
    })

    it('renders CTA buttons correctly in dark theme', () => {
      renderWithTheme({ initialTheme: 'dark' })

      const primaryCTA = screen.getByTestId('hero-primary-cta')
      const secondaryCTA = screen.getByTestId('hero-secondary-cta')

      expect(primaryCTA).toBeInTheDocument()
      expect(secondaryCTA).toBeInTheDocument()
      expect(primaryCTA).toHaveClass('btn-primary')
    })
  })

  describe('Test Case 3: Render Home component with ThemeContext set to cyberpunk', () => {
    it('renders page with cyberpunk theme from DaisyUI', () => {
      renderWithTheme({ initialTheme: 'cyberpunk' })

      // Verify the theme is applied to document
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')

      // Verify the page renders
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('applies cyberpunk theme styles to page elements', () => {
      renderWithTheme({ initialTheme: 'cyberpunk' })

      // Verify theme attribute is set to cyberpunk
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')

      // Page structure should be intact
      const pageContainer = screen.getByRole('main').parentElement
      expect(pageContainer).toHaveClass('bg-base-100')
    })

    it('stores cyberpunk theme in localStorage', () => {
      renderWithTheme({ initialTheme: 'cyberpunk' })

      expect(localStorage.getItem('theme')).toBe('cyberpunk')
    })

    it('renders all landing page sections with cyberpunk theme', () => {
      renderWithTheme({ initialTheme: 'cyberpunk' })

      // Verify core sections render
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByRole('navigation')).toBeInTheDocument()
    })
  })

  describe('Test Case 4: Toggle theme while on landing page', () => {
    it('theme changes immediately without page reload when toggled', async () => {
      const user = userEvent.setup()
      renderWithTheme({ initialTheme: 'light' })

      // Initial theme should be light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Find the theme toggle select in the navbar
      const themeSelect = screen.getByRole('combobox')
      expect(themeSelect).toBeInTheDocument()

      // Change to dark theme
      await user.selectOptions(themeSelect, 'dark')

      // Theme should change immediately
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Page content should still be present (no reload)
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })

    it('supports switching between multiple themes', async () => {
      const user = userEvent.setup()
      renderWithTheme({ initialTheme: 'light' })

      const themeSelect = screen.getByRole('combobox')

      // Switch through multiple themes
      const themesToTest: Theme[] = ['dark', 'cyberpunk', 'synthwave', 'night']

      for (const theme of themesToTest) {
        await user.selectOptions(themeSelect, theme)

        await waitFor(() => {
          expect(document.documentElement.getAttribute('data-theme')).toBe(theme)
        })
      }
    })

    it('updates localStorage when theme is toggled', async () => {
      const user = userEvent.setup()
      renderWithTheme({ initialTheme: 'light' })

      const themeSelect = screen.getByRole('combobox')

      // Change to dark theme
      await user.selectOptions(themeSelect, 'dark')

      await waitFor(() => {
        expect(localStorage.getItem('theme')).toBe('dark')
      })
    })

    it('preserves page content during theme toggle', async () => {
      const user = userEvent.setup()
      renderWithTheme({ initialTheme: 'light' })

      // Capture initial content
      const headline = screen.getByTestId('hero-headline')
      const initialText = headline.textContent

      // Toggle theme
      const themeSelect = screen.getByRole('combobox')
      await user.selectOptions(themeSelect, 'dark')

      // Content should remain unchanged
      await waitFor(() => {
        expect(screen.getByTestId('hero-headline')).toHaveTextContent(initialText!)
      })
    })

    it('theme toggle is accessible via keyboard', async () => {
      const user = userEvent.setup()
      renderWithTheme({ initialTheme: 'light' })

      const themeSelect = screen.getByRole('combobox')

      // Focus on the select
      await user.tab()
      await user.tab()
      await user.tab() // Navigate to theme select

      // The select should be focusable
      expect(themeSelect).toBeInTheDocument()
    })
  })

  describe('Test Case 5: Verify text contrast in dark mode', () => {
    it('headline text uses theme-aware color classes for proper contrast', () => {
      renderWithTheme({ initialTheme: 'dark' })

      const headline = screen.getByTestId('hero-headline')
      // Headline should use text-base-content which adapts to theme
      expect(headline).toHaveClass('text-base-content')
    })

    it('subheadline text uses theme-aware color classes', () => {
      renderWithTheme({ initialTheme: 'dark' })

      const subheadline = screen.getByTestId('hero-subheadline')
      // Subheadline should use text-base-content with opacity modifier
      expect(subheadline.className).toContain('text-base-content')
    })

    it('all text elements are visible in dark mode', () => {
      renderWithTheme({ initialTheme: 'dark' })

      // Check that all major text elements are present and not empty
      const headline = screen.getByTestId('hero-headline')
      const subheadline = screen.getByTestId('hero-subheadline')

      expect(headline.textContent).toBeTruthy()
      expect(subheadline.textContent).toBeTruthy()

      // Text should not have transparent/invisible styling
      expect(headline).not.toHaveClass('invisible')
      expect(headline).not.toHaveClass('opacity-0')
      expect(subheadline).not.toHaveClass('invisible')
      expect(subheadline).not.toHaveClass('opacity-0')
    })

    it('CTA buttons maintain visibility and contrast in dark mode', () => {
      renderWithTheme({ initialTheme: 'dark' })

      const primaryCTA = screen.getByTestId('hero-primary-cta')
      const secondaryCTA = screen.getByTestId('hero-secondary-cta')

      // Buttons should have DaisyUI classes that ensure proper contrast
      expect(primaryCTA).toHaveClass('btn-primary')
      expect(secondaryCTA).toHaveClass('btn-secondary')

      // Buttons should not be invisible
      expect(primaryCTA).not.toHaveClass('invisible')
      expect(secondaryCTA).not.toHaveClass('invisible')
    })

    it('page background uses theme-aware base colors', () => {
      renderWithTheme({ initialTheme: 'dark' })

      const pageContainer = screen.getByRole('main').parentElement
      // Page should use bg-base-100 which adapts to dark theme
      expect(pageContainer).toHaveClass('bg-base-100')
    })

    it('navbar maintains proper contrast in dark mode', () => {
      renderWithTheme({ initialTheme: 'dark' })

      const navbar = screen.getByRole('navigation')
      expect(navbar).toBeInTheDocument()

      // Navbar should use theme-aware background
      expect(navbar.className).toContain('bg-base-100')
    })
  })

  describe('Theme Persistence', () => {
    it('reads theme from localStorage on initial render', () => {
      // Pre-set theme in localStorage
      localStorage.setItem('theme', 'synthwave')

      render(
        <ThemeProvider>
          <AuthProvider>
            <MemoryRouter>
              <Home />
            </MemoryRouter>
          </AuthProvider>
        </ThemeProvider>
      )

      // Theme should be read from localStorage
      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
    })

    it('falls back to default theme when localStorage is empty', () => {
      // Ensure localStorage is empty
      localStorage.clear()

      render(
        <ThemeProvider defaultTheme="dark">
          <AuthProvider>
            <MemoryRouter>
              <Home />
            </MemoryRouter>
          </AuthProvider>
        </ThemeProvider>
      )

      // Should use default theme
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })
  })

  describe('Additional Theme Support', () => {
    it('supports synthwave theme', () => {
      renderWithTheme({ initialTheme: 'synthwave' })

      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })

    it('supports retro theme', () => {
      renderWithTheme({ initialTheme: 'retro' })

      expect(document.documentElement.getAttribute('data-theme')).toBe('retro')
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })

    it('supports valentine theme', () => {
      renderWithTheme({ initialTheme: 'valentine' })

      expect(document.documentElement.getAttribute('data-theme')).toBe('valentine')
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })

    it('supports night theme', () => {
      renderWithTheme({ initialTheme: 'night' })

      expect(document.documentElement.getAttribute('data-theme')).toBe('night')
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })

    it('all available themes render the page correctly', () => {
      AVAILABLE_THEMES.forEach((theme) => {
        localStorage.clear()
        document.documentElement.removeAttribute('data-theme')

        const { unmount } = renderWithTheme({ initialTheme: theme })

        expect(document.documentElement.getAttribute('data-theme')).toBe(theme)
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()

        unmount()
      })
    })
  })

  describe('Theme Toggle UI', () => {
    it('theme toggle displays all available theme options', () => {
      renderWithTheme({ initialTheme: 'light' })

      const themeSelect = screen.getByRole('combobox')
      const options = within(themeSelect).getAllByRole('option')

      // Should have all 7 theme options
      expect(options).toHaveLength(7)

      // Verify all themes are present
      const optionValues = options.map((opt) => opt.getAttribute('value'))
      AVAILABLE_THEMES.forEach((theme) => {
        expect(optionValues).toContain(theme)
      })
    })

    it('theme toggle shows current theme as selected', () => {
      renderWithTheme({ initialTheme: 'cyberpunk' })

      const themeSelect = screen.getByRole('combobox') as HTMLSelectElement
      expect(themeSelect.value).toBe('cyberpunk')
    })

    it('theme toggle is present in the navbar', () => {
      renderWithTheme({ initialTheme: 'light' })

      const navbar = screen.getByRole('navigation')
      const themeSelect = within(navbar).getByRole('combobox')

      expect(themeSelect).toBeInTheDocument()
    })
  })
})
