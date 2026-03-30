/**
 * Integration tests for homepage theme toggle functionality.
 * Owner: Scenario 7 - Theme Toggle Functionality
 *
 * Tests theme toggle on homepage:
 * - Theme toggle button is present and visible
 * - Clicking theme toggle changes the theme context
 * - Theme changes update document data-theme attribute
 * - Theme preference persists via localStorage
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, within, cleanup } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import React, { useEffect, useState } from 'react'
import Navbar from '@/components/Navbar'
import ThemeToggle from '@/components/ThemeToggle'
import { ThemeProvider, useTheme } from '@/contexts/ThemeContext'
import { AuthProvider } from '@/contexts/AuthContext'
import Home from '@/pages/Home'

/**
 * Custom render helper for theme tests using MemoryRouter
 * Includes all necessary providers
 */
const renderWithProviders = (
  ui: React.ReactElement,
  { initialEntries = ['/'] } = {}
) => {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <ThemeProvider>
        <AuthProvider>
          {ui}
        </AuthProvider>
      </ThemeProvider>
    </MemoryRouter>
  )
}

/**
 * Helper to get the first theme toggle button
 * (Navbar has two - one for desktop, one for mobile)
 */
const getThemeToggle = () => {
  const toggles = screen.getAllByRole('button', { name: /toggle theme/i })
  return toggles[0]
}

/**
 * Component to display current theme for testing
 */
const ThemeDisplay: React.FC = () => {
  const { theme } = useTheme()
  return <div data-testid="theme-display">{theme}</div>
}

/**
 * Component to render with theme display for verification
 */
const NavbarWithThemeDisplay: React.FC = () => {
  return (
    <>
      <Navbar />
      <ThemeDisplay />
    </>
  )
}

describe('Homepage Theme Toggle Functionality', () => {
  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear()
    vi.clearAllMocks()
    // Reset document attribute
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    cleanup()
  })

  describe('Test Case 1: Theme toggle button is present and visible in navigation', () => {
    it('should render theme toggle button in the navbar', () => {
      renderWithProviders(<Navbar />)

      const themeToggle = getThemeToggle()
      expect(themeToggle).toBeInTheDocument()
    })

    it('should have theme toggle visible within the navigation', () => {
      renderWithProviders(<Navbar />)

      const navbar = screen.getByRole('navigation')
      const themeToggles = within(navbar).getAllByRole('button', { name: /toggle theme/i })

      // Should have at least one visible theme toggle
      expect(themeToggles.length).toBeGreaterThan(0)
      expect(themeToggles[0]).toBeInTheDocument()
    })

    it('should render theme toggle with correct aria-label for accessibility', () => {
      renderWithProviders(<Navbar />)

      const themeToggle = getThemeToggle()
      expect(themeToggle).toHaveAttribute('aria-label', 'Toggle theme')
    })

    it('should display theme toggle when rendering full homepage', () => {
      renderWithProviders(
        <>
          <Navbar />
          <Home />
        </>
      )

      const themeToggle = getThemeToggle()
      expect(themeToggle).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Click theme toggle changes theme context value', () => {
    it('should change theme from dark to cyberpunk on first click', async () => {
      const user = userEvent.setup()
      renderWithProviders(<NavbarWithThemeDisplay />)

      // Default theme is 'dark'
      const themeDisplay = screen.getByTestId('theme-display')
      expect(themeDisplay).toHaveTextContent('dark')

      const themeToggle = getThemeToggle()
      await user.click(themeToggle)

      // Theme should cycle to 'cyberpunk' (after dark in the cycle: light -> dark -> cyberpunk -> synthwave)
      expect(themeDisplay).toHaveTextContent('cyberpunk')
    })

    it('should cycle through all themes when clicking toggle repeatedly', async () => {
      const user = userEvent.setup()

      // Start with light theme via mock
      vi.mocked(localStorage.getItem).mockReturnValue('light')

      renderWithProviders(<NavbarWithThemeDisplay />)

      const themeDisplay = screen.getByTestId('theme-display')
      const themeToggle = getThemeToggle()

      // Should start at light (from localStorage mock)
      expect(themeDisplay).toHaveTextContent('light')

      // Click to go to dark
      await user.click(themeToggle)
      expect(themeDisplay).toHaveTextContent('dark')

      // Click to go to cyberpunk
      await user.click(themeToggle)
      expect(themeDisplay).toHaveTextContent('cyberpunk')

      // Click to go to synthwave
      await user.click(themeToggle)
      expect(themeDisplay).toHaveTextContent('synthwave')

      // Click to cycle back to light
      await user.click(themeToggle)
      expect(themeDisplay).toHaveTextContent('light')
    })

    it('should update theme context when toggle is clicked', async () => {
      const user = userEvent.setup()
      renderWithProviders(<NavbarWithThemeDisplay />)

      const themeToggle = getThemeToggle()
      const themeDisplay = screen.getByTestId('theme-display')

      const initialTheme = themeDisplay.textContent
      await user.click(themeToggle)
      const newTheme = themeDisplay.textContent

      expect(newTheme).not.toBe(initialTheme)
    })
  })

  describe('Test Case 3: Click theme toggle updates document data-theme attribute', () => {
    it('should set data-theme attribute on document.documentElement', async () => {
      const user = userEvent.setup()
      // Ensure no saved theme (uses default 'dark')
      vi.mocked(localStorage.getItem).mockReturnValue(null)

      renderWithProviders(<NavbarWithThemeDisplay />)

      // After initial render, data-theme should be set to default (dark)
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      const themeToggle = getThemeToggle()
      await user.click(themeToggle)

      // After clicking, should update to next theme (cyberpunk)
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })

    it('should update data-theme attribute through full theme cycle', async () => {
      const user = userEvent.setup()
      // Mock localStorage to return 'light' initially
      vi.mocked(localStorage.getItem).mockReturnValue('light')

      renderWithProviders(<NavbarWithThemeDisplay />)

      const themeToggle = getThemeToggle()

      // Should start at light (from localStorage)
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Cycle through all themes
      await user.click(themeToggle)
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      await user.click(themeToggle)
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')

      await user.click(themeToggle)
      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')

      await user.click(themeToggle)
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })

    it('should reflect theme change visually via data-theme', async () => {
      const user = userEvent.setup()
      renderWithProviders(<Navbar />)

      const themeToggle = getThemeToggle()

      // Get initial data-theme
      const initialTheme = document.documentElement.getAttribute('data-theme')

      await user.click(themeToggle)

      // data-theme should have changed
      const newTheme = document.documentElement.getAttribute('data-theme')
      expect(newTheme).not.toBe(initialTheme)
      expect(newTheme).toBeTruthy()
    })
  })

  describe('Test Case 4: Theme preference persists via localStorage', () => {
    it('should save theme preference to localStorage when toggled', async () => {
      const user = userEvent.setup()
      // Start with no saved theme (default dark)
      vi.mocked(localStorage.getItem).mockReturnValue(null)

      renderWithProviders(<NavbarWithThemeDisplay />)

      const themeToggle = getThemeToggle()
      const themeDisplay = screen.getByTestId('theme-display')

      // Initial theme should be dark (default)
      expect(themeDisplay).toHaveTextContent('dark')

      await user.click(themeToggle)

      // After clicking, should save to localStorage
      expect(localStorage.setItem).toHaveBeenCalledWith('theme', 'cyberpunk')
    })

    it('should restore theme from localStorage on component mount', () => {
      // Pre-set localStorage to 'dark'
      localStorage.setItem('theme', 'dark')
      // Mock getItem to return 'dark'
      vi.mocked(localStorage.getItem).mockReturnValue('dark')

      renderWithProviders(<NavbarWithThemeDisplay />)

      const themeDisplay = screen.getByTestId('theme-display')
      expect(themeDisplay).toHaveTextContent('dark')
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should restore synthwave theme from localStorage', () => {
      // Mock getItem to return 'synthwave'
      vi.mocked(localStorage.getItem).mockReturnValue('synthwave')

      renderWithProviders(<NavbarWithThemeDisplay />)

      const themeDisplay = screen.getByTestId('theme-display')
      expect(themeDisplay).toHaveTextContent('synthwave')
      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
    })

    it('should restore cyberpunk theme from localStorage', () => {
      // Mock getItem to return 'cyberpunk'
      vi.mocked(localStorage.getItem).mockReturnValue('cyberpunk')

      renderWithProviders(<NavbarWithThemeDisplay />)

      const themeDisplay = screen.getByTestId('theme-display')
      expect(themeDisplay).toHaveTextContent('cyberpunk')
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })

    it('should persist theme preference across multiple toggles', async () => {
      const user = userEvent.setup()
      localStorage.setItem('theme', 'light')
      vi.mocked(localStorage.getItem).mockReturnValue('light')

      renderWithProviders(<NavbarWithThemeDisplay />)

      const themeToggle = getThemeToggle()

      // Toggle multiple times
      await user.click(themeToggle) // light -> dark
      expect(localStorage.setItem).toHaveBeenLastCalledWith('theme', 'dark')

      await user.click(themeToggle) // dark -> cyberpunk
      expect(localStorage.setItem).toHaveBeenLastCalledWith('theme', 'cyberpunk')

      await user.click(themeToggle) // cyberpunk -> synthwave
      expect(localStorage.setItem).toHaveBeenLastCalledWith('theme', 'synthwave')
    })

    it('should fall back to default dark theme if localStorage has invalid value', () => {
      // Mock getItem to return invalid theme
      vi.mocked(localStorage.getItem).mockReturnValue('invalid-theme')

      renderWithProviders(<NavbarWithThemeDisplay />)

      const themeDisplay = screen.getByTestId('theme-display')
      // Should fall back to default 'dark' theme
      expect(themeDisplay).toHaveTextContent('dark')
    })

    it('should use default dark theme if localStorage is empty', () => {
      // Mock getItem to return null
      vi.mocked(localStorage.getItem).mockReturnValue(null)

      renderWithProviders(<NavbarWithThemeDisplay />)

      const themeDisplay = screen.getByTestId('theme-display')
      expect(themeDisplay).toHaveTextContent('dark')
    })
  })

  describe('Theme toggle icon display', () => {
    it('should display appropriate icon based on current theme', () => {
      renderWithProviders(<Navbar />)

      const themeToggle = getThemeToggle()
      // Button should contain an icon (svg element)
      const icon = themeToggle.querySelector('svg')
      expect(icon).toBeInTheDocument()
    })
  })

  describe('Theme integration with homepage components', () => {
    it('should allow theme toggle from homepage with navbar', async () => {
      const user = userEvent.setup()
      renderWithProviders(
        <>
          <Navbar />
          <Home />
          <ThemeDisplay />
        </>
      )

      const themeToggle = getThemeToggle()
      const themeDisplay = screen.getByTestId('theme-display')

      expect(themeDisplay).toHaveTextContent('dark')

      await user.click(themeToggle)

      expect(themeDisplay).toHaveTextContent('cyberpunk')
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })
  })
})

/**
 * Scenario 17: Design Consistency with Application Theme System
 *
 * Tests design token usage and theme variant support:
 * - Components use existing patterns (GlassMorphismCard, FuturisticButton)
 * - All theme variants (light, dark, cyberpunk, synthwave) render correctly
 * - Design tokens from DaisyUI/Tailwind are consistently applied
 */
describe('Design Consistency with Application Theme System (Scenario 17)', () => {
  beforeEach(() => {
    localStorage.clear()
    vi.clearAllMocks()
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    cleanup()
  })

  describe('Test Case 1: Homepage CSS classes use existing component patterns', () => {
    it('should render homepage with correct structural elements', () => {
      renderWithProviders(<Home />)

      // Homepage should have core sections
      const homepage = screen.getByTestId('homepage')
      expect(homepage).toBeInTheDocument()
      expect(homepage).toHaveClass('min-h-screen')

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      const footer = screen.getByTestId('footer')
      expect(footer).toBeInTheDocument()
    })

    it('should use FuturisticButton for CTA buttons in hero section', () => {
      renderWithProviders(<Home />)

      // Check that buttons have DaisyUI btn classes from FuturisticButton
      const shortenButton = screen.getByTestId('shorten-button')
      expect(shortenButton).toHaveClass('btn')
      expect(shortenButton).toHaveClass('btn-primary')
      expect(shortenButton).toHaveClass('btn-lg')
    })

    it('should use GlassMorphismCard styling in feature cards', () => {
      renderWithProviders(<Home />)

      // Feature cards should exist within GlassMorphismCard wrapper
      const featureCards = screen.getAllByTestId(/^feature-card-/)
      expect(featureCards.length).toBe(4)

      // Each feature card should be wrapped with glassmorphism styling
      featureCards.forEach((card) => {
        // GlassMorphismCard applies backdrop-blur-lg, bg-base-100/30, etc.
        const glassWrapper = card.querySelector('.backdrop-blur-lg')
        expect(glassWrapper).toBeInTheDocument()
      })
    })

    it('should use DaisyUI semantic color classes for text', () => {
      renderWithProviders(<Home />)

      // Check hero subheadline uses base-content semantic color
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toHaveClass('text-base-content/70')

      // Check footer uses semantic color classes
      const footer = screen.getByTestId('footer')
      expect(footer).toHaveClass('text-base-content/60')
      expect(footer).toHaveClass('border-base-300')
    })

    it('should use DaisyUI input classes for URL input', () => {
      renderWithProviders(<Home />)

      const urlInput = screen.getByTestId('url-input')
      expect(urlInput).toHaveClass('input')
      expect(urlInput).toHaveClass('input-bordered')
      expect(urlInput).toHaveClass('input-lg')
    })
  })

  describe('Test Case 2: Light theme renders correctly', () => {
    beforeEach(() => {
      vi.mocked(localStorage.getItem).mockReturnValue('light')
    })

    it('should set data-theme to light when light theme is active', () => {
      renderWithProviders(<Home />)

      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })

    it('should render homepage elements correctly in light theme', () => {
      renderWithProviders(<Home />)

      // Verify homepage renders without errors
      expect(screen.getByTestId('homepage')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()
    })

    it('should display hero headline correctly in light theme', () => {
      renderWithProviders(<Home />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveTextContent('Shorten URLs, Track Insights')
    })

    it('should display all feature cards in light theme', () => {
      renderWithProviders(<Home />)

      const featureCards = screen.getAllByTestId(/^feature-card-/)
      expect(featureCards.length).toBe(4)

      // Verify all feature titles are visible
      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
      expect(screen.getByText('Analytics Dashboard')).toBeInTheDocument()
      expect(screen.getByText('Click Tracking')).toBeInTheDocument()
      expect(screen.getByText('Secure & Reliable')).toBeInTheDocument()
    })
  })

  describe('Test Case 3: Dark theme renders correctly with proper contrast', () => {
    beforeEach(() => {
      vi.mocked(localStorage.getItem).mockReturnValue('dark')
    })

    it('should set data-theme to dark when dark theme is active', () => {
      renderWithProviders(<Home />)

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should render homepage elements correctly in dark theme', () => {
      renderWithProviders(<Home />)

      expect(screen.getByTestId('homepage')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()
    })

    it('should display URL input with proper styling in dark theme', () => {
      renderWithProviders(<Home />)

      const urlInput = screen.getByTestId('url-input')
      expect(urlInput).toBeInTheDocument()
      // Input should have bordered style for visibility
      expect(urlInput).toHaveClass('input-bordered')
    })

    it('should display feature cards with glassmorphism effect in dark theme', () => {
      renderWithProviders(<Home />)

      const featureCards = screen.getAllByTestId(/^feature-card-/)
      featureCards.forEach((card) => {
        const glassWrapper = card.querySelector('.backdrop-blur-lg')
        expect(glassWrapper).toBeInTheDocument()
        // bg-base-100/30 provides semi-transparent background for contrast
        expect(glassWrapper).toHaveClass('bg-base-100/30')
      })
    })

    it('should maintain text readability with semantic color classes in dark theme', () => {
      renderWithProviders(<Home />)

      // Semantic classes adapt to theme automatically
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toHaveClass('text-base-content/70')
    })
  })

  describe('Test Case 4: Cyberpunk theme styling applied correctly', () => {
    beforeEach(() => {
      vi.mocked(localStorage.getItem).mockReturnValue('cyberpunk')
    })

    it('should set data-theme to cyberpunk when cyberpunk theme is active', () => {
      renderWithProviders(<Home />)

      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })

    it('should render homepage elements correctly in cyberpunk theme', () => {
      renderWithProviders(<Home />)

      expect(screen.getByTestId('homepage')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()
    })

    it('should display primary buttons with cyberpunk theme colors', () => {
      renderWithProviders(<Home />)

      const shortenButton = screen.getByTestId('shorten-button')
      // btn-primary class will use cyberpunk's primary color
      expect(shortenButton).toHaveClass('btn-primary')
    })

    it('should display feature cards with theme-aware styling', () => {
      renderWithProviders(<Home />)

      const featureCards = screen.getAllByTestId(/^feature-card-/)
      expect(featureCards.length).toBe(4)

      // Each card should still use glassmorphism which adapts to theme
      featureCards.forEach((card) => {
        const glassWrapper = card.querySelector('.border-base-content\\/10')
        expect(glassWrapper).toBeInTheDocument()
      })
    })

    it('should maintain all interactive elements in cyberpunk theme', () => {
      renderWithProviders(<Home />)

      const urlInput = screen.getByTestId('url-input')
      expect(urlInput).toBeInTheDocument()
      expect(urlInput).not.toBeDisabled()

      const shortenButton = screen.getByTestId('shorten-button')
      expect(shortenButton).toBeInTheDocument()
    })
  })

  describe('Test Case 5: Synthwave theme styling applied correctly', () => {
    beforeEach(() => {
      vi.mocked(localStorage.getItem).mockReturnValue('synthwave')
    })

    it('should set data-theme to synthwave when synthwave theme is active', () => {
      renderWithProviders(<Home />)

      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
    })

    it('should render homepage elements correctly in synthwave theme', () => {
      renderWithProviders(<Home />)

      expect(screen.getByTestId('homepage')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()
    })

    it('should display headline with correct typography in synthwave theme', () => {
      renderWithProviders(<Home />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toHaveClass('font-bold')
      expect(headline).toHaveClass('text-4xl')
    })

    it('should display secondary CTA buttons with synthwave styling', () => {
      renderWithProviders(<Home />)

      // Get Started button uses secondary variant
      const getStartedButton = screen.getByRole('button', { name: /get started/i })
      expect(getStartedButton).toHaveClass('btn-secondary')
    })

    it('should display footer with theme-aware border in synthwave theme', () => {
      renderWithProviders(<Home />)

      const footer = screen.getByTestId('footer')
      // border-base-300 adapts to synthwave's color scheme
      expect(footer).toHaveClass('border-base-300')
    })

    it('should display all feature cards in synthwave theme', () => {
      renderWithProviders(<Home />)

      // All four features should be visible
      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
      expect(screen.getByText('Analytics Dashboard')).toBeInTheDocument()
      expect(screen.getByText('Click Tracking')).toBeInTheDocument()
      expect(screen.getByText('Secure & Reliable')).toBeInTheDocument()
    })
  })

  describe('Design token consistency across all themes', () => {
    const themes = ['light', 'dark', 'cyberpunk', 'synthwave'] as const

    themes.forEach((themeName) => {
      describe(`${themeName} theme`, () => {
        beforeEach(() => {
          vi.mocked(localStorage.getItem).mockReturnValue(themeName)
        })

        it(`should apply ${themeName} theme to document`, () => {
          renderWithProviders(<Home />)
          expect(document.documentElement.getAttribute('data-theme')).toBe(themeName)
        })

        it(`should render all homepage sections in ${themeName} theme`, () => {
          renderWithProviders(<Home />)

          expect(screen.getByTestId('homepage')).toBeInTheDocument()
          expect(screen.getByTestId('hero-section')).toBeInTheDocument()
          expect(screen.getByTestId('features-section')).toBeInTheDocument()
          expect(screen.getByTestId('footer')).toBeInTheDocument()
        })

        it(`should have functional URL input in ${themeName} theme`, () => {
          renderWithProviders(<Home />)

          const urlInput = screen.getByTestId('url-input')
          expect(urlInput).toBeInTheDocument()
          expect(urlInput).toHaveAttribute('type', 'url')
        })

        it(`should display shorten button in ${themeName} theme`, () => {
          renderWithProviders(<Home />)

          const button = screen.getByTestId('shorten-button')
          expect(button).toBeInTheDocument()
          expect(button).toHaveTextContent('Shorten URL')
        })
      })
    })
  })
})
