/**
 * Theme Toggle Integration Tests
 * Owner: Scenario 5 - Theme Toggle Support
 *
 * Purpose: Validates dark mode and theme switching functionality on the homepage.
 * Tests theme toggling, persistence, and correct rendering across themes.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor, cleanup } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter } from 'react-router-dom'
import { ThemeProvider, useTheme } from '../../src/contexts/ThemeContext'
import { ThemeToggle } from '../../src/components/ThemeToggle'
import { HeroSection } from '../../src/components/home/HeroSection'
import { FeaturesSection } from '../../src/components/home/FeaturesSection'
import Home from '../../src/pages/Home'

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
    get length() {
      return Object.keys(store).length
    },
    key: vi.fn((index: number) => Object.keys(store)[index] || null),
  }
})()

// Setup localStorage mock before tests
Object.defineProperty(window, 'localStorage', {
  value: localStorageMock,
  writable: true,
})

// Helper to get theme from DOM
function getDocumentTheme(): string | null {
  return document.documentElement.getAttribute('data-theme')
}

// Test wrapper with all providers
function TestWrapper({
  children,
  defaultTheme,
}: {
  children: React.ReactNode
  defaultTheme?: 'light' | 'dark' | 'cyberpunk' | 'synthwave' | 'system'
}) {
  return (
    <ThemeProvider defaultTheme={defaultTheme}>
      <BrowserRouter>{children}</BrowserRouter>
    </ThemeProvider>
  )
}

// Component to display current theme state for testing
function ThemeStateDisplay() {
  const { theme, themePreference, isDarkMode } = useTheme()
  return (
    <div data-testid="theme-state">
      <span data-testid="current-theme">{theme}</span>
      <span data-testid="theme-preference">{themePreference}</span>
      <span data-testid="is-dark-mode">{isDarkMode.toString()}</span>
    </div>
  )
}

describe('Theme Toggle Support', () => {
  beforeEach(() => {
    // Clear localStorage before each test
    localStorageMock.clear()
    vi.clearAllMocks()
    // Reset document theme attribute
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    cleanup()
  })

  // Test Case 1: Render homepage with ThemeToggle - ThemeToggle component is visible in navbar
  describe('Test Case 1: ThemeToggle visibility', () => {
    it('renders ThemeToggle component visible in homepage', () => {
      render(
        <TestWrapper defaultTheme="light">
          <ThemeToggle />
        </TestWrapper>
      )

      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle).toBeInTheDocument()
      expect(themeToggle).toBeVisible()
    })

    it('ThemeToggle has accessible toggle button', () => {
      render(
        <TestWrapper defaultTheme="light">
          <ThemeToggle />
        </TestWrapper>
      )

      const toggleButton = screen.getByTestId('theme-toggle-button')
      expect(toggleButton).toBeInTheDocument()
      expect(toggleButton).toHaveAttribute('aria-label')
    })

    it('ThemeToggle shows moon icon in light mode', () => {
      render(
        <TestWrapper defaultTheme="light">
          <ThemeToggle />
        </TestWrapper>
      )

      const moonIcon = screen.getByTestId('moon-icon')
      expect(moonIcon).toBeInTheDocument()
    })
  })

  // Test Case 2: Click ThemeToggle with light theme active - Theme changes to dark mode
  describe('Test Case 2: Toggle from light to dark', () => {
    it('changes theme from light to dark when toggle button is clicked', async () => {
      render(
        <TestWrapper defaultTheme="light">
          <ThemeToggle />
          <ThemeStateDisplay />
        </TestWrapper>
      )

      // Verify initial light theme
      expect(screen.getByTestId('current-theme')).toHaveTextContent('light')
      expect(screen.getByTestId('is-dark-mode')).toHaveTextContent('false')

      // Click toggle button
      const toggleButton = screen.getByTestId('theme-toggle-button')
      fireEvent.click(toggleButton)

      // Verify theme changed to dark
      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
      })
      expect(screen.getByTestId('is-dark-mode')).toHaveTextContent('true')
    })

    it('updates document data-theme attribute to dark', async () => {
      render(
        <TestWrapper defaultTheme="light">
          <ThemeToggle />
        </TestWrapper>
      )

      const toggleButton = screen.getByTestId('theme-toggle-button')
      fireEvent.click(toggleButton)

      await waitFor(() => {
        expect(getDocumentTheme()).toBe('dark')
      })
    })

    it('shows sun icon after switching to dark mode', async () => {
      render(
        <TestWrapper defaultTheme="light">
          <ThemeToggle />
        </TestWrapper>
      )

      const toggleButton = screen.getByTestId('theme-toggle-button')
      fireEvent.click(toggleButton)

      await waitFor(() => {
        const sunIcon = screen.getByTestId('sun-icon')
        expect(sunIcon).toBeInTheDocument()
      })
    })
  })

  // Test Case 3: Click ThemeToggle with dark theme active - Theme changes to light mode
  describe('Test Case 3: Toggle from dark to light', () => {
    it('changes theme from dark to light when toggle button is clicked', async () => {
      render(
        <TestWrapper defaultTheme="dark">
          <ThemeToggle />
          <ThemeStateDisplay />
        </TestWrapper>
      )

      // Verify initial dark theme
      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
      expect(screen.getByTestId('is-dark-mode')).toHaveTextContent('true')

      // Click toggle button
      const toggleButton = screen.getByTestId('theme-toggle-button')
      fireEvent.click(toggleButton)

      // Verify theme changed to light
      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('light')
      })
      expect(screen.getByTestId('is-dark-mode')).toHaveTextContent('false')
    })

    it('updates document data-theme attribute to light', async () => {
      render(
        <TestWrapper defaultTheme="dark">
          <ThemeToggle />
        </TestWrapper>
      )

      const toggleButton = screen.getByTestId('theme-toggle-button')
      fireEvent.click(toggleButton)

      await waitFor(() => {
        expect(getDocumentTheme()).toBe('light')
      })
    })

    it('shows moon icon after switching to light mode', async () => {
      render(
        <TestWrapper defaultTheme="dark">
          <ThemeToggle />
        </TestWrapper>
      )

      const toggleButton = screen.getByTestId('theme-toggle-button')
      fireEvent.click(toggleButton)

      await waitFor(() => {
        const moonIcon = screen.getByTestId('moon-icon')
        expect(moonIcon).toBeInTheDocument()
      })
    })
  })

  // Test Case 4: Set theme and reload page - Theme preference persists after reload
  describe('Test Case 4: Theme persistence', () => {
    it('saves theme preference to localStorage when theme is changed', async () => {
      render(
        <TestWrapper defaultTheme="light">
          <ThemeToggle />
        </TestWrapper>
      )

      const toggleButton = screen.getByTestId('theme-toggle-button')
      fireEvent.click(toggleButton)

      await waitFor(() => {
        expect(localStorageMock.setItem).toHaveBeenCalledWith('theme-preference', 'dark')
      })
    })

    it('restores theme from localStorage on mount', () => {
      // Set a theme in localStorage before rendering
      localStorageMock.setItem('theme-preference', 'dark')

      render(
        <TestWrapper>
          <ThemeStateDisplay />
        </TestWrapper>
      )

      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
    })

    it('persists custom theme preferences (cyberpunk)', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper defaultTheme="light">
          <ThemeToggle />
          <ThemeStateDisplay />
        </TestWrapper>
      )

      // Open dropdown and select cyberpunk
      const dropdownTrigger = screen.getByTestId('theme-dropdown-trigger')
      await user.click(dropdownTrigger)

      const cyberpunkOption = screen.getByTestId('theme-option-cyberpunk')
      await user.click(cyberpunkOption)

      await waitFor(() => {
        expect(localStorageMock.setItem).toHaveBeenCalledWith('theme-preference', 'cyberpunk')
      })
    })
  })

  // Test Case 5: Render homepage in dark mode - All homepage sections render with dark theme styling
  describe('Test Case 5: Homepage dark mode rendering', () => {
    it('renders homepage with dark theme data attribute', async () => {
      render(
        <TestWrapper defaultTheme="dark">
          <Home />
        </TestWrapper>
      )

      await waitFor(() => {
        expect(getDocumentTheme()).toBe('dark')
      })
    })

    it('applies dark theme styling to homepage container', () => {
      render(
        <TestWrapper defaultTheme="dark">
          <Home />
        </TestWrapper>
      )

      // The homepage should use bg-base-200 which adapts to theme
      const homepage = document.querySelector('.min-h-screen')
      expect(homepage).toHaveClass('bg-base-200')
    })
  })

  // Test Case 6: Render homepage in light mode - All homepage sections render with light theme styling
  describe('Test Case 6: Homepage light mode rendering', () => {
    it('renders homepage with light theme data attribute', async () => {
      render(
        <TestWrapper defaultTheme="light">
          <Home />
        </TestWrapper>
      )

      await waitFor(() => {
        expect(getDocumentTheme()).toBe('light')
      })
    })

    it('homepage sections are visible in light mode', () => {
      render(
        <TestWrapper defaultTheme="light">
          <Home />
        </TestWrapper>
      )

      const homepage = document.querySelector('.min-h-screen')
      expect(homepage).toBeInTheDocument()
    })
  })

  // Test Case 7: Render HeroSection in dark mode - Text colors have appropriate contrast for dark background
  describe('Test Case 7: HeroSection dark mode contrast', () => {
    it('renders HeroSection with appropriate text classes for dark mode', () => {
      render(
        <TestWrapper defaultTheme="dark">
          <HeroSection />
        </TestWrapper>
      )

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Check headline exists and uses theme-aware classes
      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()
    })

    it('HeroSection subheadline uses base-content class for theme adaptation', () => {
      render(
        <TestWrapper defaultTheme="dark">
          <HeroSection />
        </TestWrapper>
      )

      const subheadline = screen.getByTestId('hero-subheadline')
      // The subheadline uses text-base-content/70 which adapts to theme
      expect(subheadline).toHaveClass('text-base-content/70')
    })

    it('HeroSection CTA elements are present in dark mode', () => {
      render(
        <TestWrapper defaultTheme="dark">
          <HeroSection />
        </TestWrapper>
      )

      const ctaButton = screen.getByTestId('hero-cta-primary')
      const loginLink = screen.getByTestId('hero-login-link')

      // Use toBeInTheDocument since framer-motion animations may start with opacity: 0
      expect(ctaButton).toBeInTheDocument()
      expect(loginLink).toBeInTheDocument()
      // Verify correct navigation links
      expect(ctaButton).toHaveAttribute('href', '/register')
      expect(loginLink).toHaveAttribute('href', '/login')
    })
  })

  // Test Case 8: Render FeaturesSection in dark mode - Feature cards are readable with dark theme
  describe('Test Case 8: FeaturesSection dark mode readability', () => {
    it('renders FeaturesSection with feature cards in dark mode', () => {
      render(
        <TestWrapper defaultTheme="dark">
          <FeaturesSection />
        </TestWrapper>
      )

      // Check that feature cards are present
      const featureCards = document.querySelectorAll('article')
      expect(featureCards.length).toBeGreaterThan(0)
    })

    it('feature card titles use theme-aware text classes', () => {
      render(
        <TestWrapper defaultTheme="dark">
          <FeaturesSection />
        </TestWrapper>
      )

      // Feature cards use text-base-content which adapts to theme
      const titles = document.querySelectorAll('h3.text-base-content')
      expect(titles.length).toBeGreaterThan(0)
    })

    it('feature cards have proper background for dark mode', () => {
      render(
        <TestWrapper defaultTheme="dark">
          <FeaturesSection />
        </TestWrapper>
      )

      const cards = document.querySelectorAll('article')
      cards.forEach((card) => {
        // Cards use bg-base-100/70 which adapts to theme
        expect(card.className).toContain('bg-base-100')
      })
    })
  })

  // Test Case 9: Test theme toggle with cyberpunk theme - Homepage renders correctly with cyberpunk theme
  describe('Test Case 9: Cyberpunk theme rendering', () => {
    it('can switch to cyberpunk theme via dropdown', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper defaultTheme="light">
          <ThemeToggle />
          <ThemeStateDisplay />
        </TestWrapper>
      )

      // Open dropdown
      const dropdownTrigger = screen.getByTestId('theme-dropdown-trigger')
      await user.click(dropdownTrigger)

      // Select cyberpunk
      const cyberpunkOption = screen.getByTestId('theme-option-cyberpunk')
      await user.click(cyberpunkOption)

      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('cyberpunk')
      })
    })

    it('applies cyberpunk theme to document', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper defaultTheme="light">
          <ThemeToggle />
        </TestWrapper>
      )

      const dropdownTrigger = screen.getByTestId('theme-dropdown-trigger')
      await user.click(dropdownTrigger)

      const cyberpunkOption = screen.getByTestId('theme-option-cyberpunk')
      await user.click(cyberpunkOption)

      await waitFor(() => {
        expect(getDocumentTheme()).toBe('cyberpunk')
      })
    })

    it('recognizes cyberpunk as a dark theme', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper defaultTheme="light">
          <ThemeToggle />
          <ThemeStateDisplay />
        </TestWrapper>
      )

      const dropdownTrigger = screen.getByTestId('theme-dropdown-trigger')
      await user.click(dropdownTrigger)

      const cyberpunkOption = screen.getByTestId('theme-option-cyberpunk')
      await user.click(cyberpunkOption)

      await waitFor(() => {
        expect(screen.getByTestId('is-dark-mode')).toHaveTextContent('true')
      })
    })

    it('homepage renders without errors in cyberpunk theme', () => {
      render(
        <TestWrapper defaultTheme="cyberpunk">
          <Home />
        </TestWrapper>
      )

      const homepage = document.querySelector('.min-h-screen')
      expect(homepage).toBeInTheDocument()
    })
  })

  // Test Case 10: Test theme toggle with synthwave theme - Homepage renders correctly with synthwave theme
  describe('Test Case 10: Synthwave theme rendering', () => {
    it('can switch to synthwave theme via dropdown', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper defaultTheme="light">
          <ThemeToggle />
          <ThemeStateDisplay />
        </TestWrapper>
      )

      const dropdownTrigger = screen.getByTestId('theme-dropdown-trigger')
      await user.click(dropdownTrigger)

      const synthwaveOption = screen.getByTestId('theme-option-synthwave')
      await user.click(synthwaveOption)

      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('synthwave')
      })
    })

    it('applies synthwave theme to document', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper defaultTheme="light">
          <ThemeToggle />
        </TestWrapper>
      )

      const dropdownTrigger = screen.getByTestId('theme-dropdown-trigger')
      await user.click(dropdownTrigger)

      const synthwaveOption = screen.getByTestId('theme-option-synthwave')
      await user.click(synthwaveOption)

      await waitFor(() => {
        expect(getDocumentTheme()).toBe('synthwave')
      })
    })

    it('recognizes synthwave as a dark theme', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper defaultTheme="light">
          <ThemeToggle />
          <ThemeStateDisplay />
        </TestWrapper>
      )

      const dropdownTrigger = screen.getByTestId('theme-dropdown-trigger')
      await user.click(dropdownTrigger)

      const synthwaveOption = screen.getByTestId('theme-option-synthwave')
      await user.click(synthwaveOption)

      await waitFor(() => {
        expect(screen.getByTestId('is-dark-mode')).toHaveTextContent('true')
      })
    })

    it('homepage renders without errors in synthwave theme', () => {
      render(
        <TestWrapper defaultTheme="synthwave">
          <Home />
        </TestWrapper>
      )

      const homepage = document.querySelector('.min-h-screen')
      expect(homepage).toBeInTheDocument()
    })
  })

  // Additional integration tests
  describe('Theme dropdown functionality', () => {
    it('displays all available theme options', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper defaultTheme="light">
          <ThemeToggle />
        </TestWrapper>
      )

      const dropdownTrigger = screen.getByTestId('theme-dropdown-trigger')
      await user.click(dropdownTrigger)

      expect(screen.getByTestId('theme-option-light')).toBeInTheDocument()
      expect(screen.getByTestId('theme-option-dark')).toBeInTheDocument()
      expect(screen.getByTestId('theme-option-cyberpunk')).toBeInTheDocument()
      expect(screen.getByTestId('theme-option-synthwave')).toBeInTheDocument()
      expect(screen.getByTestId('theme-option-system')).toBeInTheDocument()
    })

    it('shows current theme selection in dropdown', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper defaultTheme="dark">
          <ThemeToggle />
        </TestWrapper>
      )

      const dropdownTrigger = screen.getByTestId('theme-dropdown-trigger')
      expect(dropdownTrigger).toHaveTextContent(/dark/i)
    })

    it('marks active theme option in dropdown', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper defaultTheme="dark">
          <ThemeToggle />
        </TestWrapper>
      )

      const dropdownTrigger = screen.getByTestId('theme-dropdown-trigger')
      await user.click(dropdownTrigger)

      const darkOption = screen.getByTestId('theme-option-dark')
      expect(darkOption).toHaveAttribute('aria-pressed', 'true')
    })
  })

  describe('System theme preference', () => {
    it('can select system theme preference', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper defaultTheme="light">
          <ThemeToggle />
          <ThemeStateDisplay />
        </TestWrapper>
      )

      const dropdownTrigger = screen.getByTestId('theme-dropdown-trigger')
      await user.click(dropdownTrigger)

      const systemOption = screen.getByTestId('theme-option-system')
      await user.click(systemOption)

      await waitFor(() => {
        expect(screen.getByTestId('theme-preference')).toHaveTextContent('system')
      })
    })
  })
})
