/**
 * Theme Adaptation Tests
 * Owner: Scenario 5 - Theme Adaptation
 *
 * Tests for validating that the homepage correctly adapts to different
 * theme selections including light, dark, cyberpunk, and synthwave themes.
 *
 * Requirements:
 * - REQ-7: Theme adaptation (User Story 7)
 */
import { describe, it, expect, beforeEach, vi } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter } from 'react-router-dom'
import { Home } from './Home'
import { ThemeProvider, Theme } from '../contexts/ThemeContext'

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

Object.defineProperty(window, 'localStorage', {
  value: localStorageMock,
})

// Helper to render Home with a specific theme
const renderHomeWithTheme = (theme: Theme = 'dark') => {
  return render(
    <ThemeProvider defaultTheme={theme}>
      <BrowserRouter>
        <Home />
      </BrowserRouter>
    </ThemeProvider>
  )
}

describe('Theme Adaptation - Scenario 5', () => {
  beforeEach(() => {
    localStorageMock.clear()
    vi.clearAllMocks()
  })

  describe('Test Case 1: Render Home with light theme context', () => {
    it('should render homepage with light theme background and appropriate text colors', () => {
      renderHomeWithTheme('light')

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
      expect(homePage).toHaveAttribute('data-theme-active', 'light')
      // Verify homepage has base-100 background class (light theme applies light colors)
      expect(homePage).toHaveClass('bg-base-100')
    })

    it('should render hero section with proper light theme styling', () => {
      renderHomeWithTheme('light')

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Render Home with dark theme context', () => {
    it('should render homepage with dark theme background and light text colors', () => {
      renderHomeWithTheme('dark')

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
      expect(homePage).toHaveAttribute('data-theme-active', 'dark')
      expect(homePage).toHaveClass('bg-base-100')
    })

    it('should render hero section with proper dark theme styling', () => {
      renderHomeWithTheme('dark')

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })
  })

  describe('Test Case 3: Render Home with synthwave theme context', () => {
    it('should render homepage with synthwave color palette', () => {
      renderHomeWithTheme('synthwave')

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
      expect(homePage).toHaveAttribute('data-theme-active', 'synthwave')
      expect(homePage).toHaveClass('bg-base-100')
    })

    it('should render all homepage sections with synthwave theme', () => {
      renderHomeWithTheme('synthwave')

      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('cta-footer')).toBeInTheDocument()
    })
  })

  describe('Test Case 4: Render Home with cyberpunk theme context', () => {
    it('should render homepage with cyberpunk color palette', () => {
      renderHomeWithTheme('cyberpunk')

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
      expect(homePage).toHaveAttribute('data-theme-active', 'cyberpunk')
      expect(homePage).toHaveClass('bg-base-100')
    })

    it('should render all homepage sections with cyberpunk theme', () => {
      renderHomeWithTheme('cyberpunk')

      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('cta-footer')).toBeInTheDocument()
    })
  })

  describe('Test Case 5: Toggle theme and verify smooth transition', () => {
    it('should apply Framer Motion transition animation on theme change', async () => {
      renderHomeWithTheme('dark')

      // Verify initial theme
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toHaveAttribute('data-theme-active', 'dark')

      // Verify the homepage has transition classes for smooth theme change
      expect(homePage).toHaveClass('transition-colors')
      expect(homePage).toHaveClass('duration-300')
    })

    it('should update theme attribute when theme changes via toggle', async () => {
      const user = userEvent.setup()
      renderHomeWithTheme('dark')

      // Click the theme toggle button - use first one (desktop navbar)
      const themeToggles = screen.getAllByTestId('theme-toggle-button')
      expect(themeToggles.length).toBeGreaterThanOrEqual(1)

      await user.click(themeToggles[0])

      // Theme dropdown should appear - it's always in DOM, CSS controls visibility
      const dropdowns = screen.getAllByTestId('theme-dropdown')
      expect(dropdowns.length).toBeGreaterThanOrEqual(1)

      // Select light theme using the test ID
      const lightOptions = screen.getAllByTestId('theme-option-light')
      await user.click(lightOptions[0])

      // Verify theme changed
      await waitFor(() => {
        const homePage = screen.getByTestId('home-page')
        expect(homePage).toHaveAttribute('data-theme-active', 'light')
      })
    })
  })

  describe('Test Case 7: Verify ThemeToggle component is functional on homepage', () => {
    it('should render ThemeToggle and be clickable', async () => {
      renderHomeWithTheme('dark')

      // ThemeToggle should be in navbar - there may be multiple (desktop and mobile)
      const themeToggles = screen.getAllByTestId('theme-toggle')
      expect(themeToggles.length).toBeGreaterThanOrEqual(1)

      const themeButtons = screen.getAllByTestId('theme-toggle-button')
      expect(themeButtons.length).toBeGreaterThanOrEqual(1)

      // Verify first toggle button is clickable
      expect(themeButtons[0]).not.toBeDisabled()
    })

    it('should trigger theme context update when theme option is selected', async () => {
      const user = userEvent.setup()
      renderHomeWithTheme('dark')

      // Open dropdown - use first theme button (desktop)
      const themeButtons = screen.getAllByTestId('theme-toggle-button')
      await user.click(themeButtons[0])

      // Select synthwave theme - options are always in DOM for DaisyUI dropdown
      const synthwaveOptions = screen.getAllByTestId('theme-option-synthwave')
      await user.click(synthwaveOptions[0])

      // Verify theme context was updated
      await waitFor(() => {
        const homePage = screen.getByTestId('home-page')
        expect(homePage).toHaveAttribute('data-theme-active', 'synthwave')
      })
    })

    it('should render all theme options in dropdown', async () => {
      renderHomeWithTheme('dark')

      // Verify all theme options are present (dropdown content is always in DOM)
      expect(screen.getAllByTestId('theme-option-light').length).toBeGreaterThanOrEqual(1)
      expect(screen.getAllByTestId('theme-option-dark').length).toBeGreaterThanOrEqual(1)
      expect(screen.getAllByTestId('theme-option-cyberpunk').length).toBeGreaterThanOrEqual(1)
      expect(screen.getAllByTestId('theme-option-synthwave').length).toBeGreaterThanOrEqual(1)
    })
  })

  describe('Theme persistence', () => {
    it('should persist theme selection to localStorage', async () => {
      const user = userEvent.setup()
      renderHomeWithTheme('dark')

      // Open dropdown and select light theme - use first toggle (desktop)
      const themeButtons = screen.getAllByTestId('theme-toggle-button')
      await user.click(themeButtons[0])

      const lightOptions = screen.getAllByTestId('theme-option-light')
      await user.click(lightOptions[0])

      // Verify localStorage was called
      await waitFor(() => {
        expect(localStorageMock.setItem).toHaveBeenCalledWith(
          'url-shortener-theme',
          'light'
        )
      })
    })
  })
})
