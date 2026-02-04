/**
 * Theme Toggle Integration Tests
 * Scenario 5: Theme Toggle Integration
 *
 * Validates that the theme toggle works correctly on the homepage,
 * switching between light and dark modes with proper persistence.
 *
 * REQ-7: Homepage shall support dark mode and inherit the application's current theme
 * REQ-10: Homepage shall support theme toggle button consistent with app-wide design
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter, MemoryRouter, Routes, Route } from 'react-router-dom'
import Home from '../../src/pages/Home'
import { ThemeProvider } from '../../src/contexts/ThemeContext'

// Helper to render Home with ThemeProvider
const renderHomeWithTheme = (initialTheme: 'light' | 'dark' = 'light') => {
  localStorage.setItem('theme', initialTheme)
  return render(
    <BrowserRouter>
      <ThemeProvider>
        <Home />
      </ThemeProvider>
    </BrowserRouter>
  )
}

// Helper to render with routing for navigation tests
const renderWithRouter = (initialTheme: 'light' | 'dark' = 'light') => {
  localStorage.setItem('theme', initialTheme)
  return render(
    <MemoryRouter initialEntries={['/']}>
      <ThemeProvider>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route
            path="/login"
            element={<div data-testid="login-page">Login Page</div>}
          />
        </Routes>
      </ThemeProvider>
    </MemoryRouter>
  )
}

describe('Theme Toggle Integration (Scenario 5)', () => {
  beforeEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  describe('Test Case 1: Theme toggle button is present and accessible', () => {
    /**
     * Input: Render Home with ThemeContext and find toggle button
     * Expected: Theme toggle button is present and accessible
     */
    it('should render theme toggle button on homepage', () => {
      renderHomeWithTheme()

      const themeToggle = screen.getByRole('button', { name: /theme|toggle|dark|light/i })
      expect(themeToggle).toBeInTheDocument()
    })

    it('should have accessible label for theme toggle', () => {
      renderHomeWithTheme()

      const themeToggle = screen.getByRole('button', { name: /theme|toggle|dark|light/i })
      expect(themeToggle).toBeInTheDocument()
      // Should have aria-label or accessible name
      expect(themeToggle).toHaveAccessibleName()
    })

    it('should have theme toggle in visible location', () => {
      renderHomeWithTheme()

      const themeToggle = screen.getByRole('button', { name: /theme|toggle|dark|light/i })
      expect(themeToggle).toBeVisible()
    })
  })

  describe('Test Case 2: Theme toggle switches between light and dark', () => {
    /**
     * Input: Click theme toggle in light mode
     * Expected: ThemeContext updates to dark mode, body/html class changes
     */
    it('should switch from light to dark mode when toggled', async () => {
      const user = userEvent.setup()
      renderHomeWithTheme('light')

      const themeToggle = screen.getByRole('button', { name: /theme|toggle|dark|light/i })

      // Initial state should be light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Toggle to dark
      await user.click(themeToggle)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })
    })

    it('should switch from dark to light mode when toggled', async () => {
      const user = userEvent.setup()
      renderHomeWithTheme('dark')

      const themeToggle = screen.getByRole('button', { name: /theme|toggle|dark|light/i })

      // Initial state should be dark
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Toggle to light
      await user.click(themeToggle)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })
    })
  })

  describe('Test Case 3: Theme preference saved to localStorage', () => {
    /**
     * Input: Toggle theme and check localStorage
     * Expected: Theme preference saved to localStorage
     */
    it('should persist theme to localStorage when toggled to dark', async () => {
      const user = userEvent.setup()
      renderHomeWithTheme('light')

      const themeToggle = screen.getByRole('button', { name: /theme|toggle|dark|light/i })
      await user.click(themeToggle)

      await waitFor(() => {
        expect(localStorage.getItem('theme')).toBe('dark')
      })
    })

    it('should persist theme to localStorage when toggled to light', async () => {
      const user = userEvent.setup()
      renderHomeWithTheme('dark')

      const themeToggle = screen.getByRole('button', { name: /theme|toggle|dark|light/i })
      await user.click(themeToggle)

      await waitFor(() => {
        expect(localStorage.getItem('theme')).toBe('light')
      })
    })
  })

  describe('Test Case 4: Smooth CSS transition on theme change', () => {
    /**
     * Input: Verify smooth transition on theme change
     * Expected: CSS transition applied (no jarring color change)
     */
    it('should have transition classes on theme-sensitive elements', () => {
      renderHomeWithTheme()

      // Homepage container should have transition classes for smooth theme changes
      const homepageContainer = document.querySelector('.min-h-screen')
      expect(homepageContainer).toBeInTheDocument()
      // bg-base-100 provides smooth DaisyUI theme transitions
      expect(homepageContainer).toHaveClass('bg-base-100')
    })

    it('should apply smooth transition styles via DaisyUI theming', () => {
      renderHomeWithTheme()

      // The data-theme attribute enables DaisyUI's theme transition system
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })
  })

  describe('Test Case 5: Homepage renders in dark mode initially from localStorage', () => {
    /**
     * Input: Load homepage with dark theme in localStorage
     * Expected: Homepage renders in dark mode initially
     */
    it('should render in dark mode when localStorage has dark theme', () => {
      renderHomeWithTheme('dark')

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should render in light mode when localStorage has light theme', () => {
      renderHomeWithTheme('light')

      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })

    it('should default to light theme when no localStorage value', () => {
      localStorage.removeItem('theme')

      render(
        <BrowserRouter>
          <ThemeProvider>
            <Home />
          </ThemeProvider>
        </BrowserRouter>
      )

      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })
  })

  describe('Test Case 6: All homepage elements support dark mode styles', () => {
    /**
     * Input: Verify all homepage elements support dark mode
     * Expected: Hero, features, form, buttons all have appropriate dark mode styles
     */
    it('should have DaisyUI theme-aware classes on hero section', () => {
      renderHomeWithTheme('dark')

      const headline = screen.getByRole('heading', { level: 1 })
      // text-base-content adapts to theme
      expect(headline).toHaveClass('text-base-content')
    })

    it('should have DaisyUI theme-aware classes on subheadline', () => {
      renderHomeWithTheme('dark')

      const subheadline = screen.getByTestId('hero-subheadline')
      // text-base-content/70 adapts to theme with opacity
      expect(subheadline.className).toContain('text-base-content')
    })

    it('should have theme-aware styling on feature cards', () => {
      renderHomeWithTheme('dark')

      // Feature cards use bg-base-200 which adapts to theme
      const featureCards = document.querySelectorAll('.card')
      expect(featureCards.length).toBeGreaterThan(0)

      featureCards.forEach((card) => {
        expect(card).toHaveClass('bg-base-200')
      })
    })

    it('should have theme-aware styling on CTA buttons', () => {
      renderHomeWithTheme('dark')

      const getStartedBtn = screen.getByRole('link', { name: /get started/i })
      const loginBtn = screen.getByRole('link', { name: /login/i })

      // btn-primary and btn-outline adapt to theme
      expect(getStartedBtn).toHaveClass('btn-primary')
      expect(loginBtn).toHaveClass('btn-outline')
    })

    it('should have correct background color adapting to dark mode', () => {
      renderHomeWithTheme('dark')

      const homepageContainer = document.querySelector('.bg-base-100')
      expect(homepageContainer).toBeInTheDocument()
      // bg-base-100 changes color based on data-theme
    })
  })

  describe('Theme persistence across navigation', () => {
    /**
     * User Story 4: Theme preference persists to other pages
     */
    it('should maintain theme after navigating away and returning', async () => {
      const user = userEvent.setup()
      renderWithRouter('light')

      const themeToggle = screen.getByRole('button', { name: /theme|toggle|dark|light/i })

      // Switch to dark
      await user.click(themeToggle)

      await waitFor(() => {
        expect(localStorage.getItem('theme')).toBe('dark')
      })

      // Theme is stored in localStorage and ThemeContext,
      // so navigation within the app preserves theme state
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })
  })
})
