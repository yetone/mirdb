import { describe, it, expect, beforeEach, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { ThemeProvider } from '../../contexts/ThemeContext'
import { ThemeToggle } from '../ThemeToggle'
import Home from '../../pages/Home'

// Helper to render components with ThemeProvider
function renderWithTheme(ui: React.ReactElement) {
  return render(
    <ThemeProvider>
      <BrowserRouter>
        {ui}
      </BrowserRouter>
    </ThemeProvider>
  )
}

describe('Theme Toggle Functionality', () => {
  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear()
    // Reset data-theme attribute
    document.documentElement.removeAttribute('data-theme')
  })

  /**
   * Test Case 1: ThemeToggle component is present and visible on homepage
   * Input: Render homepage and query for ThemeToggle component
   * Expected: ThemeToggle component is present and visible
   */
  describe('Test Case 1: ThemeToggle component visibility', () => {
    it('should render ThemeToggle component on homepage', () => {
      renderWithTheme(<Home />)

      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle).toBeInTheDocument()
      expect(themeToggle).toBeVisible()
    })

    it('should render ThemeToggle as a button with correct aria-label', () => {
      renderWithTheme(<Home />)

      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle.tagName).toBe('BUTTON')
      expect(themeToggle).toHaveAttribute('aria-label')
    })

    it('should display moon icon in dark mode (default)', () => {
      renderWithTheme(<Home />)

      const moonIcon = screen.getByTestId('moon-icon')
      expect(moonIcon).toBeInTheDocument()
    })
  })

  /**
   * Test Case 2: Click theme toggle when in light mode
   * Input: Click theme toggle when in light mode
   * Expected: Theme changes to dark mode immediately (data-theme attribute updates)
   */
  describe('Test Case 2: Theme toggle from light to dark mode', () => {
    it('should change theme to dark mode when clicking toggle in light mode', () => {
      // Set initial theme to light
      localStorage.setItem('theme', 'light')

      renderWithTheme(<ThemeToggle />)

      // Verify initial state is light mode
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      expect(screen.getByTestId('sun-icon')).toBeInTheDocument()

      // Click toggle
      const themeToggle = screen.getByTestId('theme-toggle')
      fireEvent.click(themeToggle)

      // Verify theme changed to dark
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      expect(screen.getByTestId('moon-icon')).toBeInTheDocument()
    })
  })

  /**
   * Test Case 3: Click theme toggle when in dark mode
   * Input: Click theme toggle when in dark mode
   * Expected: Theme changes to light mode immediately
   */
  describe('Test Case 3: Theme toggle from dark to light mode', () => {
    it('should change theme to light mode when clicking toggle in dark mode', () => {
      // Set initial theme to dark
      localStorage.setItem('theme', 'dark')

      renderWithTheme(<ThemeToggle />)

      // Verify initial state is dark mode
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      expect(screen.getByTestId('moon-icon')).toBeInTheDocument()

      // Click toggle
      const themeToggle = screen.getByTestId('theme-toggle')
      fireEvent.click(themeToggle)

      // Verify theme changed to light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      expect(screen.getByTestId('sun-icon')).toBeInTheDocument()
    })
  })

  /**
   * Test Case 4: Theme persistence in localStorage
   * Input: Toggle theme and reload page
   * Expected: Theme preference is persisted in localStorage and restored on reload
   */
  describe('Test Case 4: Theme persistence', () => {
    it('should persist theme preference to localStorage', () => {
      // Start with dark mode (default)
      renderWithTheme(<ThemeToggle />)

      // Click toggle to switch to light mode
      const themeToggle = screen.getByTestId('theme-toggle')
      fireEvent.click(themeToggle)

      // Verify localStorage was updated
      expect(localStorage.getItem('theme')).toBe('light')
    })

    it('should restore theme from localStorage on mount', () => {
      // Pre-set theme in localStorage
      localStorage.setItem('theme', 'light')

      renderWithTheme(<ThemeToggle />)

      // Verify theme is restored from localStorage
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      expect(screen.getByTestId('sun-icon')).toBeInTheDocument()
    })

    it('should default to dark theme when no preference is stored', () => {
      // Ensure localStorage is empty
      localStorage.clear()

      renderWithTheme(<ThemeToggle />)

      // Verify default dark theme
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      expect(screen.getByTestId('moon-icon')).toBeInTheDocument()
    })
  })

  /**
   * Test Case 5: ThemeContext integration on homepage
   * Input: Verify ThemeContext integration on homepage
   * Expected: Homepage uses ThemeContext for theme state management
   */
  describe('Test Case 5: ThemeContext integration', () => {
    it('should use ThemeContext for theme state management on homepage', () => {
      renderWithTheme(<Home />)

      // Verify ThemeToggle is present (which uses ThemeContext)
      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle).toBeInTheDocument()

      // Click toggle and verify state change propagates
      fireEvent.click(themeToggle)

      // Verify data-theme attribute changed (managed by ThemeContext)
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })

    it('should throw error when ThemeToggle is used outside ThemeProvider', () => {
      // Suppress console.error for this test
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

      expect(() => {
        render(
          <BrowserRouter>
            <ThemeToggle />
          </BrowserRouter>
        )
      }).toThrow('useTheme must be used within a ThemeProvider')

      consoleSpy.mockRestore()
    })
  })

  /**
   * Additional tests for complete coverage
   */
  describe('Additional theme toggle tests', () => {
    it('should update aria-label based on current theme', () => {
      localStorage.setItem('theme', 'dark')
      renderWithTheme(<ThemeToggle />)

      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle).toHaveAttribute('aria-label', 'Switch to light mode')

      fireEvent.click(themeToggle)

      expect(themeToggle).toHaveAttribute('aria-label', 'Switch to dark mode')
    })

    it('should toggle theme multiple times correctly', () => {
      renderWithTheme(<ThemeToggle />)
      const themeToggle = screen.getByTestId('theme-toggle')

      // Default: dark
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Toggle to light
      fireEvent.click(themeToggle)
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Toggle back to dark
      fireEvent.click(themeToggle)
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Toggle to light again
      fireEvent.click(themeToggle)
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })
  })
})
