import { describe, it, expect, beforeEach, vi } from 'vitest'
import { render, screen, act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { ThemeProvider, useTheme } from '../src/contexts/ThemeContext'
import Home from '../src/pages/Home'

// Test wrapper component for accessing theme context
function ThemeTestComponent() {
  const { theme, setTheme, toggleTheme } = useTheme()
  return (
    <div data-testid="theme-test">
      <span data-testid="current-theme">{theme}</span>
      <button data-testid="set-dark" onClick={() => setTheme('dark')}>
        Set Dark
      </button>
      <button data-testid="set-light" onClick={() => setTheme('light')}>
        Set Light
      </button>
      <button data-testid="toggle-theme" onClick={toggleTheme}>
        Toggle Theme
      </button>
    </div>
  )
}

// Helper function to render with ThemeProvider and MemoryRouter
function renderWithTheme(component: React.ReactElement, defaultTheme: 'light' | 'dark' = 'light') {
  return render(
    <ThemeProvider defaultTheme={defaultTheme}>
      <MemoryRouter>{component}</MemoryRouter>
    </ThemeProvider>
  )
}

describe('Theme Support - Dark Mode', () => {
  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear()
    // Reset document theme attribute
    document.documentElement.removeAttribute('data-theme')
  })

  // Test Case 1: Render Homepage with ThemeContext set to 'dark'
  describe('Test Case 1: Homepage renders with dark theme', () => {
    it('should render Homepage with dark theme styles applied when ThemeContext is set to dark', () => {
      renderWithTheme(<Home />, 'dark')

      // Verify the homepage renders
      const homepage = screen.getByTestId('homepage')
      expect(homepage).toBeInTheDocument()

      // Verify the hero section renders
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Verify the document has dark theme applied
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should apply dark theme class to document when theme context is dark', () => {
      renderWithTheme(<Home />, 'dark')

      // DaisyUI themes use data-theme attribute on html element
      const dataTheme = document.documentElement.getAttribute('data-theme')
      expect(dataTheme).toBe('dark')
    })

    it('should render all homepage sections with dark theme context', () => {
      renderWithTheme(<Home />, 'dark')

      // Verify all main sections are present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()

      // Theme should be dark
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })
  })

  // Test Case 2: Query background color in dark mode
  describe('Test Case 2: Background uses dark color scheme', () => {
    it('should have data-theme attribute set to dark (not light)', () => {
      renderWithTheme(<Home />, 'dark')

      const dataTheme = document.documentElement.getAttribute('data-theme')
      expect(dataTheme).toBe('dark')
      expect(dataTheme).not.toBe('light')
    })

    it('should apply dark theme which provides dark background colors via DaisyUI', () => {
      renderWithTheme(<Home />, 'dark')

      // The DaisyUI dark theme sets CSS custom properties for dark colors
      // We verify the theme attribute is set, which DaisyUI uses to apply dark styles
      const theme = document.documentElement.getAttribute('data-theme')
      expect(theme).toBe('dark')

      // The hero section should exist and have bg-base-100/bg-base-200 classes
      // which DaisyUI styles differently based on theme
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('bg-gradient-to-b')
    })

    it('should not use light theme when dark is specified', () => {
      renderWithTheme(<Home />, 'dark')

      const dataTheme = document.documentElement.getAttribute('data-theme')
      // Verify we're not in light mode
      expect(dataTheme).not.toBe('light')
      // Verify we're in dark mode
      expect(dataTheme).toBe('dark')
    })
  })

  // Test Case 3: Toggle theme from dark to light
  describe('Test Case 3: Theme toggle functionality', () => {
    it('should re-render Homepage with light theme after toggle without page reload', async () => {
      const user = userEvent.setup()

      render(
        <ThemeProvider defaultTheme="dark">
          <MemoryRouter>
            <Home />
            <ThemeTestComponent />
          </MemoryRouter>
        </ThemeProvider>
      )

      // Initially should be dark
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')

      // Toggle theme
      await user.click(screen.getByTestId('toggle-theme'))

      // Should now be light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      expect(screen.getByTestId('current-theme')).toHaveTextContent('light')

      // Homepage should still be rendered (no page reload)
      expect(screen.getByTestId('homepage')).toBeInTheDocument()
    })

    it('should update theme dynamically using setTheme without page reload', async () => {
      const user = userEvent.setup()

      render(
        <ThemeProvider defaultTheme="dark">
          <MemoryRouter>
            <Home />
            <ThemeTestComponent />
          </MemoryRouter>
        </ThemeProvider>
      )

      // Initially dark
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Set to light explicitly
      await user.click(screen.getByTestId('set-light'))

      // Should be light now
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      expect(screen.getByTestId('current-theme')).toHaveTextContent('light')

      // Homepage components should all remain rendered
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()
    })

    it('should toggle back to dark from light', async () => {
      const user = userEvent.setup()

      render(
        <ThemeProvider defaultTheme="light">
          <MemoryRouter>
            <Home />
            <ThemeTestComponent />
          </MemoryRouter>
        </ThemeProvider>
      )

      // Initially light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Toggle to dark
      await user.click(screen.getByTestId('toggle-theme'))

      // Should be dark
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')

      // Homepage should still be there
      expect(screen.getByTestId('homepage')).toBeInTheDocument()
    })

    it('should persist theme change to localStorage', async () => {
      const user = userEvent.setup()

      render(
        <ThemeProvider defaultTheme="dark">
          <MemoryRouter>
            <ThemeTestComponent />
          </MemoryRouter>
        </ThemeProvider>
      )

      // Toggle theme
      await user.click(screen.getByTestId('toggle-theme'))

      // Check localStorage was updated
      expect(localStorage.getItem('theme')).toBe('light')

      // Toggle back
      await user.click(screen.getByTestId('toggle-theme'))
      expect(localStorage.getItem('theme')).toBe('dark')
    })
  })

  // Additional ThemeContext tests
  describe('ThemeContext functionality', () => {
    it('should load theme from localStorage on initial render', () => {
      localStorage.setItem('theme', 'dark')

      render(
        <ThemeProvider>
          <MemoryRouter>
            <ThemeTestComponent />
          </MemoryRouter>
        </ThemeProvider>
      )

      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should use defaultTheme when localStorage is empty', () => {
      render(
        <ThemeProvider defaultTheme="dark">
          <MemoryRouter>
            <ThemeTestComponent />
          </MemoryRouter>
        </ThemeProvider>
      )

      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
    })

    it('should throw error when useTheme is used outside ThemeProvider', () => {
      // Suppress console.error for this test
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

      expect(() => {
        render(<ThemeTestComponent />)
      }).toThrow('useTheme must be used within a ThemeProvider')

      consoleSpy.mockRestore()
    })
  })
})
