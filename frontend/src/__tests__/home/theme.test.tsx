/**
 * Theme Toggle Tests
 * Owner: Scenario 5 - Theme Toggle Functionality
 *
 * Test coverage:
 * - Default theme applied
 * - Theme toggle changes theme
 * - Theme changes reflected in components
 * - Theme persistence
 */
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { screen, fireEvent, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { renderWithProviders } from './test-utils'
import Home from '../../pages/Home'
import ThemeToggle from '../../components/ThemeToggle'
import { ThemeProvider, useTheme } from '../../contexts/ThemeContext'
import { BrowserRouter } from 'react-router-dom'
import { AuthProvider } from '../../contexts/AuthContext'

// Helper to get the data-theme attribute from the document element
const getDataTheme = () => document.documentElement.getAttribute('data-theme')

// Mock localStorage
const localStorageMock = (() => {
  let store: Record<string, string> = {}
  return {
    getItem: vi.fn((key: string) => store[key] || null),
    setItem: vi.fn((key: string, value: string) => {
      store[key] = value
    }),
    clear: vi.fn(() => {
      store = {}
    }),
    removeItem: vi.fn((key: string) => {
      delete store[key]
    }),
  }
})()

Object.defineProperty(window, 'localStorage', { value: localStorageMock })

describe('Theme Toggle Functionality', () => {
  beforeEach(() => {
    localStorageMock.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  describe('Test Case 1: Render homepage with ThemeContext', () => {
    it('renders page with default theme applied', async () => {
      renderWithProviders(<Home />)

      // Wait for theme to be applied
      await waitFor(() => {
        expect(getDataTheme()).toBe('dark')
      })

      // Verify the page has rendered
      expect(screen.getByRole('navigation')).toBeInTheDocument()
    })

    it('applies default dark theme when no localStorage value exists', async () => {
      renderWithProviders(<Home />)

      await waitFor(() => {
        expect(getDataTheme()).toBe('dark')
      })
    })

    it('applies theme from localStorage if previously saved', async () => {
      localStorageMock.setItem('theme', 'light')

      renderWithProviders(<Home />)

      await waitFor(() => {
        expect(getDataTheme()).toBe('light')
      })
    })
  })

  describe('Test Case 2: Click theme toggle button', () => {
    it('calls toggleTheme function when theme toggle is clicked', async () => {
      const user = userEvent.setup()
      renderWithProviders(<Home />)

      // Find the theme toggle button by its aria-label
      const toggleButton = screen.getByRole('button', {
        name: /switch to (light|dark) mode/i,
      })

      expect(toggleButton).toBeInTheDocument()

      // Initial theme is dark
      await waitFor(() => {
        expect(getDataTheme()).toBe('dark')
      })

      // Click the toggle
      await user.click(toggleButton)

      // Theme should have changed
      await waitFor(() => {
        expect(getDataTheme()).toBe('light')
      })
    })

    it('toggles theme from dark to light on click', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />)

      // Initial state is dark
      await waitFor(() => {
        expect(getDataTheme()).toBe('dark')
      })

      const toggleButton = screen.getByRole('button', {
        name: /switch to light mode/i,
      })
      await user.click(toggleButton)

      await waitFor(() => {
        expect(getDataTheme()).toBe('light')
      })
    })

    it('toggles theme from light to dark on click', async () => {
      localStorageMock.setItem('theme', 'light')

      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />)

      await waitFor(() => {
        expect(getDataTheme()).toBe('light')
      })

      const toggleButton = screen.getByRole('button', {
        name: /switch to dark mode/i,
      })
      await user.click(toggleButton)

      await waitFor(() => {
        expect(getDataTheme()).toBe('dark')
      })
    })
  })

  describe('Test Case 3: Check data-theme attribute after toggle', () => {
    it('updates document body data-theme attribute when toggled', async () => {
      const user = userEvent.setup()
      renderWithProviders(<Home />)

      // Verify initial dark theme is applied to document
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      const toggleButton = screen.getByRole('button', {
        name: /switch to light mode/i,
      })

      await user.click(toggleButton)

      // Verify data-theme attribute changed to light
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      // Toggle back
      const toggleButtonAfter = screen.getByRole('button', {
        name: /switch to dark mode/i,
      })
      await user.click(toggleButtonAfter)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })
    })

    it('persists theme to localStorage when changed', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />)

      await waitFor(() => {
        expect(getDataTheme()).toBe('dark')
      })

      const toggleButton = screen.getByRole('button', {
        name: /switch to light mode/i,
      })
      await user.click(toggleButton)

      await waitFor(() => {
        expect(localStorageMock.setItem).toHaveBeenCalledWith('theme', 'light')
      })
    })
  })

  describe('Test Case 4: Toggle theme and check homepage components', () => {
    it('homepage sections reflect theme through data-theme attribute', async () => {
      const user = userEvent.setup()
      renderWithProviders(<Home />)

      // Initial dark theme
      await waitFor(() => {
        expect(getDataTheme()).toBe('dark')
      })

      // Find the homepage sections (hero, navigation)
      const navigation = screen.getByRole('navigation')
      expect(navigation).toBeInTheDocument()

      // Find the main content area
      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()

      // Toggle to light theme
      const toggleButton = screen.getByRole('button', {
        name: /switch to light mode/i,
      })
      await user.click(toggleButton)

      // After toggle, the data-theme should be light
      await waitFor(() => {
        expect(getDataTheme()).toBe('light')
      })

      // Components should still be rendered (theme change doesn't break rendering)
      expect(screen.getByRole('navigation')).toBeInTheDocument()
      expect(screen.getByRole('main')).toBeInTheDocument()
    })

    it('theme toggle button icon changes based on current theme', async () => {
      const user = userEvent.setup()
      renderWithProviders(<Home />)

      // In dark mode, button should say "Switch to light mode"
      await waitFor(() => {
        expect(
          screen.getByRole('button', { name: /switch to light mode/i })
        ).toBeInTheDocument()
      })

      const toggleButton = screen.getByRole('button', {
        name: /switch to light mode/i,
      })
      await user.click(toggleButton)

      // In light mode, button should say "Switch to dark mode"
      await waitFor(() => {
        expect(
          screen.getByRole('button', { name: /switch to dark mode/i })
        ).toBeInTheDocument()
      })
    })

    it('all homepage sections respond to theme change without errors', async () => {
      const user = userEvent.setup()
      renderWithProviders(<Home />)

      // Toggle theme multiple times to ensure stability
      const getToggleButton = () =>
        screen.getByRole('button', { name: /switch to (light|dark) mode/i })

      for (let i = 0; i < 4; i++) {
        await user.click(getToggleButton())
        await waitFor(() => {
          expect(screen.getByRole('navigation')).toBeInTheDocument()
          expect(screen.getByRole('main')).toBeInTheDocument()
        })
      }
    })
  })

  describe('Theme persistence', () => {
    it('theme preference is maintained after component re-render', async () => {
      const user = userEvent.setup()
      const { unmount } = renderWithProviders(<Home />)

      // Initial dark theme
      await waitFor(() => {
        expect(getDataTheme()).toBe('dark')
      })

      // Toggle to light
      const toggleButton = screen.getByRole('button', {
        name: /switch to light mode/i,
      })
      await user.click(toggleButton)

      await waitFor(() => {
        expect(getDataTheme()).toBe('light')
      })

      // Unmount and remount
      unmount()
      renderWithProviders(<Home />)

      // Theme should persist
      await waitFor(() => {
        expect(getDataTheme()).toBe('light')
      })
    })

    it('saves theme to localStorage for persistence', async () => {
      const user = userEvent.setup()
      renderWithProviders(<ThemeToggle />)

      const toggleButton = screen.getByRole('button', {
        name: /switch to light mode/i,
      })
      await user.click(toggleButton)

      await waitFor(() => {
        expect(localStorageMock.setItem).toHaveBeenCalledWith('theme', 'light')
      })
    })
  })
})
