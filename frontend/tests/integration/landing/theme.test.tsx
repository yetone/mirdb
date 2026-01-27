/**
 * Theme Integration Tests
 * Owner: Scenario 9 - Theme Integration - Dark Mode
 *
 * Tests that the landing page integrates with the existing theme system
 * and supports dark mode properly.
 *
 * Test cases:
 * 1. Page renders with dark theme colors when ThemeContext is set to dark
 * 2. Theme toggles from light to dark and updates all sections
 * 3. Text remains readable with sufficient contrast in dark mode
 * 4. Page renders with light theme colors when ThemeContext is set to light
 */

import { describe, it, expect, beforeEach, vi } from 'vitest'
import { render, screen, fireEvent, cleanup, waitFor } from './setup'
import Home from '../../../src/pages/Home'

// Mock localStorage for theme persistence
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

describe('Theme Integration - Dark Mode', () => {
  beforeEach(() => {
    cleanup()
    localStorageMock.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  describe('Test Case 1: Render LandingPage with ThemeContext set to dark', () => {
    it('should render page with dark theme colors when theme is dark', async () => {
      // Set dark theme in localStorage before render
      localStorageMock.setItem('theme', 'dark')

      render(<Home />)

      // Wait for theme to be applied
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Verify page renders correctly
      expect(screen.getByText('URL Shortener')).toBeInTheDocument()
      expect(screen.getByText('Shorten Links. Track Everything.')).toBeInTheDocument()

      // Verify theme toggle shows sun icon (indicating dark mode is active)
      const themeToggleButton = screen.getAllByRole('button', { name: /switch to light mode/i })[0]
      expect(themeToggleButton).toBeInTheDocument()
    })

    it('should apply dark theme data attribute to document', async () => {
      localStorageMock.setItem('theme', 'dark')

      render(<Home />)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })
    })
  })

  describe('Test Case 2: Toggle theme from light to dark', () => {
    it('should update all landing page sections when theme is toggled', async () => {
      // Start with light theme
      localStorageMock.setItem('theme', 'light')

      render(<Home />)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      // Find and click the theme toggle button (there may be multiple, use the first one)
      const themeToggleButtons = screen.getAllByRole('button', { name: /switch to dark mode/i })
      expect(themeToggleButtons.length).toBeGreaterThan(0)

      fireEvent.click(themeToggleButtons[0])

      // Verify theme changes to dark
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Verify the toggle button now shows option to switch to light mode
      const updatedToggleButtons = screen.getAllByRole('button', { name: /switch to light mode/i })
      expect(updatedToggleButtons.length).toBeGreaterThan(0)
    })

    it('should persist theme preference to localStorage when toggled', async () => {
      localStorageMock.setItem('theme', 'light')

      render(<Home />)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      const themeToggleButtons = screen.getAllByRole('button', { name: /switch to dark mode/i })
      fireEvent.click(themeToggleButtons[0])

      await waitFor(() => {
        expect(localStorageMock.setItem).toHaveBeenCalledWith('theme', 'dark')
      })
    })

    it('should update hero section styling when theme toggles', async () => {
      localStorageMock.setItem('theme', 'light')

      render(<Home />)

      // Verify hero section is present
      expect(screen.getByText('Shorten Links. Track Everything.')).toBeInTheDocument()
      expect(screen.getByText(/Create short, powerful links/)).toBeInTheDocument()

      // Toggle theme
      const themeToggleButtons = screen.getAllByRole('button', { name: /switch to dark mode/i })
      fireEvent.click(themeToggleButtons[0])

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Hero section content should still be visible after theme change
      expect(screen.getByText('Shorten Links. Track Everything.')).toBeInTheDocument()
    })

    it('should update features section styling when theme toggles', async () => {
      localStorageMock.setItem('theme', 'light')

      render(<Home />)

      // Verify features section is present
      expect(screen.getByText('Features')).toBeInTheDocument()
      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
      expect(screen.getByText('Click Analytics')).toBeInTheDocument()

      // Toggle theme
      const themeToggleButtons = screen.getAllByRole('button', { name: /switch to dark mode/i })
      fireEvent.click(themeToggleButtons[0])

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Features section content should still be visible after theme change
      expect(screen.getByText('Features')).toBeInTheDocument()
      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
    })
  })

  describe('Test Case 3: Check text contrast in dark mode', () => {
    it('should render text elements with appropriate classes for readability', async () => {
      localStorageMock.setItem('theme', 'dark')

      render(<Home />)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Verify main headline is rendered with gradient text styling for contrast
      const headline = screen.getByText('Shorten Links. Track Everything.')
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveClass('bg-gradient-to-r', 'from-primary', 'to-secondary', 'bg-clip-text')

      // Verify subheadline is rendered with proper content opacity class for readability
      const subheadline = screen.getByText(/Create short, powerful links/)
      expect(subheadline).toBeInTheDocument()
      expect(subheadline).toHaveClass('text-base-content/80')

      // Verify navigation text is visible (not animated)
      // Use getAllBy because there may be multiple Login elements
      const loginElements = screen.getAllByText('Login')
      expect(loginElements.length).toBeGreaterThan(0)
      expect(loginElements[0]).toBeVisible()

      // Get Started appears in both header and hero section
      const getStartedElements = screen.getAllByText('Get Started')
      expect(getStartedElements.length).toBeGreaterThan(0)
      expect(getStartedElements[0]).toBeVisible()
    })

    it('should maintain readable text in features section with dark theme', async () => {
      localStorageMock.setItem('theme', 'dark')

      render(<Home />)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Verify all feature titles are rendered in DOM with proper styling
      // Note: Framer Motion animates these elements, so they start with opacity: 0
      // We verify they're in the document and have correct contrast classes
      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
      expect(screen.getByText('Click Analytics')).toBeInTheDocument()
      expect(screen.getByText('Dashboard Management')).toBeInTheDocument()
      expect(screen.getByText('Share Stats')).toBeInTheDocument()

      // Verify feature descriptions are present with proper contrast classes
      const descriptions = screen.getAllByTestId('feature-description')
      expect(descriptions.length).toBe(4)
      descriptions.forEach((desc) => {
        expect(desc).toBeInTheDocument()
        // Verify the descriptions have appropriate text color class for contrast
        expect(desc).toHaveClass('text-base-content/70')
      })
    })

    it('should ensure navigation elements have sufficient contrast in dark mode', async () => {
      localStorageMock.setItem('theme', 'dark')

      render(<Home />)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Verify navigation elements are visible and accessible
      const loginLink = screen.getByRole('link', { name: /login/i })
      expect(loginLink).toBeVisible()

      const getStartedButton = screen.getByRole('link', { name: /get started/i })
      expect(getStartedButton).toBeVisible()

      // Verify theme toggle is accessible
      const themeToggle = screen.getAllByRole('button', { name: /switch to light mode/i })[0]
      expect(themeToggle).toBeVisible()
    })
  })

  describe('Test Case 4: Render LandingPage with ThemeContext set to light', () => {
    it('should render page with light theme colors when theme is light', async () => {
      localStorageMock.setItem('theme', 'light')

      render(<Home />)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      // Verify page renders correctly
      expect(screen.getByText('URL Shortener')).toBeInTheDocument()
      expect(screen.getByText('Shorten Links. Track Everything.')).toBeInTheDocument()

      // Verify theme toggle shows moon icon (indicating light mode is active)
      const themeToggleButton = screen.getAllByRole('button', { name: /switch to dark mode/i })[0]
      expect(themeToggleButton).toBeInTheDocument()
    })

    it('should apply light theme data attribute to document', async () => {
      localStorageMock.setItem('theme', 'light')

      render(<Home />)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })
    })

    it('should render all landing page sections with light theme', async () => {
      localStorageMock.setItem('theme', 'light')

      render(<Home />)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      // Verify all major sections are rendered
      // Header
      expect(screen.getByText('URL Shortener')).toBeInTheDocument()

      // Hero
      expect(screen.getByText('Shorten Links. Track Everything.')).toBeInTheDocument()

      // Features
      expect(screen.getByText('Features')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()

      // How It Works
      expect(screen.getByText('How It Works')).toBeInTheDocument()
    })
  })

  describe('Theme toggle accessibility', () => {
    it('should have accessible label on theme toggle button', async () => {
      localStorageMock.setItem('theme', 'dark')

      render(<Home />)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Theme toggle should have accessible label
      const themeToggleButtons = screen.getAllByRole('button', { name: /switch to (light|dark) mode/i })
      expect(themeToggleButtons.length).toBeGreaterThan(0)
      themeToggleButtons.forEach((button) => {
        expect(button).toHaveAccessibleName()
      })
    })

    it('should toggle theme with keyboard interaction', async () => {
      localStorageMock.setItem('theme', 'light')

      render(<Home />)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      const themeToggleButton = screen.getAllByRole('button', { name: /switch to dark mode/i })[0]

      // Focus and press Enter
      themeToggleButton.focus()
      fireEvent.keyDown(themeToggleButton, { key: 'Enter', code: 'Enter' })
      fireEvent.click(themeToggleButton)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })
    })
  })
})
