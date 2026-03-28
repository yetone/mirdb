/**
 * Theme Toggle Integration Tests
 * Owner: Scenario 10 - Dark/Light Theme Toggle
 *
 * Tests theme toggle functionality switches between dark and light modes
 * and persists preference via localStorage.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor, cleanup } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import React from 'react'
import { MemoryRouter, Routes, Route, useLocation } from 'react-router-dom'
import { ThemeProvider, useTheme } from '@/contexts/ThemeContext'
import { AuthProvider } from '@/contexts/AuthContext'
import Home from '@/pages/Home'
import Navbar from '@/components/Navbar'

// Mock localStorage with full functionality
const createLocalStorageMock = () => {
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
    _getStore: () => store,
    _setStore: (newStore: Record<string, string>) => {
      store = newStore
    },
  }
}

let localStorageMock: ReturnType<typeof createLocalStorageMock>

beforeEach(() => {
  localStorageMock = createLocalStorageMock()
  Object.defineProperty(window, 'localStorage', {
    value: localStorageMock,
    writable: true,
  })
  // Reset document theme attribute
  document.documentElement.setAttribute('data-theme', 'light')
})

afterEach(() => {
  cleanup()
  vi.clearAllMocks()
})

// Helper component to display current location for routing tests
const LocationDisplay: React.FC = () => {
  const location = useLocation()
  return <div data-testid="location-display">{location.pathname}</div>
}

// Component to display current theme for testing
const ThemeDisplay: React.FC = () => {
  const { theme } = useTheme()
  return <div data-testid="current-theme">{theme}</div>
}

// Test wrapper with all providers
interface TestWrapperProps {
  children: React.ReactNode
  initialEntries?: string[]
  initialTheme?: string
}

const TestWrapper: React.FC<TestWrapperProps> = ({
  children,
  initialEntries = ['/'],
  initialTheme,
}) => {
  // Set initial theme in localStorage before render if specified
  if (initialTheme) {
    localStorageMock._setStore({ theme: initialTheme })
  }

  return (
    <MemoryRouter initialEntries={initialEntries}>
      <ThemeProvider>
        <AuthProvider>
          {children}
          <ThemeDisplay />
        </AuthProvider>
      </ThemeProvider>
    </MemoryRouter>
  )
}

// Test app with routing for navigation persistence tests
const TestApp: React.FC<{ initialTheme?: string }> = ({ initialTheme }) => {
  if (initialTheme) {
    localStorageMock._setStore({ theme: initialTheme })
  }

  return (
    <MemoryRouter initialEntries={['/']}>
      <ThemeProvider>
        <AuthProvider>
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/features" element={<div data-testid="features-page"><Navbar />Features Page</div>} />
            <Route path="/about" element={<div data-testid="about-page"><Navbar />About Page</div>} />
          </Routes>
          <ThemeDisplay />
          <LocationDisplay />
        </AuthProvider>
      </ThemeProvider>
    </MemoryRouter>
  )
}

describe('Dark/Light Theme Toggle', () => {
  describe('Test Case 1: Theme toggle button/icon is visible', () => {
    it('should render theme toggle button in HomePage navigation', () => {
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle).toBeInTheDocument()
    })

    it('should have accessible aria-label on theme toggle', () => {
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle).toHaveAttribute('aria-label')
      expect(themeToggle.getAttribute('aria-label')).toMatch(/switch to (dark|light) mode/i)
    })

    it('should display theme toggle as a button element', () => {
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle.tagName.toLowerCase()).toBe('button')
    })

    it('should display icon within theme toggle', () => {
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      const themeToggle = screen.getByTestId('theme-toggle')
      const svg = themeToggle.querySelector('svg')
      expect(svg).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Page displays with light theme colors and backgrounds', () => {
    it('should apply light theme by default', () => {
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      const themeDisplay = screen.getByTestId('current-theme')
      expect(themeDisplay).toHaveTextContent('light')
    })

    it('should set data-theme attribute to light on document', () => {
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })

    it('should display homepage with base-100 background class (light theme compatible)', () => {
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toHaveClass('bg-base-100')
    })

    it('should show moon icon in light theme (indicating dark mode is available)', () => {
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      const themeToggle = screen.getByTestId('theme-toggle')
      const ariaLabel = themeToggle.getAttribute('aria-label')
      expect(ariaLabel).toContain('dark')
    })
  })

  describe('Test Case 3: Page displays with dark theme colors and backgrounds', () => {
    it('should apply dark theme when initialized with dark', async () => {
      render(<TestApp initialTheme="dark" />)

      await waitFor(() => {
        const themeDisplay = screen.getByTestId('current-theme')
        expect(themeDisplay).toHaveTextContent('dark')
      })
    })

    it('should set data-theme attribute to dark on document when dark theme', async () => {
      render(<TestApp initialTheme="dark" />)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })
    })

    it('should show sun icon in dark theme (indicating light mode is available)', async () => {
      render(<TestApp initialTheme="dark" />)

      await waitFor(() => {
        const themeToggle = screen.getByTestId('theme-toggle')
        const ariaLabel = themeToggle.getAttribute('aria-label')
        expect(ariaLabel).toContain('light')
      })
    })
  })

  describe('Test Case 4: Click theme toggle from light to dark', () => {
    it('should switch theme from light to dark on toggle click', async () => {
      const user = userEvent.setup()
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      // Verify initial light theme
      expect(screen.getByTestId('current-theme')).toHaveTextContent('light')

      // Click theme toggle
      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      // Verify theme changed to dark
      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
      })
    })

    it('should update data-theme attribute from light to dark', async () => {
      const user = userEvent.setup()
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      // Click theme toggle
      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      // Verify document attribute updated
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })
    })

    it('should update aria-label to indicate light mode is available after switching to dark', async () => {
      const user = userEvent.setup()
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      await waitFor(() => {
        expect(themeToggle.getAttribute('aria-label')).toContain('light')
      })
    })

    it('should change icon from moon to sun when switching to dark theme', async () => {
      const user = userEvent.setup()
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      const themeToggle = screen.getByTestId('theme-toggle')

      // Get initial SVG path
      const initialSvg = themeToggle.querySelector('svg')
      const initialPath = initialSvg?.querySelector('path')?.getAttribute('d')

      await user.click(themeToggle)

      await waitFor(() => {
        const newSvg = themeToggle.querySelector('svg')
        const newPath = newSvg?.querySelector('path')?.getAttribute('d')
        // The path should be different (different icon)
        expect(newPath).not.toBe(initialPath)
      })
    })
  })

  describe('Test Case 5: Click theme toggle from dark to light', () => {
    it('should switch theme from dark to light on toggle click', async () => {
      const user = userEvent.setup()
      render(<TestApp initialTheme="dark" />)

      // Verify initial dark theme
      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
      })

      // Click theme toggle
      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      // Verify theme changed to light
      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('light')
      })
    })

    it('should update data-theme attribute from dark to light', async () => {
      const user = userEvent.setup()
      render(<TestApp initialTheme="dark" />)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Click theme toggle
      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      // Verify document attribute updated
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })
    })

    it('should update aria-label to indicate dark mode is available after switching to light', async () => {
      const user = userEvent.setup()
      render(<TestApp initialTheme="dark" />)

      const themeToggle = await screen.findByTestId('theme-toggle')
      await user.click(themeToggle)

      await waitFor(() => {
        expect(themeToggle.getAttribute('aria-label')).toContain('dark')
      })
    })
  })

  describe('Test Case 6: Theme preference persists across navigation via localStorage', () => {
    it('should save theme to localStorage when toggled', async () => {
      const user = userEvent.setup()
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      // Toggle to dark
      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      // Verify localStorage was called with dark theme
      await waitFor(() => {
        expect(localStorageMock.setItem).toHaveBeenCalledWith('theme', 'dark')
      })
    })

    it('should maintain theme when navigating to another page', async () => {
      const user = userEvent.setup()
      render(<TestApp />)

      // Toggle to dark
      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
      })

      // Navigate to features page
      const featuresLink = screen.getByTestId('nav-features')
      await user.click(featuresLink)

      // Verify we're on features page
      await waitFor(() => {
        expect(screen.getByTestId('location-display')).toHaveTextContent('/features')
      })

      // Theme should still be dark
      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
    })

    it('should maintain theme when navigating back to homepage', async () => {
      const user = userEvent.setup()
      render(<TestApp />)

      // Toggle to dark
      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      // Navigate to features
      const featuresLink = screen.getByTestId('nav-features')
      await user.click(featuresLink)

      await waitFor(() => {
        expect(screen.getByTestId('features-page')).toBeInTheDocument()
      })

      // Navigate back to home
      const logo = screen.getByTestId('navbar-logo')
      await user.click(logo)

      // Verify we're back on homepage
      await waitFor(() => {
        expect(screen.getByTestId('location-display')).toHaveTextContent('/')
      })

      // Theme should still be dark
      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
    })
  })

  describe('Test Case 7: Theme preference persists after page refresh', () => {
    it('should read theme from localStorage on initial render', () => {
      // Set initial theme in localStorage
      localStorageMock._setStore({ theme: 'dark' })

      render(<TestApp />)

      // Theme should be read from localStorage
      expect(localStorageMock.getItem).toHaveBeenCalledWith('theme')
    })

    it('should apply stored dark theme on component mount', async () => {
      render(<TestApp initialTheme="dark" />)

      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })
    })

    it('should apply stored light theme on component mount', async () => {
      render(<TestApp initialTheme="light" />)

      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('light')
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })
    })

    it('should default to light theme when localStorage is empty', () => {
      // Ensure localStorage returns null
      localStorageMock._setStore({})

      render(<TestApp />)

      expect(screen.getByTestId('current-theme')).toHaveTextContent('light')
    })

    it('should persist theme changes to localStorage for future sessions', async () => {
      const user = userEvent.setup()
      render(<TestApp />)

      // Toggle multiple times
      const themeToggle = screen.getByTestId('theme-toggle')

      await user.click(themeToggle) // light -> dark
      await waitFor(() => {
        expect(localStorageMock.setItem).toHaveBeenCalledWith('theme', 'dark')
      })

      await user.click(themeToggle) // dark -> light
      await waitFor(() => {
        expect(localStorageMock.setItem).toHaveBeenCalledWith('theme', 'light')
      })
    })
  })

  describe('Test Case 8: All text content is readable against dark backgrounds', () => {
    it('should use semantic DaisyUI color classes for text that work in both themes', () => {
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      const homePage = screen.getByTestId('home-page')

      // Check that homepage uses theme-aware classes (base-100, base-200, base-content)
      // These classes automatically adapt to dark/light themes
      expect(homePage.className).toMatch(/bg-base-100/)
    })

    it('should have navigation text using theme-aware classes in dark mode', async () => {
      render(<TestApp initialTheme="dark" />)

      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
      })

      // Navbar should be present and using theme-aware classes
      const navbar = screen.getByTestId('navbar')
      expect(navbar).toHaveClass('bg-base-100')
    })

    it('should maintain text visibility when switching themes', async () => {
      const user = userEvent.setup()
      render(<TestApp />)

      // Check elements are visible in light theme
      expect(screen.getByTestId('navbar-logo')).toBeVisible()
      expect(screen.getByTestId('nav-features')).toBeVisible()

      // Toggle to dark
      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
      })

      // Check elements are still visible in dark theme
      expect(screen.getByTestId('navbar-logo')).toBeVisible()
      expect(screen.getByTestId('nav-features')).toBeVisible()
    })

    it('should use text-base-content class for readable text in any theme', async () => {
      render(<TestApp initialTheme="dark" />)

      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
      })

      // Features link uses text-base-content for theme-aware text color
      const featuresLink = screen.getByTestId('nav-features')
      expect(featuresLink).toHaveClass('text-base-content')
    })

    it('should have cards using theme-aware background classes', async () => {
      render(<TestApp initialTheme="dark" />)

      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
      })

      // Find feature cards - they should use bg-base-200 or similar theme classes
      const homePage = screen.getByTestId('home-page')
      const cards = homePage.querySelectorAll('.card')

      // Cards should exist and use theme-aware classes
      expect(cards.length).toBeGreaterThan(0)
      cards.forEach((card) => {
        expect(card.className).toMatch(/bg-base-\d+/)
      })
    })
  })

  describe('Additional Theme Toggle Tests', () => {
    it('should handle rapid theme toggles correctly', async () => {
      const user = userEvent.setup()
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      const themeToggle = screen.getByTestId('theme-toggle')

      // Rapid toggles
      await user.click(themeToggle) // light -> dark
      await user.click(themeToggle) // dark -> light
      await user.click(themeToggle) // light -> dark

      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
      })
    })

    it('should sync theme state with document attribute', async () => {
      const user = userEvent.setup()
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      const themeToggle = screen.getByTestId('theme-toggle')

      // Toggle and verify sync
      await user.click(themeToggle)

      await waitFor(() => {
        const contextTheme = screen.getByTestId('current-theme').textContent
        const documentTheme = document.documentElement.getAttribute('data-theme')
        expect(contextTheme).toBe(documentTheme)
      })
    })

    it('should be keyboard accessible', async () => {
      const user = userEvent.setup()
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      const themeToggle = screen.getByTestId('theme-toggle')

      // Focus and activate with keyboard
      themeToggle.focus()
      expect(document.activeElement).toBe(themeToggle)

      // Press Enter to activate
      await user.keyboard('{Enter}')

      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
      })
    })

    it('should be activatable with Space key', async () => {
      const user = userEvent.setup()
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      const themeToggle = screen.getByTestId('theme-toggle')
      themeToggle.focus()

      await user.keyboard(' ')

      await waitFor(() => {
        expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
      })
    })
  })
})
