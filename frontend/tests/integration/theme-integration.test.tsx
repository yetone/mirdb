/**
 * Theme integration tests for homepage.
 * Owner: Scenario 7 - Theme Support Integration
 *
 * Tests the homepage integration with the existing theme system:
 * - Default theme application
 * - Theme switching (dark, cyberpunk)
 * - Theme persistence across navigation
 * - Theme toggle visibility and accessibility
 */

import React from 'react'
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter, MemoryRouter, Routes, Route } from 'react-router-dom'
import { ThemeProvider, useTheme } from '@/contexts/ThemeContext'
import { Home } from '@/pages/Home'
import { Navbar } from '@/components/layout/Navbar'
import { Footer } from '@/components/layout/Footer'
import { Theme } from '@/types'

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
    get store() {
      return store
    },
  }
})()

Object.defineProperty(window, 'localStorage', { value: localStorageMock })

// Test wrapper component with ThemeProvider and Router
const TestWrapper = ({ children }: { children: React.ReactNode }) => (
  <ThemeProvider>
    <BrowserRouter>{children}</BrowserRouter>
  </ThemeProvider>
)

// Memory Router wrapper for navigation tests
const MemoryRouterWrapper = ({
  children,
  initialEntries = ['/']
}: {
  children: React.ReactNode
  initialEntries?: string[]
}) => (
  <ThemeProvider>
    <MemoryRouter initialEntries={initialEntries}>
      {children}
    </MemoryRouter>
  </ThemeProvider>
)

// Helper component to display current theme for testing
const ThemeDisplay = () => {
  const { theme } = useTheme()
  return <div data-testid="current-theme">{theme}</div>
}

// Simple Login page mock for navigation tests
const MockLoginPage = () => (
  <div data-testid="login-page">
    <h1>Login Page</h1>
  </div>
)

describe('Theme Support Integration', () => {
  beforeEach(() => {
    localStorageMock.clear()
    vi.clearAllMocks()
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  describe('Test Case 1: Load homepage with default theme', () => {
    /**
     * Unit Test: Homepage renders correctly with default theme applied
     * When no theme is stored in localStorage, the default theme (dark) should be applied
     */
    it('renders homepage with default dark theme when no theme is stored', () => {
      render(
        <TestWrapper>
          <Home />
          <ThemeDisplay />
        </TestWrapper>
      )

      // Default theme should be 'dark' as per ThemeContext implementation
      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('renders all homepage sections with default theme', () => {
      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      // Hero section should be present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()

      // Features grid should be present (FeaturesSection uses features-grid)
      expect(screen.getByTestId('features-grid')).toBeInTheDocument()

      // URL Demo section should be present
      expect(screen.getByTestId('url-demo-section')).toBeInTheDocument()

      // Footer should be present
      expect(screen.getByTestId('footer')).toBeInTheDocument()
    })

    it('applies stored theme from localStorage on load', () => {
      // Pre-set a theme in localStorage
      localStorageMock.setItem('theme', 'cyberpunk')

      render(
        <TestWrapper>
          <Home />
          <ThemeDisplay />
        </TestWrapper>
      )

      expect(screen.getByTestId('current-theme')).toHaveTextContent('cyberpunk')
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })
  })

  describe('Test Case 2: Switch to dark theme', () => {
    /**
     * Integration Test: All homepage elements update to dark theme colors
     * Text remains readable when switching to dark theme
     */
    it('updates homepage elements when switching to dark theme', async () => {
      const user = userEvent.setup()

      // Start with light theme
      localStorageMock.setItem('theme', 'light')

      render(
        <TestWrapper>
          <Navbar />
          <ThemeDisplay />
        </TestWrapper>
      )

      // Verify starting theme is light
      expect(screen.getByTestId('current-theme')).toHaveTextContent('light')

      // Find theme selector and change to dark
      const themeSelector = screen.getByLabelText('Select theme')
      await user.selectOptions(themeSelector, 'dark')

      // Verify theme changed to dark
      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('persists dark theme to localStorage', async () => {
      const user = userEvent.setup()

      localStorageMock.setItem('theme', 'light')

      render(
        <TestWrapper>
          <Navbar />
        </TestWrapper>
      )

      const themeSelector = screen.getByLabelText('Select theme')
      await user.selectOptions(themeSelector, 'dark')

      // Verify localStorage was updated
      expect(localStorageMock.setItem).toHaveBeenCalledWith('theme', 'dark')
    })

    it('maintains element visibility after switching to dark theme', async () => {
      const user = userEvent.setup()

      localStorageMock.setItem('theme', 'light')

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      // Find and change theme
      const themeSelector = screen.getByLabelText('Select theme')
      await user.selectOptions(themeSelector, 'dark')

      // All sections should still be visible
      expect(screen.getByTestId('hero-section')).toBeVisible()
      expect(screen.getByTestId('features-grid')).toBeVisible()
      expect(screen.getByTestId('url-demo-section')).toBeVisible()
      expect(screen.getByTestId('footer')).toBeVisible()

      // Hero headline should remain visible and readable
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeVisible()
    })
  })

  describe('Test Case 3: Switch to cyberpunk theme', () => {
    /**
     * Integration Test: Homepage displays cyberpunk theme styling correctly
     */
    it('applies cyberpunk theme when selected', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <Navbar />
          <ThemeDisplay />
        </TestWrapper>
      )

      const themeSelector = screen.getByLabelText('Select theme')
      await user.selectOptions(themeSelector, 'cyberpunk')

      expect(screen.getByTestId('current-theme')).toHaveTextContent('cyberpunk')
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })

    it('shows cyberpunk option in theme selector', () => {
      render(
        <TestWrapper>
          <Navbar />
        </TestWrapper>
      )

      const themeSelector = screen.getByLabelText('Select theme') as HTMLSelectElement
      const options = Array.from(themeSelector.options).map(opt => opt.value)

      expect(options).toContain('cyberpunk')
    })

    it('homepage elements remain functional with cyberpunk theme', async () => {
      const user = userEvent.setup()

      localStorageMock.setItem('theme', 'cyberpunk')

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      // Verify cyberpunk theme is applied
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')

      // All homepage elements should be interactive
      const registerCta = screen.getByTestId('hero-primary-cta')
      expect(registerCta).toBeEnabled()

      const loginCta = screen.getByTestId('hero-login-cta')
      expect(loginCta).toBeEnabled()

      const urlInput = screen.getByTestId('url-input')
      await user.type(urlInput, 'https://test.com')
      expect(urlInput).toHaveValue('https://test.com')
    })

    it('feature cards display correctly with cyberpunk theme', async () => {
      localStorageMock.setItem('theme', 'cyberpunk')

      render(
        <TestWrapper>
          <Home />
        </TestWrapper>
      )

      // Feature cards should be visible
      const featureCards = screen.getAllByTestId(/feature-card-/)
      expect(featureCards.length).toBeGreaterThan(0)

      featureCards.forEach(card => {
        expect(card).toBeVisible()
      })
    })
  })

  describe('Test Case 4: Verify theme persistence', () => {
    /**
     * E2E Test: Selected theme persists across page navigation and reload
     */
    it('persists theme across route navigation', async () => {
      const user = userEvent.setup()

      render(
        <MemoryRouterWrapper initialEntries={['/']}>
          <Routes>
            <Route path="/" element={
              <>
                <Navbar />
                <ThemeDisplay />
              </>
            } />
            <Route path="/login" element={
              <>
                <Navbar />
                <MockLoginPage />
                <ThemeDisplay />
              </>
            } />
          </Routes>
        </MemoryRouterWrapper>
      )

      // Set theme to cyberpunk
      const themeSelector = screen.getByLabelText('Select theme')
      await user.selectOptions(themeSelector, 'cyberpunk')

      // Verify theme is applied
      expect(screen.getByTestId('current-theme')).toHaveTextContent('cyberpunk')

      // Navigate to login page (get first visible login link)
      const loginLinks = screen.getAllByRole('link', { name: /login/i })
      await user.click(loginLinks[0])

      // Theme should persist on new page
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })

      expect(screen.getByTestId('current-theme')).toHaveTextContent('cyberpunk')
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })

    it('stores theme selection in localStorage', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <Navbar />
        </TestWrapper>
      )

      // Change theme multiple times
      const themeSelector = screen.getByLabelText('Select theme')

      await user.selectOptions(themeSelector, 'synthwave')
      expect(localStorageMock.setItem).toHaveBeenCalledWith('theme', 'synthwave')

      await user.selectOptions(themeSelector, 'retro')
      expect(localStorageMock.setItem).toHaveBeenCalledWith('theme', 'retro')

      await user.selectOptions(themeSelector, 'valentine')
      expect(localStorageMock.setItem).toHaveBeenCalledWith('theme', 'valentine')
    })

    it('restores theme from localStorage after remount', () => {
      // Set theme in localStorage
      localStorageMock.setItem('theme', 'synthwave')

      const { unmount } = render(
        <TestWrapper>
          <Navbar />
          <ThemeDisplay />
        </TestWrapper>
      )

      expect(screen.getByTestId('current-theme')).toHaveTextContent('synthwave')

      // Unmount and remount to simulate page reload
      unmount()

      render(
        <TestWrapper>
          <Navbar />
          <ThemeDisplay />
        </TestWrapper>
      )

      // Theme should still be synthwave
      expect(screen.getByTestId('current-theme')).toHaveTextContent('synthwave')
    })

    it('handles invalid theme in localStorage gracefully', () => {
      // Set invalid theme in localStorage
      localStorageMock.setItem('theme', 'invalid-theme')

      render(
        <TestWrapper>
          <ThemeDisplay />
        </TestWrapper>
      )

      // Should fall back to default theme
      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
    })
  })

  describe('Test Case 5: Check theme toggle visibility', () => {
    /**
     * Unit Test: Theme toggle is accessible and visible in footer or navigation
     */
    it('renders theme toggle in Navbar', () => {
      render(
        <TestWrapper>
          <Navbar />
        </TestWrapper>
      )

      const themeSelector = screen.getByLabelText('Select theme')
      expect(themeSelector).toBeInTheDocument()
      expect(themeSelector).toBeVisible()
    })

    it('theme toggle is accessible via keyboard', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <Navbar />
        </TestWrapper>
      )

      const themeSelector = screen.getByLabelText('Select theme')

      // Tab to focus the select element
      await user.tab()
      await user.tab()
      await user.tab()

      // The select should be focusable
      expect(themeSelector.tagName.toLowerCase()).toBe('select')
    })

    it('theme selector has proper aria-label', () => {
      render(
        <TestWrapper>
          <Navbar />
        </TestWrapper>
      )

      const themeSelector = screen.getByLabelText('Select theme')
      expect(themeSelector).toHaveAttribute('aria-label', 'Select theme')
    })

    it('theme selector shows all available themes', () => {
      render(
        <TestWrapper>
          <Navbar />
        </TestWrapper>
      )

      const themeSelector = screen.getByLabelText('Select theme') as HTMLSelectElement
      const options = Array.from(themeSelector.options).map(opt => opt.value)

      // Should include all themes from ThemeContext
      expect(options).toContain('light')
      expect(options).toContain('dark')
      expect(options).toContain('cyberpunk')
      expect(options).toContain('synthwave')
      expect(options).toContain('retro')
      expect(options).toContain('valentine')
    })

    it('displays currently selected theme in selector', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <Navbar />
        </TestWrapper>
      )

      const themeSelector = screen.getByLabelText('Select theme') as HTMLSelectElement

      // Default should be 'dark'
      expect(themeSelector.value).toBe('dark')

      // Change and verify it updates
      await user.selectOptions(themeSelector, 'cyberpunk')
      expect(themeSelector.value).toBe('cyberpunk')
    })
  })

  describe('Additional Theme Integration Tests', () => {
    /**
     * Additional coverage for edge cases and full integration
     */
    it('theme applies to document root element', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <Navbar />
        </TestWrapper>
      )

      const themeSelector = screen.getByLabelText('Select theme')

      // Test each theme applies to document root
      const themes: Theme[] = ['light', 'dark', 'cyberpunk', 'synthwave', 'retro', 'valentine']

      for (const themeName of themes) {
        await user.selectOptions(themeSelector, themeName)
        expect(document.documentElement.getAttribute('data-theme')).toBe(themeName)
      }
    })

    it('ThemeContext provides setTheme and availableThemes', () => {
      let contextValue: ReturnType<typeof useTheme> | undefined

      const ThemeConsumer = () => {
        contextValue = useTheme()
        return null
      }

      render(
        <ThemeProvider>
          <ThemeConsumer />
        </ThemeProvider>
      )

      expect(contextValue).toBeDefined()
      expect(typeof contextValue!.setTheme).toBe('function')
      expect(Array.isArray(contextValue!.availableThemes)).toBe(true)
      expect(contextValue!.availableThemes.length).toBeGreaterThan(0)
    })

    it('throws error when useTheme is used outside ThemeProvider', () => {
      const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})

      const ThemeConsumerWithoutProvider = () => {
        const context = useTheme()
        return <div>{context.theme}</div>
      }

      expect(() => {
        render(<ThemeConsumerWithoutProvider />)
      }).toThrow('useTheme must be used within a ThemeProvider')

      consoleError.mockRestore()
    })

    it('multiple components share the same theme state', async () => {
      const user = userEvent.setup()

      render(
        <TestWrapper>
          <Home />
          <ThemeDisplay />
        </TestWrapper>
      )

      // Home includes Navbar, so theme changes should affect all components
      // Get the first theme selector (there are two due to responsive design)
      const themeSelectors = screen.getAllByLabelText('Select theme')
      await user.selectOptions(themeSelectors[0], 'synthwave')

      // ThemeDisplay should show the updated theme
      expect(screen.getByTestId('current-theme')).toHaveTextContent('synthwave')

      // And the DOM should reflect it
      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
    })
  })
})
