/**
 * Theme Switching Integration Tests
 * Owner: Scenario 5 - Theme Switching
 *
 * Tests for homepage theme switching functionality.
 *
 * Test coverage:
 * - Home renders correctly with dark theme (default)
 * - Home renders correctly with light theme
 * - Theme toggle switches between dark and light
 * - Theme preference persists across navigation
 * - Home renders correctly with cyberpunk theme
 * - Home renders correctly with synthwave theme
 */
import { describe, it, expect, beforeEach, vi } from 'vitest'
import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { render as rtlRender } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { ReactElement, ReactNode } from 'react'
import App from '../../../src/App'
import Home from '../../../src/pages/Home'
import { ThemeProvider } from '../../../src/contexts/ThemeContext'
import { AuthProvider } from '../../../src/contexts/AuthContext'

type Theme = 'light' | 'dark' | 'cyberpunk' | 'synthwave'

// Custom render with theme control
interface RenderWithThemeOptions {
  initialTheme?: Theme
  initialEntries?: string[]
}

function renderWithTheme(
  ui: ReactElement,
  { initialTheme = 'dark', initialEntries = ['/'] }: RenderWithThemeOptions = {}
) {
  // Mock localStorage to return the specified theme
  const localStorageData: Record<string, string> = {
    theme: initialTheme,
  }

  const localStorageMock = {
    getItem: vi.fn((key: string) => localStorageData[key] ?? null),
    setItem: vi.fn((key: string, value: string) => {
      localStorageData[key] = value
    }),
    removeItem: vi.fn((key: string) => {
      delete localStorageData[key]
    }),
    clear: vi.fn(() => {
      Object.keys(localStorageData).forEach((key) => delete localStorageData[key])
    }),
    length: 0,
    key: vi.fn(() => null),
  }

  Object.defineProperty(window, 'localStorage', {
    value: localStorageMock,
    writable: true,
  })

  function Wrapper({ children }: { children: ReactNode }) {
    return (
      <MemoryRouter initialEntries={initialEntries}>
        <ThemeProvider>
          <AuthProvider>{children}</AuthProvider>
        </ThemeProvider>
      </MemoryRouter>
    )
  }

  return {
    ...rtlRender(ui, { wrapper: Wrapper }),
    localStorageMock,
    localStorageData,
  }
}

describe('Theme Switching', () => {
  beforeEach(() => {
    // Reset document theme attribute
    document.documentElement.removeAttribute('data-theme')
  })

  describe('Unit Tests - Theme Rendering', () => {
    it('should render Home with dark theme and all text readable', async () => {
      renderWithTheme(<Home />, { initialTheme: 'dark' })

      // Verify theme is set to dark
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Verify home page renders
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()

      // Verify key text elements are visible (hero section)
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
      expect(heroSection).toBeVisible()

      // Verify features section text is readable
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
      expect(featuresSection).toBeVisible()

      // Verify CTA buttons are visible and readable
      const signUpButton = screen.getByRole('link', { name: /sign up/i })
      expect(signUpButton).toBeInTheDocument()
      expect(signUpButton).toBeVisible()
    })

    it('should render Home with light theme and all text readable', async () => {
      renderWithTheme(<Home />, { initialTheme: 'light' })

      // Verify theme is set to light
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      // Verify home page renders
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()

      // Verify key text elements are visible
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
      expect(heroSection).toBeVisible()

      // Verify features section text is readable
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
      expect(featuresSection).toBeVisible()

      // Verify CTA buttons are visible and readable
      const loginButton = screen.getByRole('link', { name: /log in/i })
      expect(loginButton).toBeInTheDocument()
      expect(loginButton).toBeVisible()
    })

    it('should render Home with cyberpunk theme and correct colors', async () => {
      renderWithTheme(<Home />, { initialTheme: 'cyberpunk' })

      // Verify theme is set to cyberpunk
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
      })

      // Verify home page renders correctly
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
      expect(homePage).toBeVisible()

      // Verify all sections render with cyberpunk theme
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
      expect(heroSection).toBeVisible()

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
      expect(featuresSection).toBeVisible()

      // Verify buttons are visible
      const signUpButton = screen.getByRole('link', { name: /sign up/i })
      expect(signUpButton).toBeVisible()
    })

    it('should render Home with synthwave theme and correct colors', async () => {
      renderWithTheme(<Home />, { initialTheme: 'synthwave' })

      // Verify theme is set to synthwave
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
      })

      // Verify home page renders correctly
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
      expect(homePage).toBeVisible()

      // Verify all sections render with synthwave theme
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
      expect(heroSection).toBeVisible()

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
      expect(featuresSection).toBeVisible()

      // Verify buttons are visible
      const loginButton = screen.getByRole('link', { name: /log in/i })
      expect(loginButton).toBeVisible()
    })
  })

  describe('Integration Tests - Theme Toggle', () => {
    it('should toggle theme from dark to light when clicking theme toggle', async () => {
      const user = userEvent.setup()
      const { localStorageData } = renderWithTheme(<Home />, { initialTheme: 'dark' })

      // Verify initial dark theme
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Find and click the theme toggle
      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle).toBeInTheDocument()

      // Toggle should indicate switching to light mode
      expect(themeToggle).toHaveAttribute('aria-label', 'Switch to light theme')

      await user.click(themeToggle)

      // Verify theme changed to light
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      // Verify localStorage was updated
      expect(localStorageData.theme).toBe('light')

      // Verify aria-label updated
      expect(themeToggle).toHaveAttribute('aria-label', 'Switch to dark theme')
    })

    it('should toggle theme from light to dark when clicking theme toggle', async () => {
      const user = userEvent.setup()
      const { localStorageData } = renderWithTheme(<Home />, { initialTheme: 'light' })

      // Verify initial light theme
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      // Find and click the theme toggle
      const themeToggle = screen.getByTestId('theme-toggle')

      // Toggle should indicate switching to dark mode
      expect(themeToggle).toHaveAttribute('aria-label', 'Switch to dark theme')

      await user.click(themeToggle)

      // Verify theme changed to dark
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Verify localStorage was updated
      expect(localStorageData.theme).toBe('dark')
    })

    it('should persist theme preference across navigation', async () => {
      const user = userEvent.setup()
      const { localStorageData } = renderWithTheme(<App />, {
        initialTheme: 'dark',
        initialEntries: ['/'],
      })

      // Verify initial dark theme
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // Toggle to light theme
      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      // Verify theme is now light
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })
      expect(localStorageData.theme).toBe('light')

      // Navigate away to login page
      const loginLink = screen.getByTestId('nav-login')
      await user.click(loginLink)

      // Verify we navigated to login
      await waitFor(() => {
        expect(screen.getByRole('heading', { name: /login/i })).toBeInTheDocument()
      })

      // Theme should still be light after navigation
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      expect(localStorageData.theme).toBe('light')

      // Navigate back to home (using the logo/brand link if available, or register then back)
      const registerLink = screen.getByTestId('nav-register')
      await user.click(registerLink)

      // Navigate to register page
      await waitFor(() => {
        expect(screen.getByRole('heading', { name: /register/i })).toBeInTheDocument()
      })

      // Theme should still be light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })
  })

  describe('Edge Cases', () => {
    it('should handle multiple rapid theme toggles', async () => {
      const user = userEvent.setup()
      const { localStorageData } = renderWithTheme(<Home />, { initialTheme: 'dark' })

      const themeToggle = screen.getByTestId('theme-toggle')

      // Toggle multiple times rapidly
      await user.click(themeToggle) // dark -> light
      await user.click(themeToggle) // light -> dark
      await user.click(themeToggle) // dark -> light

      // Final state should be light
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })
      expect(localStorageData.theme).toBe('light')
    })

    it('should maintain visual consistency after theme toggle', async () => {
      const user = userEvent.setup()
      renderWithTheme(<Home />, { initialTheme: 'dark' })

      // All elements should be visible in dark theme
      expect(screen.getByTestId('home-page')).toBeVisible()
      expect(screen.getByTestId('hero-section')).toBeVisible()
      expect(screen.getByTestId('features-section')).toBeVisible()

      // Toggle to light
      const themeToggle = screen.getByTestId('theme-toggle')
      await user.click(themeToggle)

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      // All elements should still be visible in light theme
      expect(screen.getByTestId('home-page')).toBeVisible()
      expect(screen.getByTestId('hero-section')).toBeVisible()
      expect(screen.getByTestId('features-section')).toBeVisible()
    })
  })
})
