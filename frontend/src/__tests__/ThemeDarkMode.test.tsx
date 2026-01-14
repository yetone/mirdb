import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor, act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter } from 'react-router-dom'
import Home from '../pages/Home'
import { ThemeProvider, useTheme, type Theme, type ThemePreference } from '../contexts/ThemeContext'

// Helper to render with ThemeProvider
const renderWithTheme = (
  component: React.ReactElement,
  defaultTheme?: ThemePreference
) => {
  return render(
    <ThemeProvider defaultTheme={defaultTheme}>
      <BrowserRouter>{component}</BrowserRouter>
    </ThemeProvider>
  )
}

// Test component to control and verify theme
const ThemeTestController = ({ onThemeChange }: { onThemeChange?: (theme: Theme) => void }) => {
  const { theme, themePreference, setThemePreference, isDarkMode } = useTheme()

  return (
    <div>
      <span data-testid="current-theme">{theme}</span>
      <span data-testid="theme-preference">{themePreference}</span>
      <span data-testid="is-dark-mode">{isDarkMode ? 'true' : 'false'}</span>
      <button
        data-testid="set-dark"
        onClick={() => {
          setThemePreference('dark')
          onThemeChange?.('dark')
        }}
      >
        Set Dark
      </button>
      <button
        data-testid="set-light"
        onClick={() => {
          setThemePreference('light')
          onThemeChange?.('light')
        }}
      >
        Set Light
      </button>
      <button
        data-testid="set-system"
        onClick={() => setThemePreference('system')}
      >
        Set System
      </button>
    </div>
  )
}

describe('Theme Toggle - Dark Mode Support (REQ-9, US-6)', () => {
  let originalMatchMedia: typeof window.matchMedia

  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear()

    // Store original matchMedia
    originalMatchMedia = window.matchMedia

    // Default mock - light mode system preference
    window.matchMedia = vi.fn().mockImplementation((query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }))
  })

  afterEach(() => {
    window.matchMedia = originalMatchMedia
    localStorage.clear()
    // Reset document theme attribute
    document.documentElement.removeAttribute('data-theme')
  })

  // Test Case 1: Set ThemeContext to dark mode and render homepage - Background color changes
  describe('Test Case 1: Background color changes to dark theme color', () => {
    it('should set data-theme attribute to dark when dark mode is set', async () => {
      const user = userEvent.setup()

      render(
        <ThemeProvider defaultTheme="light">
          <BrowserRouter>
            <ThemeTestController />
            <Home />
          </BrowserRouter>
        </ThemeProvider>
      )

      // Initially light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Click dark mode button
      const darkButton = screen.getByTestId('set-dark')
      await user.click(darkButton)

      // Verify theme changes to dark
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })
    })

    it('should render homepage with dark theme when ThemeContext is set to dark', () => {
      renderWithTheme(<Home />, 'dark')

      // Verify the document has dark theme applied
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Verify homepage sections are rendered
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })

    it('homepage sections use DaisyUI base colors that adapt to dark theme', () => {
      renderWithTheme(<Home />, 'dark')

      const heroSection = screen.getByTestId('hero-section')
      // Hero section uses bg-gradient-to-br from-base-200 to-base-300 which adapts to theme
      expect(heroSection.className).toMatch(/bg-gradient/)
      expect(heroSection.className).toMatch(/base-200|base-300/)
    })
  })

  // Test Case 2: Text color provides sufficient contrast (WCAG AA)
  describe('Test Case 2: Text color provides sufficient contrast (WCAG AA)', () => {
    it('should use text-base-content class for primary text in dark mode', () => {
      renderWithTheme(<Home />, 'dark')

      // The headline uses text-base-content which provides proper contrast in DaisyUI themes
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline.className).toMatch(/text-base-content/)
    })

    it('should use appropriate opacity classes for secondary text in dark mode', () => {
      renderWithTheme(<Home />, 'dark')

      // Subheadline and secondary text use text-base-content/70 for proper contrast
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline.className).toMatch(/text-base-content/)
    })

    it('all sections maintain readable text classes in dark mode', () => {
      renderWithTheme(<Home />, 'dark')

      // Verify DaisyUI semantic color classes are used throughout
      // These classes automatically provide WCAG AA compliant contrast
      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBeGreaterThan(0)

      // Each card uses text-base-content variants for text
      featureCards.forEach((card) => {
        const cardText = card.querySelectorAll('[class*="text-base-content"]')
        expect(cardText.length).toBeGreaterThan(0)
      })
    })
  })

  // Test Case 3: CTA buttons remain visible and properly styled
  describe('Test Case 3: CTA buttons remain visible and properly styled', () => {
    it('should render CTA buttons with proper DaisyUI button classes in dark mode', () => {
      renderWithTheme(<Home />, 'dark')

      const getStartedButton = screen.getByTestId('cta-get-started')
      const loginButton = screen.getByTestId('cta-login')

      // Primary button should have btn-primary class
      expect(getStartedButton).toHaveClass('btn', 'btn-primary')

      // Secondary button should have btn-outline class
      expect(loginButton).toHaveClass('btn', 'btn-outline')
    })

    it('CTA buttons should be in the document and accessible in dark mode', () => {
      renderWithTheme(<Home />, 'dark')

      const getStartedButton = screen.getByTestId('cta-get-started')
      const loginButton = screen.getByTestId('cta-login')

      // Buttons should be in the document
      expect(getStartedButton).toBeInTheDocument()
      expect(loginButton).toBeInTheDocument()

      // Buttons should be clickable (not disabled)
      expect(getStartedButton).not.toBeDisabled()
      expect(loginButton).not.toBeDisabled()

      // Buttons should have accessible navigation targets
      expect(getStartedButton).toHaveAttribute('href', '/register')
      expect(loginButton).toHaveAttribute('href', '/login')
    })

    it('demo section submit button remains properly styled in dark mode', () => {
      renderWithTheme(<Home />, 'dark')

      const submitButton = screen.getByTestId('demo-submit-button')
      expect(submitButton).toHaveClass('btn', 'btn-primary')
      expect(submitButton).toBeInTheDocument()
      expect(submitButton).not.toBeDisabled()
    })
  })

  // Test Case 4: All homepage sections transition smoothly to new theme
  describe('Test Case 4: All homepage sections transition smoothly to new theme', () => {
    it('should toggle from light to dark mode and update all sections', async () => {
      const user = userEvent.setup()

      render(
        <ThemeProvider defaultTheme="light">
          <BrowserRouter>
            <ThemeTestController />
            <Home />
          </BrowserRouter>
        </ThemeProvider>
      )

      // Initially light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // All sections should be present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()

      // Toggle to dark mode
      const darkButton = screen.getByTestId('set-dark')
      await user.click(darkButton)

      // Theme should now be dark
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })

      // All sections should still be present and rendered
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('should toggle back from dark to light mode', async () => {
      const user = userEvent.setup()

      render(
        <ThemeProvider defaultTheme="dark">
          <BrowserRouter>
            <ThemeTestController />
            <Home />
          </BrowserRouter>
        </ThemeProvider>
      )

      // Initially dark
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Toggle to light mode
      const lightButton = screen.getByTestId('set-light')
      await user.click(lightButton)

      // Theme should now be light
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })
    })

    it('ThemeProvider updates isDarkMode flag when switching themes', async () => {
      const user = userEvent.setup()

      render(
        <ThemeProvider defaultTheme="light">
          <BrowserRouter>
            <ThemeTestController />
          </BrowserRouter>
        </ThemeProvider>
      )

      // Initially not dark mode
      expect(screen.getByTestId('is-dark-mode').textContent).toBe('false')

      // Toggle to dark mode
      await user.click(screen.getByTestId('set-dark'))

      await waitFor(() => {
        expect(screen.getByTestId('is-dark-mode').textContent).toBe('true')
      })

      // Toggle back to light
      await user.click(screen.getByTestId('set-light'))

      await waitFor(() => {
        expect(screen.getByTestId('is-dark-mode').textContent).toBe('false')
      })
    })

    it('theme preference persists to localStorage', async () => {
      const user = userEvent.setup()

      render(
        <ThemeProvider defaultTheme="light">
          <BrowserRouter>
            <ThemeTestController />
          </BrowserRouter>
        </ThemeProvider>
      )

      // Toggle to dark mode
      await user.click(screen.getByTestId('set-dark'))

      await waitFor(() => {
        expect(localStorage.getItem('theme-preference')).toBe('dark')
      })
    })
  })

  // Test Case 5: Homepage respects system dark mode preference
  describe('Test Case 5: Homepage respects system dark mode preference', () => {
    it('should detect system dark mode preference when set to system', () => {
      // Mock system preference as dark
      window.matchMedia = vi.fn().mockImplementation((query: string) => ({
        matches: query === '(prefers-color-scheme: dark)',
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }))

      render(
        <ThemeProvider defaultTheme="system">
          <BrowserRouter>
            <ThemeTestController />
            <Home />
          </BrowserRouter>
        </ThemeProvider>
      )

      // Theme should be dark since system preference is dark
      expect(screen.getByTestId('current-theme').textContent).toBe('dark')
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('should detect system light mode preference when set to system', () => {
      // Mock system preference as light
      window.matchMedia = vi.fn().mockImplementation((query: string) => ({
        matches: false, // Not dark mode
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }))

      render(
        <ThemeProvider defaultTheme="system">
          <BrowserRouter>
            <ThemeTestController />
            <Home />
          </BrowserRouter>
        </ThemeProvider>
      )

      // Theme should be light since system preference is light
      expect(screen.getByTestId('current-theme').textContent).toBe('light')
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })

    it('should respond to system preference changes dynamically', async () => {
      let mediaQueryCallback: ((e: MediaQueryListEvent) => void) | null = null

      // Mock matchMedia with event listener capture
      window.matchMedia = vi.fn().mockImplementation((query: string) => ({
        matches: false, // Initially light
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn((event: string, callback: (e: MediaQueryListEvent) => void) => {
          if (event === 'change') {
            mediaQueryCallback = callback
          }
        }),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }))

      render(
        <ThemeProvider defaultTheme="system">
          <BrowserRouter>
            <ThemeTestController />
            <Home />
          </BrowserRouter>
        </ThemeProvider>
      )

      // Initially light
      expect(screen.getByTestId('current-theme').textContent).toBe('light')

      // Simulate system theme change to dark
      if (mediaQueryCallback) {
        act(() => {
          mediaQueryCallback!({ matches: true } as MediaQueryListEvent)
        })
      }

      await waitFor(() => {
        expect(screen.getByTestId('current-theme').textContent).toBe('dark')
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
      })
    })

    it('homepage renders correctly with system dark mode preference', () => {
      // Mock system preference as dark
      window.matchMedia = vi.fn().mockImplementation((query: string) => ({
        matches: query === '(prefers-color-scheme: dark)',
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }))

      render(
        <ThemeProvider defaultTheme="system">
          <BrowserRouter>
            <Home />
          </BrowserRouter>
        </ThemeProvider>
      )

      // All homepage sections should render
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()

      // Theme should be applied
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })
  })

  // Additional integration tests for theme context
  describe('ThemeContext Integration', () => {
    it('throws error when useTheme is used outside ThemeProvider', () => {
      // Suppress console.error for this test
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

      const TestComponent = () => {
        useTheme()
        return null
      }

      expect(() => render(<TestComponent />)).toThrow(
        'useTheme must be used within a ThemeProvider'
      )

      consoleSpy.mockRestore()
    })

    it('cyberpunk theme is recognized as dark mode', () => {
      render(
        <ThemeProvider defaultTheme="cyberpunk">
          <BrowserRouter>
            <ThemeTestController />
          </BrowserRouter>
        </ThemeProvider>
      )

      expect(screen.getByTestId('current-theme').textContent).toBe('cyberpunk')
      expect(screen.getByTestId('is-dark-mode').textContent).toBe('true')
    })

    it('synthwave theme is recognized as dark mode', () => {
      render(
        <ThemeProvider defaultTheme="synthwave">
          <BrowserRouter>
            <ThemeTestController />
          </BrowserRouter>
        </ThemeProvider>
      )

      expect(screen.getByTestId('current-theme').textContent).toBe('synthwave')
      expect(screen.getByTestId('is-dark-mode').textContent).toBe('true')
    })

    it('light theme is not recognized as dark mode', () => {
      render(
        <ThemeProvider defaultTheme="light">
          <BrowserRouter>
            <ThemeTestController />
          </BrowserRouter>
        </ThemeProvider>
      )

      expect(screen.getByTestId('current-theme').textContent).toBe('light')
      expect(screen.getByTestId('is-dark-mode').textContent).toBe('false')
    })
  })
})
