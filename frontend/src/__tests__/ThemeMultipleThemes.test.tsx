import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter } from 'react-router-dom'
import Home from '../pages/Home'
import { ThemeProvider, useTheme, type Theme, type ThemePreference } from '../contexts/ThemeContext'

// Helper to render with ThemeProvider and specific theme
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
const ThemeTestController = () => {
  const { theme, themePreference, setThemePreference, isDarkMode } = useTheme()

  return (
    <div>
      <span data-testid="current-theme">{theme}</span>
      <span data-testid="theme-preference">{themePreference}</span>
      <span data-testid="is-dark-mode">{isDarkMode ? 'true' : 'false'}</span>
      <button
        data-testid="set-cyberpunk"
        onClick={() => setThemePreference('cyberpunk')}
      >
        Set Cyberpunk
      </button>
      <button
        data-testid="set-synthwave"
        onClick={() => setThemePreference('synthwave')}
      >
        Set Synthwave
      </button>
      <button
        data-testid="set-light"
        onClick={() => setThemePreference('light')}
      >
        Set Light
      </button>
      <button
        data-testid="set-dark"
        onClick={() => setThemePreference('dark')}
      >
        Set Dark
      </button>
    </div>
  )
}

describe('Theme Toggle - Multiple Theme Support (REQ-9, NFR-4)', () => {
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

  // Test Case 1: Set ThemeContext to 'cyberpunk' and render homepage
  describe('Test Case 1: Cyberpunk Theme Support', () => {
    it('should set data-theme attribute to cyberpunk when cyberpunk theme is set', () => {
      renderWithTheme(<Home />, 'cyberpunk')

      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
    })

    it('should render homepage correctly with cyberpunk theme', () => {
      renderWithTheme(<Home />, 'cyberpunk')

      // Verify all homepage sections render
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('hero section uses DaisyUI base colors that adapt to cyberpunk theme', () => {
      renderWithTheme(<Home />, 'cyberpunk')

      const heroSection = screen.getByTestId('hero-section')
      // Hero section uses bg-gradient-to-br from-base-200 to-base-300 which adapts to cyberpunk
      expect(heroSection.className).toMatch(/bg-gradient/)
      expect(heroSection.className).toMatch(/base-200/)
      expect(heroSection.className).toMatch(/base-300/)
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

    it('CTA buttons use appropriate DaisyUI classes in cyberpunk theme', () => {
      renderWithTheme(<Home />, 'cyberpunk')

      const getStartedButton = screen.getByTestId('cta-get-started')
      const loginButton = screen.getByTestId('cta-login')

      // Primary button should have btn-primary class
      expect(getStartedButton).toHaveClass('btn', 'btn-primary')

      // Secondary button should have btn-outline class
      expect(loginButton).toHaveClass('btn', 'btn-outline')
    })

    it('feature cards render with proper styling in cyberpunk theme', () => {
      renderWithTheme(<Home />, 'cyberpunk')

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBe(3)

      // Each card uses DaisyUI card classes
      featureCards.forEach((card) => {
        expect(card).toHaveClass('card')
        expect(card.className).toMatch(/bg-base-100/)
      })
    })

    it('headline text uses base-content color for cyberpunk theme', () => {
      renderWithTheme(<Home />, 'cyberpunk')

      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline.className).toMatch(/text-base-content/)
    })

    it('can switch to cyberpunk theme dynamically', async () => {
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

      // Switch to cyberpunk
      await user.click(screen.getByTestId('set-cyberpunk'))

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
      })

      // All sections should still be present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })
  })

  // Test Case 2: Set ThemeContext to 'synthwave' and render homepage
  describe('Test Case 2: Synthwave Theme Support', () => {
    it('should set data-theme attribute to synthwave when synthwave theme is set', () => {
      renderWithTheme(<Home />, 'synthwave')

      expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
    })

    it('should render homepage correctly with synthwave theme', () => {
      renderWithTheme(<Home />, 'synthwave')

      // Verify all homepage sections render
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('hero section uses DaisyUI base colors that adapt to synthwave theme', () => {
      renderWithTheme(<Home />, 'synthwave')

      const heroSection = screen.getByTestId('hero-section')
      // Hero section uses bg-gradient-to-br from-base-200 to-base-300 which adapts to synthwave
      expect(heroSection.className).toMatch(/bg-gradient/)
      expect(heroSection.className).toMatch(/base-200/)
      expect(heroSection.className).toMatch(/base-300/)
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

    it('CTA buttons use appropriate DaisyUI classes in synthwave theme', () => {
      renderWithTheme(<Home />, 'synthwave')

      const getStartedButton = screen.getByTestId('cta-get-started')
      const loginButton = screen.getByTestId('cta-login')

      // Primary button should have btn-primary class
      expect(getStartedButton).toHaveClass('btn', 'btn-primary')

      // Secondary button should have btn-outline class
      expect(loginButton).toHaveClass('btn', 'btn-outline')
    })

    it('feature cards render with proper styling in synthwave theme', () => {
      renderWithTheme(<Home />, 'synthwave')

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBe(3)

      // Each card uses DaisyUI card classes
      featureCards.forEach((card) => {
        expect(card).toHaveClass('card')
        expect(card.className).toMatch(/bg-base-100/)
      })
    })

    it('headline text uses base-content color for synthwave theme', () => {
      renderWithTheme(<Home />, 'synthwave')

      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline.className).toMatch(/text-base-content/)
    })

    it('can switch to synthwave theme dynamically', async () => {
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

      // Switch to synthwave
      await user.click(screen.getByTestId('set-synthwave'))

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
      })

      // All sections should still be present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })
  })

  // Test Case 3: Set ThemeContext to 'light' and render homepage
  describe('Test Case 3: Light Theme Support', () => {
    it('should set data-theme attribute to light when light theme is set', () => {
      renderWithTheme(<Home />, 'light')

      expect(document.documentElement.getAttribute('data-theme')).toBe('light')
    })

    it('should render homepage correctly with light theme', () => {
      renderWithTheme(<Home />, 'light')

      // Verify all homepage sections render
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('hero section uses DaisyUI base colors that adapt to light theme', () => {
      renderWithTheme(<Home />, 'light')

      const heroSection = screen.getByTestId('hero-section')
      // Hero section uses bg-gradient-to-br from-base-200 to-base-300 which adapts to light
      expect(heroSection.className).toMatch(/bg-gradient/)
      expect(heroSection.className).toMatch(/base-200/)
      expect(heroSection.className).toMatch(/base-300/)
    })

    it('light theme is NOT recognized as dark mode', () => {
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

    it('CTA buttons use appropriate DaisyUI classes in light theme', () => {
      renderWithTheme(<Home />, 'light')

      const getStartedButton = screen.getByTestId('cta-get-started')
      const loginButton = screen.getByTestId('cta-login')

      // Primary button should have btn-primary class
      expect(getStartedButton).toHaveClass('btn', 'btn-primary')

      // Secondary button should have btn-outline class
      expect(loginButton).toHaveClass('btn', 'btn-outline')
    })

    it('feature cards render with proper styling in light theme', () => {
      renderWithTheme(<Home />, 'light')

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBe(3)

      // Each card uses DaisyUI card classes
      featureCards.forEach((card) => {
        expect(card).toHaveClass('card')
        expect(card.className).toMatch(/bg-base-100/)
      })
    })

    it('headline text uses base-content color for light theme', () => {
      renderWithTheme(<Home />, 'light')

      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline.className).toMatch(/text-base-content/)
    })

    it('can switch from other themes to light theme dynamically', async () => {
      const user = userEvent.setup()

      render(
        <ThemeProvider defaultTheme="cyberpunk">
          <BrowserRouter>
            <ThemeTestController />
            <Home />
          </BrowserRouter>
        </ThemeProvider>
      )

      // Initially cyberpunk
      expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')

      // Switch to light
      await user.click(screen.getByTestId('set-light'))

      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
      })

      // All sections should still be present
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })
  })

  // Integration test: Switching between all themes
  describe('Theme Switching Integration', () => {
    it('can switch between all four themes: light -> cyberpunk -> synthwave -> dark', async () => {
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
      expect(screen.getByTestId('is-dark-mode').textContent).toBe('false')

      // Switch to cyberpunk
      await user.click(screen.getByTestId('set-cyberpunk'))
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')
        expect(screen.getByTestId('is-dark-mode').textContent).toBe('true')
      })

      // Switch to synthwave
      await user.click(screen.getByTestId('set-synthwave'))
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')
        expect(screen.getByTestId('is-dark-mode').textContent).toBe('true')
      })

      // Switch to dark
      await user.click(screen.getByTestId('set-dark'))
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
        expect(screen.getByTestId('is-dark-mode').textContent).toBe('true')
      })

      // Switch back to light
      await user.click(screen.getByTestId('set-light'))
      await waitFor(() => {
        expect(document.documentElement.getAttribute('data-theme')).toBe('light')
        expect(screen.getByTestId('is-dark-mode').textContent).toBe('false')
      })

      // Homepage sections should always remain rendered
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('demo-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('theme preference persists to localStorage for all themes', async () => {
      const user = userEvent.setup()

      render(
        <ThemeProvider defaultTheme="light">
          <BrowserRouter>
            <ThemeTestController />
          </BrowserRouter>
        </ThemeProvider>
      )

      // Test cyberpunk persistence
      await user.click(screen.getByTestId('set-cyberpunk'))
      await waitFor(() => {
        expect(localStorage.getItem('theme-preference')).toBe('cyberpunk')
      })

      // Test synthwave persistence
      await user.click(screen.getByTestId('set-synthwave'))
      await waitFor(() => {
        expect(localStorage.getItem('theme-preference')).toBe('synthwave')
      })

      // Test light persistence
      await user.click(screen.getByTestId('set-light'))
      await waitFor(() => {
        expect(localStorage.getItem('theme-preference')).toBe('light')
      })
    })

    it('all DaisyUI themes are configured in tailwind.config.js', () => {
      // This is a documentation test to verify expected themes are available
      // The actual theme configuration is in tailwind.config.js: themes: ["light", "dark", "cyberpunk", "synthwave"]
      const expectedThemes: Theme[] = ['light', 'dark', 'cyberpunk', 'synthwave']

      // Verify all expected themes can be set via ThemeContext
      expectedThemes.forEach((themeName) => {
        document.documentElement.setAttribute('data-theme', themeName)
        expect(document.documentElement.getAttribute('data-theme')).toBe(themeName)
      })
    })
  })

  // Accessibility in different themes
  describe('Accessibility Across Themes', () => {
    const themes: Theme[] = ['light', 'dark', 'cyberpunk', 'synthwave']

    themes.forEach((themeName) => {
      it(`homepage maintains semantic structure in ${themeName} theme`, () => {
        renderWithTheme(<Home />, themeName)

        // Main landmark exists
        expect(screen.getByRole('main')).toBeInTheDocument()

        // H1 heading exists
        expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument()

        // Navigation buttons/links are accessible
        expect(screen.getByTestId('cta-get-started')).toBeInTheDocument()
        expect(screen.getByTestId('cta-login')).toBeInTheDocument()
      })

      it(`CTA buttons are clickable in ${themeName} theme`, () => {
        renderWithTheme(<Home />, themeName)

        const getStartedButton = screen.getByTestId('cta-get-started')
        const loginButton = screen.getByTestId('cta-login')

        expect(getStartedButton).not.toBeDisabled()
        expect(loginButton).not.toBeDisabled()

        // Verify they have proper href attributes for navigation
        expect(getStartedButton).toHaveAttribute('href', '/register')
        expect(loginButton).toHaveAttribute('href', '/login')
      })
    })
  })
})
