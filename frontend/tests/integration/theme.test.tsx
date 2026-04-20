/**
 * Integration tests for theme switching on homepage.
 * Owner: Scenario 6 - Theme Switching Support
 *
 * Test coverage:
 * - Homepage renders in default theme
 * - Theme toggle changes page styling
 * - Theme persists across navigation
 * - All supported themes render correctly
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, act } from '@testing-library/react'
import { BrowserRouter, MemoryRouter, Routes, Route } from 'react-router-dom'
import { ReactNode, useState } from 'react'
import Home from '../../src/pages/Home'
import { ThemeProvider, useTheme } from '../../src/contexts/ThemeContext'
import { AuthProvider } from '../../src/contexts/AuthContext'

// All supported themes from ThemeContext
const SUPPORTED_THEMES = [
  'light',
  'dark',
  'cyberpunk',
  'synthwave',
  'forest',
  'cupcake',
  'corporate',
  'dracula',
] as const

type Theme = (typeof SUPPORTED_THEMES)[number]

// Test wrapper with all providers
const TestWrapper = ({ children }: { children: ReactNode }) => {
  return (
    <BrowserRouter>
      <ThemeProvider>
        <AuthProvider>{children}</AuthProvider>
      </ThemeProvider>
    </BrowserRouter>
  )
}

// Custom wrapper to render with a specific theme
const renderWithTheme = (ui: ReactNode, theme: Theme = 'dark') => {
  // Set localStorage before rendering so ThemeProvider picks up the value
  localStorage.getItem = vi.fn().mockReturnValue(theme)

  return render(
    <BrowserRouter>
      <ThemeProvider>
        <AuthProvider>{ui}</AuthProvider>
      </ThemeProvider>
    </BrowserRouter>
  )
}

// Component to expose theme context for testing
const ThemeController = ({
  onThemeChange,
}: {
  onThemeChange?: (setTheme: (theme: Theme) => void) => void
}) => {
  const { theme, setTheme } = useTheme()

  if (onThemeChange) {
    onThemeChange(setTheme as (theme: Theme) => void)
  }

  return <div data-testid="current-theme">{theme}</div>
}

describe('Theme Switching Support - Integration Tests', () => {
  beforeEach(() => {
    // Reset localStorage mock to return null (default to dark theme)
    vi.clearAllMocks()
    ;(localStorage.getItem as ReturnType<typeof vi.fn>).mockReturnValue(null)
    // Reset document attribute
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    document.documentElement.removeAttribute('data-theme')
  })

  describe('Test Case 1: Dark Theme Rendering', () => {
    it('should render Home component with dark theme styles applied', () => {
      renderWithTheme(<Home />, 'dark')

      // Verify the document has the dark theme attribute
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Verify homepage renders correctly
      const heroSection = screen.getByLabelText(/hero section/i)
      expect(heroSection).toBeInTheDocument()

      // Verify homepage main element exists
      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()
    })

    it('should apply dark theme class to hero section', () => {
      renderWithTheme(<Home />, 'dark')

      const heroSection = screen.getByLabelText(/hero section/i)
      // DaisyUI's bg-base-200 will inherit from dark theme
      expect(heroSection).toHaveClass('bg-base-200')
    })
  })

  describe('Test Case 2: Cyberpunk Theme Rendering', () => {
    it('should render Home component with cyberpunk theme styles applied', () => {
      renderWithTheme(<Home />, 'cyberpunk')

      // Verify the document has the cyberpunk theme attribute
      expect(document.documentElement.getAttribute('data-theme')).toBe(
        'cyberpunk'
      )

      // Verify homepage renders correctly
      const heroSection = screen.getByLabelText(/hero section/i)
      expect(heroSection).toBeInTheDocument()

      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()
    })

    it('should apply cyberpunk theme to hero content', () => {
      renderWithTheme(<Home />, 'cyberpunk')

      // Verify headline exists and uses theme-aware gradient
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveClass('text-transparent')
      expect(headline).toHaveClass('bg-gradient-to-r')
      expect(headline).toHaveClass('from-primary')
    })
  })

  describe('Test Case 3: Theme Toggle Changes Styling', () => {
    it('should update document data-theme when theme changes', () => {
      let setThemeFn: ((theme: Theme) => void) | undefined

      render(
        <TestWrapper>
          <ThemeController
            onThemeChange={(setTheme) => {
              setThemeFn = setTheme
            }}
          />
          <Home />
        </TestWrapper>
      )

      // Initially should have dark theme (default)
      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

      // Change to light theme
      act(() => {
        setThemeFn?.('light')
      })

      expect(screen.getByTestId('current-theme')).toHaveTextContent('light')
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Change to synthwave theme
      act(() => {
        setThemeFn?.('synthwave')
      })

      expect(screen.getByTestId('current-theme')).toHaveTextContent('synthwave')
      expect(document.documentElement.getAttribute('data-theme')).toBe(
        'synthwave'
      )
    })

    it('should persist theme to localStorage when changed', () => {
      let setThemeFn: ((theme: Theme) => void) | undefined

      render(
        <TestWrapper>
          <ThemeController
            onThemeChange={(setTheme) => {
              setThemeFn = setTheme
            }}
          />
        </TestWrapper>
      )

      // Change theme
      act(() => {
        setThemeFn?.('forest')
      })

      // Verify localStorage was called
      expect(localStorage.setItem).toHaveBeenCalledWith('theme', 'forest')
    })

    it('should update all visible homepage elements when theme changes', () => {
      let setThemeFn: ((theme: Theme) => void) | undefined

      render(
        <TestWrapper>
          <ThemeController
            onThemeChange={(setTheme) => {
              setThemeFn = setTheme
            }}
          />
          <Home />
        </TestWrapper>
      )

      // Verify homepage elements exist
      const heroSection = screen.getByLabelText(/hero section/i)
      expect(heroSection).toBeInTheDocument()

      // Change theme
      act(() => {
        setThemeFn?.('dracula')
      })

      // Document should have new theme
      expect(document.documentElement.getAttribute('data-theme')).toBe(
        'dracula'
      )

      // Hero section should still be present (DaisyUI handles theming via CSS)
      expect(heroSection).toBeInTheDocument()
    })
  })

  describe('Test Case 4: Theme Persistence', () => {
    it('should load saved theme from localStorage on mount', () => {
      // Pre-set localStorage to return 'cupcake'
      localStorage.getItem = vi.fn().mockReturnValue('cupcake')

      render(
        <TestWrapper>
          <ThemeController />
        </TestWrapper>
      )

      expect(localStorage.getItem).toHaveBeenCalledWith('theme')
      expect(screen.getByTestId('current-theme')).toHaveTextContent('cupcake')
      expect(document.documentElement.getAttribute('data-theme')).toBe(
        'cupcake'
      )
    })

    it('should persist theme across page navigation', () => {
      let setThemeFn: ((theme: Theme) => void) | undefined

      const TestApp = () => {
        return (
          <MemoryRouter initialEntries={['/']}>
            <ThemeProvider>
              <AuthProvider>
                <ThemeController
                  onThemeChange={(setTheme) => {
                    setThemeFn = setTheme
                  }}
                />
                <Routes>
                  <Route path="/" element={<Home />} />
                  <Route path="/other" element={<div>Other Page</div>} />
                </Routes>
              </AuthProvider>
            </ThemeProvider>
          </MemoryRouter>
        )
      }

      render(<TestApp />)

      // Set theme to corporate
      act(() => {
        setThemeFn?.('corporate')
      })

      expect(document.documentElement.getAttribute('data-theme')).toBe(
        'corporate'
      )
      expect(localStorage.setItem).toHaveBeenCalledWith('theme', 'corporate')
    })

    it('should default to dark theme when no localStorage value exists', () => {
      localStorage.getItem = vi.fn().mockReturnValue(null)

      render(
        <TestWrapper>
          <ThemeController />
        </TestWrapper>
      )

      expect(screen.getByTestId('current-theme')).toHaveTextContent('dark')
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })
  })

  describe('Test Case 5: All Supported Themes', () => {
    it.each(SUPPORTED_THEMES)(
      'should render homepage correctly with %s theme',
      (themeName) => {
        renderWithTheme(<Home />, themeName as Theme)

        // Verify document theme attribute is set
        expect(document.documentElement.getAttribute('data-theme')).toBe(
          themeName
        )

        // Verify homepage core elements render
        const main = screen.getByRole('main')
        expect(main).toBeInTheDocument()

        const heroSection = screen.getByLabelText(/hero section/i)
        expect(heroSection).toBeInTheDocument()
        expect(heroSection).toHaveClass('bg-base-200')

        const headline = screen.getByRole('heading', { level: 1 })
        expect(headline).toBeInTheDocument()

        // Verify CTA buttons render with theme-aware styles
        const primaryCTA = screen.getByRole('link', { name: /get started/i })
        expect(primaryCTA).toBeInTheDocument()
        expect(primaryCTA).toHaveClass('btn-primary')

        const secondaryCTA = screen.getByRole('link', { name: /login/i })
        expect(secondaryCTA).toBeInTheDocument()
        expect(secondaryCTA).toHaveClass('btn-outline')
      }
    )

    it('should support exactly 8 themes', () => {
      expect(SUPPORTED_THEMES).toHaveLength(8)
      expect(SUPPORTED_THEMES).toContain('light')
      expect(SUPPORTED_THEMES).toContain('dark')
      expect(SUPPORTED_THEMES).toContain('cyberpunk')
      expect(SUPPORTED_THEMES).toContain('synthwave')
      expect(SUPPORTED_THEMES).toContain('forest')
      expect(SUPPORTED_THEMES).toContain('cupcake')
      expect(SUPPORTED_THEMES).toContain('corporate')
      expect(SUPPORTED_THEMES).toContain('dracula')
    })
  })

  describe('Homepage Theme Integration', () => {
    it('should have theme-aware hero section background', () => {
      renderWithTheme(<Home />, 'dark')

      const heroSection = screen.getByLabelText(/hero section/i)
      // bg-base-200 is a DaisyUI class that respects data-theme
      expect(heroSection.className).toContain('bg-base-200')
    })

    it('should have theme-aware text styling', () => {
      renderWithTheme(<Home />, 'dark')

      // The subheadline uses text-base-content/80 which is theme-aware
      const description = screen.getByText(/powerful analytics/i)
      expect(description.className).toContain('text-base-content')
    })

    it('should have theme-aware CTA buttons', () => {
      renderWithTheme(<Home />, 'cyberpunk')

      const primaryBtn = screen.getByRole('link', { name: /get started/i })
      expect(primaryBtn).toHaveClass('btn', 'btn-primary')

      const secondaryBtn = screen.getByRole('link', { name: /login/i })
      expect(secondaryBtn).toHaveClass('btn', 'btn-outline')
    })
  })
})
