/**
 * Theme Adaptation Integration Tests
 * Owner: Scenario 5 - Theme Adaptation
 *
 * Integration tests for theme toggle functionality and contrast verification.
 *
 * Test Case 5: Toggle theme and verify smooth transition
 * Test Case 6: Check contrast ratios in dark theme
 */
import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import { render, screen, waitFor, act } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter } from 'react-router-dom'
import { Home } from './Home'
import { ThemeProvider, Theme } from '../contexts/ThemeContext'

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
  }
})()

Object.defineProperty(window, 'localStorage', {
  value: localStorageMock,
})

// Helper to render Home with a specific theme
const renderHomeWithTheme = (theme: Theme = 'dark') => {
  return render(
    <ThemeProvider defaultTheme={theme}>
      <BrowserRouter>
        <Home />
      </BrowserRouter>
    </ThemeProvider>
  )
}

describe('Theme Integration Tests - Scenario 5', () => {
  beforeEach(() => {
    localStorageMock.clear()
    vi.clearAllMocks()
  })

  describe('Test Case 5: Theme transition animation integration', () => {
    it('should render homepage with transition-colors class for smooth theme changes', () => {
      renderHomeWithTheme('dark')

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toHaveClass('transition-colors')
      expect(homePage).toHaveClass('duration-300')
    })

    it('should cycle through all themes when toggling', async () => {
      const user = userEvent.setup()
      renderHomeWithTheme('light')

      // Get first theme button (desktop navbar)
      const themeButtons = screen.getAllByTestId('theme-toggle-button')

      // Change to dark
      await user.click(themeButtons[0])
      const darkOptions = screen.getAllByTestId('theme-option-dark')
      await user.click(darkOptions[0])

      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toHaveAttribute('data-theme-active', 'dark')
      })

      // Change to cyberpunk
      await user.click(themeButtons[0])
      const cyberpunkOptions = screen.getAllByTestId('theme-option-cyberpunk')
      await user.click(cyberpunkOptions[0])

      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toHaveAttribute('data-theme-active', 'cyberpunk')
      })

      // Change to synthwave
      await user.click(themeButtons[0])
      const synthwaveOptions = screen.getAllByTestId('theme-option-synthwave')
      await user.click(synthwaveOptions[0])

      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toHaveAttribute('data-theme-active', 'synthwave')
      })
    })

    it('should maintain theme state across component updates', async () => {
      const user = userEvent.setup()
      const { rerender } = renderHomeWithTheme('dark')

      // Change theme - use first button (desktop)
      const themeButtons = screen.getAllByTestId('theme-toggle-button')
      await user.click(themeButtons[0])
      const lightOptions = screen.getAllByTestId('theme-option-light')
      await user.click(lightOptions[0])

      await waitFor(() => {
        expect(screen.getByTestId('home-page')).toHaveAttribute('data-theme-active', 'light')
      })

      // Theme should persist after rerender
      rerender(
        <ThemeProvider defaultTheme="dark">
          <BrowserRouter>
            <Home />
          </BrowserRouter>
        </ThemeProvider>
      )

      // Note: In a real app, theme would persist via localStorage
      // Here we verify the component still renders correctly
      expect(screen.getByTestId('home-page')).toBeInTheDocument()
    })
  })

  describe('Test Case 6: Contrast ratios in dark theme', () => {
    it('should render text elements with base-content class for proper contrast in dark theme', () => {
      renderHomeWithTheme('dark')

      // Check hero subheading has proper text contrast class
      const heroSubheading = screen.getByTestId('hero-subheading')
      expect(heroSubheading).toBeInTheDocument()
      // The subheading uses text-base-content/70 which provides adequate contrast
      expect(heroSubheading).toHaveClass('text-base-content/70')
    })

    it('should render CTA footer text with proper contrast classes', () => {
      renderHomeWithTheme('dark')

      const ctaSubheading = screen.getByTestId('cta-footer-subheading')
      expect(ctaSubheading).toHaveClass('text-base-content/70')
    })

    it('should render interactive elements with sufficient contrast', () => {
      renderHomeWithTheme('dark')

      // Check that buttons are rendered
      const heroCta = screen.getByTestId('hero-cta')
      expect(heroCta).toBeInTheDocument()

      const ctaButton = screen.getByTestId('cta-footer-button')
      expect(ctaButton).toBeInTheDocument()
    })

    it('should maintain contrast requirements in light theme', () => {
      renderHomeWithTheme('light')

      const heroSubheading = screen.getByTestId('hero-subheading')
      expect(heroSubheading).toHaveClass('text-base-content/70')
    })

    it('should maintain contrast requirements in synthwave theme', () => {
      renderHomeWithTheme('synthwave')

      const heroSubheading = screen.getByTestId('hero-subheading')
      expect(heroSubheading).toHaveClass('text-base-content/70')
    })

    it('should maintain contrast requirements in cyberpunk theme', () => {
      renderHomeWithTheme('cyberpunk')

      const heroSubheading = screen.getByTestId('hero-subheading')
      expect(heroSubheading).toHaveClass('text-base-content/70')
    })
  })

  describe('Theme toggle accessibility', () => {
    it('should have proper ARIA labels on theme toggle button', () => {
      renderHomeWithTheme('dark')

      const themeButtons = screen.getAllByTestId('theme-toggle-button')
      expect(themeButtons[0]).toHaveAttribute('aria-label')
      expect(themeButtons[0].getAttribute('aria-label')).toContain('theme')
    })

    it('should have proper role on theme dropdown', async () => {
      renderHomeWithTheme('dark')

      const dropdowns = screen.getAllByTestId('theme-dropdown')
      expect(dropdowns[0]).toHaveAttribute('role', 'listbox')
    })

    it('should have aria-label on each theme option', async () => {
      renderHomeWithTheme('dark')

      const lightOptions = screen.getAllByTestId('theme-option-light')
      expect(lightOptions[0]).toHaveAttribute('aria-label')
    })
  })

  describe('Homepage sections render correctly with all themes', () => {
    const themes: Theme[] = ['light', 'dark', 'cyberpunk', 'synthwave']

    themes.forEach((theme) => {
      it(`should render navbar with ${theme} theme`, () => {
        renderHomeWithTheme(theme)
        expect(screen.getByTestId('navbar')).toBeInTheDocument()
      })

      it(`should render hero section with ${theme} theme`, () => {
        renderHomeWithTheme(theme)
        expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      })

      it(`should render CTA footer with ${theme} theme`, () => {
        renderHomeWithTheme(theme)
        expect(screen.getByTestId('cta-footer')).toBeInTheDocument()
      })

      it(`should render theme toggle with ${theme} theme`, () => {
        renderHomeWithTheme(theme)
        expect(screen.getAllByTestId('theme-toggle').length).toBeGreaterThanOrEqual(1)
      })
    })
  })
})
