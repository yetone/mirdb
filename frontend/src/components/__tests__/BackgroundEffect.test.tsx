import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import BackgroundEffect from '../BackgroundEffect'
import Home from '../../pages/Home'
import { ThemeProvider, type ThemePreference } from '../../contexts/ThemeContext'

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

describe('BackgroundEffect Component', () => {
  let originalMatchMedia: typeof window.matchMedia

  beforeEach(() => {
    localStorage.clear()
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
    document.documentElement.removeAttribute('data-theme')
  })

  // Test Case 1: BackgroundEffect component is rendered in the DOM when Home page loads
  describe('Test Case 1: BackgroundEffect component is rendered in the DOM', () => {
    it('should render BackgroundEffect component when Home page loads', () => {
      renderWithTheme(<Home />, 'light')

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toBeInTheDocument()
    })

    it('should render BackgroundEffect with all visual elements', () => {
      renderWithTheme(<BackgroundEffect />, 'light')

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toBeInTheDocument()

      // Check for gradient orbs
      const primaryOrb = screen.getByTestId('background-orb-primary')
      const secondaryOrb = screen.getByTestId('background-orb-secondary')
      const accentOrb = screen.getByTestId('background-orb-accent')
      const noiseOverlay = screen.getByTestId('background-noise')

      expect(primaryOrb).toBeInTheDocument()
      expect(secondaryOrb).toBeInTheDocument()
      expect(accentOrb).toBeInTheDocument()
      expect(noiseOverlay).toBeInTheDocument()
    })

    it('should accept custom data-testid prop', () => {
      renderWithTheme(<BackgroundEffect data-testid="custom-bg" />, 'light')

      const backgroundEffect = screen.getByTestId('custom-bg')
      expect(backgroundEffect).toBeInTheDocument()
    })
  })

  // Test Case 2: Background effects are positioned behind content (negative z-index)
  describe('Test Case 2: Background effects positioned behind content (negative z-index)', () => {
    it('should have z-[-1] class for negative z-index positioning', () => {
      renderWithTheme(<BackgroundEffect />, 'light')

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toHaveClass('z-[-1]')
    })

    it('should have fixed positioning to cover viewport', () => {
      renderWithTheme(<BackgroundEffect />, 'light')

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toHaveClass('fixed')
      expect(backgroundEffect).toHaveClass('inset-0')
    })

    it('should have pointer-events-none to not block interactions', () => {
      renderWithTheme(<BackgroundEffect />, 'light')

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toHaveClass('pointer-events-none')
    })

    it('should be aria-hidden for screen readers', () => {
      renderWithTheme(<BackgroundEffect />, 'light')

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toHaveAttribute('aria-hidden', 'true')
    })

    it('should render behind main content when used in Home page', () => {
      renderWithTheme(<Home />, 'light')

      const backgroundEffect = screen.getByTestId('background-effect')
      const heroSection = screen.getByTestId('hero-section')

      expect(backgroundEffect).toBeInTheDocument()
      expect(heroSection).toBeInTheDocument()

      // BackgroundEffect should have negative z-index
      expect(backgroundEffect).toHaveClass('z-[-1]')
    })
  })

  // Test Case 3: Background effects adapt to dark theme colors
  describe('Test Case 3: Background effects adapt to dark theme colors', () => {
    it('should have data-theme-mode attribute reflecting theme mode', () => {
      renderWithTheme(<BackgroundEffect />, 'dark')

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toHaveAttribute('data-theme-mode', 'dark')
    })

    it('should have data-theme-mode="light" in light mode', () => {
      renderWithTheme(<BackgroundEffect />, 'light')

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toHaveAttribute('data-theme-mode', 'light')
    })

    it('should have data-theme attribute with current theme name', () => {
      renderWithTheme(<BackgroundEffect />, 'dark')

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toHaveAttribute('data-theme', 'dark')
    })

    it('should use dark theme colors when theme is dark', () => {
      renderWithTheme(<BackgroundEffect />, 'dark')

      const primaryOrb = screen.getByTestId('background-orb-primary')
      // Dark theme uses blue-600 colors
      expect(primaryOrb.className).toMatch(/blue-600/)
    })

    it('should use light theme colors when theme is light', () => {
      renderWithTheme(<BackgroundEffect />, 'light')

      const primaryOrb = screen.getByTestId('background-orb-primary')
      // Light theme uses blue-400 colors
      expect(primaryOrb.className).toMatch(/blue-400/)
    })

    it('should use cyberpunk theme colors when theme is cyberpunk', () => {
      renderWithTheme(<BackgroundEffect />, 'cyberpunk')

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toHaveAttribute('data-theme', 'cyberpunk')
      expect(backgroundEffect).toHaveAttribute('data-theme-mode', 'dark')

      const primaryOrb = screen.getByTestId('background-orb-primary')
      // Cyberpunk theme uses cyan colors
      expect(primaryOrb.className).toMatch(/cyan-500/)
    })

    it('should use synthwave theme colors when theme is synthwave', () => {
      renderWithTheme(<BackgroundEffect />, 'synthwave')

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toHaveAttribute('data-theme', 'synthwave')
      expect(backgroundEffect).toHaveAttribute('data-theme-mode', 'dark')

      const primaryOrb = screen.getByTestId('background-orb-primary')
      // Synthwave theme uses purple colors
      expect(primaryOrb.className).toMatch(/purple-500/)
    })

    it('should adapt when rendered with Home page in dark mode', () => {
      renderWithTheme(<Home />, 'dark')

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toHaveAttribute('data-theme-mode', 'dark')
      expect(backgroundEffect).toHaveAttribute('data-theme', 'dark')
    })

    it('should detect system dark mode preference', () => {
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

      renderWithTheme(<BackgroundEffect />, 'system')

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toHaveAttribute('data-theme-mode', 'dark')
    })
  })

  // Visual elements and animations
  describe('Visual Elements and Animations', () => {
    it('should have gradient orbs with blur effect', () => {
      renderWithTheme(<BackgroundEffect />, 'light')

      const primaryOrb = screen.getByTestId('background-orb-primary')
      const secondaryOrb = screen.getByTestId('background-orb-secondary')
      const accentOrb = screen.getByTestId('background-orb-accent')

      expect(primaryOrb).toHaveClass('blur-3xl')
      expect(secondaryOrb).toHaveClass('blur-3xl')
      expect(accentOrb).toHaveClass('blur-3xl')
    })

    it('should have rounded orbs', () => {
      renderWithTheme(<BackgroundEffect />, 'light')

      const primaryOrb = screen.getByTestId('background-orb-primary')
      expect(primaryOrb).toHaveClass('rounded-full')
    })

    it('should have animation classes for visual interest', () => {
      renderWithTheme(<BackgroundEffect />, 'light')

      const primaryOrb = screen.getByTestId('background-orb-primary')
      const secondaryOrb = screen.getByTestId('background-orb-secondary')
      const accentOrb = screen.getByTestId('background-orb-accent')

      expect(primaryOrb).toHaveClass('animate-pulse-slow')
      expect(secondaryOrb).toHaveClass('animate-pulse-slower')
      expect(accentOrb).toHaveClass('animate-float')
    })

    it('should have noise texture overlay', () => {
      renderWithTheme(<BackgroundEffect />, 'light')

      const noiseOverlay = screen.getByTestId('background-noise')
      expect(noiseOverlay).toBeInTheDocument()
      expect(noiseOverlay).toHaveClass('absolute', 'inset-0')
    })

    it('should have overflow-hidden to contain visual elements', () => {
      renderWithTheme(<BackgroundEffect />, 'light')

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toHaveClass('overflow-hidden')
    })
  })

  // Accessibility considerations
  describe('Accessibility', () => {
    it('should be completely hidden from screen readers', () => {
      renderWithTheme(<BackgroundEffect />, 'light')

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toHaveAttribute('aria-hidden', 'true')
    })

    it('should not interfere with user interactions', () => {
      renderWithTheme(<BackgroundEffect />, 'light')

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toHaveClass('pointer-events-none')
    })
  })
})
