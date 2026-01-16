import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import BackgroundEffect from './BackgroundEffect'
import { ThemeProvider } from '../contexts/ThemeContext'

const renderWithTheme = (component: React.ReactNode) => {
  return render(
    <ThemeProvider>
      {component}
    </ThemeProvider>
  )
}

describe('BackgroundEffect', () => {
  beforeEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  describe('Component Rendering', () => {
    it('renders without crashing', () => {
      renderWithTheme(<BackgroundEffect data-testid="bg-effect" />)
      expect(screen.getByTestId('bg-effect')).toBeInTheDocument()
    })

    it('has fixed positioning', () => {
      renderWithTheme(<BackgroundEffect data-testid="bg-effect" />)
      const element = screen.getByTestId('bg-effect')
      expect(element).toHaveClass('fixed', 'inset-0')
    })

    it('has negative z-index for background layering', () => {
      renderWithTheme(<BackgroundEffect data-testid="bg-effect" />)
      const element = screen.getByTestId('bg-effect')
      expect(element).toHaveClass('-z-10')
    })

    it('is not interactive (pointer-events-none)', () => {
      renderWithTheme(<BackgroundEffect data-testid="bg-effect" />)
      const element = screen.getByTestId('bg-effect')
      expect(element).toHaveClass('pointer-events-none')
    })

    it('is hidden from screen readers', () => {
      renderWithTheme(<BackgroundEffect data-testid="bg-effect" />)
      const element = screen.getByTestId('bg-effect')
      expect(element).toHaveAttribute('aria-hidden', 'true')
    })

    it('contains animated gradient orbs', () => {
      renderWithTheme(<BackgroundEffect data-testid="bg-effect" />)
      const element = screen.getByTestId('bg-effect')
      // Check for blur class indicating gradient orbs (using blur-2xl for performance)
      const children = element.querySelectorAll('.blur-2xl')
      expect(children.length).toBeGreaterThanOrEqual(3)
    })

    it('contains gradient background elements', () => {
      renderWithTheme(<BackgroundEffect data-testid="bg-effect" />)
      const element = screen.getByTestId('bg-effect')
      // Check for gradient orbs with rounded-full class
      const gradientOrbs = element.querySelectorAll('.rounded-full')
      expect(gradientOrbs.length).toBeGreaterThanOrEqual(3)
    })
  })

  describe('Theme Integration', () => {
    it('renders with light theme colors by default', () => {
      localStorage.setItem('theme-preference', 'light')
      renderWithTheme(<BackgroundEffect data-testid="bg-effect" />)
      const element = screen.getByTestId('bg-effect')

      // Verify gradient orbs are present
      const gradientOrbs = element.querySelectorAll('.rounded-full')
      expect(gradientOrbs.length).toBe(3)

      // All orbs should have opacity-70 class
      gradientOrbs.forEach(orb => {
        expect(orb).toHaveClass('opacity-70')
      })
    })

    it('renders with dark theme colors when dark theme is set', () => {
      localStorage.setItem('theme-preference', 'dark')
      renderWithTheme(<BackgroundEffect data-testid="bg-effect" />)
      const element = screen.getByTestId('bg-effect')

      // Verify gradient orbs are present
      const gradientOrbs = element.querySelectorAll('.rounded-full')
      expect(gradientOrbs.length).toBe(3)

      // All orbs should have opacity-70 class
      gradientOrbs.forEach(orb => {
        expect(orb).toHaveClass('opacity-70')
      })
    })

    it('renders with cyberpunk theme colors when cyberpunk theme is set', () => {
      localStorage.setItem('theme-preference', 'cyberpunk')
      renderWithTheme(<BackgroundEffect data-testid="bg-effect" />)
      const element = screen.getByTestId('bg-effect')

      // Verify gradient orbs are present
      const gradientOrbs = element.querySelectorAll('.rounded-full')
      expect(gradientOrbs.length).toBe(3)
    })

    it('renders with synthwave theme colors when synthwave theme is set', () => {
      localStorage.setItem('theme-preference', 'synthwave')
      renderWithTheme(<BackgroundEffect data-testid="bg-effect" />)
      const element = screen.getByTestId('bg-effect')

      // Verify gradient orbs are present
      const gradientOrbs = element.querySelectorAll('.rounded-full')
      expect(gradientOrbs.length).toBe(3)
    })
  })

  describe('Performance Optimizations', () => {
    it('uses reduced blur (blur-2xl) for performance optimization', () => {
      renderWithTheme(<BackgroundEffect data-testid="bg-effect" />)
      const element = screen.getByTestId('bg-effect')

      // Check for blur-2xl class (optimized blur)
      const blurElements = element.querySelectorAll('.blur-2xl')
      expect(blurElements.length).toBe(3)
    })

    it('has overflow-hidden to contain blur effects', () => {
      renderWithTheme(<BackgroundEffect data-testid="bg-effect" />)
      const element = screen.getByTestId('bg-effect')
      expect(element).toHaveClass('overflow-hidden')
    })

    it('does not use JavaScript-based animations (CSS only)', () => {
      renderWithTheme(<BackgroundEffect data-testid="bg-effect" />)
      const element = screen.getByTestId('bg-effect')

      // BackgroundEffect should not have any Framer Motion data attributes
      expect(element).not.toHaveAttribute('data-framer-appear')
      expect(element).not.toHaveAttribute('data-framer-animation-state')
    })
  })
})
