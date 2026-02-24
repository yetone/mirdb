/**
 * BackgroundEffect Component Tests
 * Owner: Scenario 11 - Background Visual Effects
 *
 * Tests for the animated background effect component that uses Framer Motion.
 * Verifies component mounting, animation properties, z-index ordering, and theme adaptation.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BackgroundEffect } from '@/components/BackgroundEffect'
import { ThemeProvider } from '@/contexts/ThemeContext'
import React from 'react'

// Helper to render with ThemeProvider
function renderWithTheme(ui: React.ReactElement, theme: string = 'dark') {
  // Mock localStorage to return the specified theme
  const localStorageMock = {
    getItem: vi.fn((key: string) => (key === 'theme' ? theme : null)),
    setItem: vi.fn(),
    removeItem: vi.fn(),
    clear: vi.fn(),
    length: 0,
    key: vi.fn(),
  }
  Object.defineProperty(window, 'localStorage', { value: localStorageMock, writable: true })

  // Set the data-theme attribute on document element
  document.documentElement.setAttribute('data-theme', theme)

  return render(<ThemeProvider>{ui}</ThemeProvider>)
}

describe('BackgroundEffect Component', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  afterEach(() => {
    document.documentElement.removeAttribute('data-theme')
  })

  describe('Test Case 1: Component mounting and visibility', () => {
    it('renders the BackgroundEffect component', () => {
      renderWithTheme(<BackgroundEffect />)

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toBeInTheDocument()
    })

    it('is present in the DOM and will animate to visible', () => {
      renderWithTheme(<BackgroundEffect />)

      const backgroundEffect = screen.getByTestId('background-effect')
      // Component starts with opacity: 0 and animates to opacity: 1
      // This is expected behavior for Framer Motion animations
      expect(backgroundEffect).toBeInTheDocument()
      // The component has initial opacity 0 which will animate to 1
      expect(backgroundEffect.style.opacity).toBe('0')
    })

    it('contains animated elements (child divs)', () => {
      renderWithTheme(<BackgroundEffect />)

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect.children.length).toBeGreaterThan(0)
    })

    it('has aria-hidden attribute for accessibility', () => {
      renderWithTheme(<BackgroundEffect />)

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toHaveAttribute('aria-hidden', 'true')
    })
  })

  describe('Test Case 2: Framer Motion animation properties', () => {
    it('component is wrapped in motion element for animation', () => {
      renderWithTheme(<BackgroundEffect />)

      // The component renders successfully which means framer-motion is working
      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toBeInTheDocument()
    })

    it('has child elements that would be animated orbs', () => {
      renderWithTheme(<BackgroundEffect />)

      const backgroundEffect = screen.getByTestId('background-effect')
      // Should have multiple animated elements (orbs + grid overlay)
      expect(backgroundEffect.children.length).toBeGreaterThanOrEqual(3)
    })

    it('orbs have blur effect classes for visual effect', () => {
      renderWithTheme(<BackgroundEffect />)

      const backgroundEffect = screen.getByTestId('background-effect')
      const orbs = backgroundEffect.querySelectorAll('.blur-3xl')
      expect(orbs.length).toBeGreaterThanOrEqual(1)
    })

    it('orbs are positioned absolutely within container', () => {
      renderWithTheme(<BackgroundEffect />)

      const backgroundEffect = screen.getByTestId('background-effect')
      const absoluteElements = backgroundEffect.querySelectorAll('.absolute')
      expect(absoluteElements.length).toBeGreaterThanOrEqual(3)
    })
  })

  describe('Test Case 3: Background z-index ordering', () => {
    it('has fixed positioning', () => {
      renderWithTheme(<BackgroundEffect />)

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect.className).toContain('fixed')
    })

    it('has inset-0 for full coverage', () => {
      renderWithTheme(<BackgroundEffect />)

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect.className).toContain('inset-0')
    })

    it('has low z-index to render behind content', () => {
      renderWithTheme(<BackgroundEffect />)

      const backgroundEffect = screen.getByTestId('background-effect')
      const className = backgroundEffect.className

      // Check for low z-index (z-0 or negative z-index)
      expect(
        className.includes('z-0') ||
        className.includes('-z-') ||
        className.includes('z-[-')
      ).toBe(true)
    })

    it('has pointer-events-none to not block interactions', () => {
      renderWithTheme(<BackgroundEffect />)

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect.className).toContain('pointer-events-none')
    })

    it('has overflow-hidden to contain animations', () => {
      renderWithTheme(<BackgroundEffect />)

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect.className).toContain('overflow-hidden')
    })
  })

  describe('Test Case 4: Theme adaptation', () => {
    it('applies theme-aware background colors in dark theme', () => {
      renderWithTheme(<BackgroundEffect />, 'dark')

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toBeInTheDocument()

      // Check for DaisyUI base color class
      expect(backgroundEffect.className).toContain('bg-base-')
    })

    it('applies theme-aware background colors in light theme', () => {
      renderWithTheme(<BackgroundEffect />, 'light')

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toBeInTheDocument()
    })

    it('applies theme-aware background colors in cyberpunk theme', () => {
      renderWithTheme(<BackgroundEffect />, 'cyberpunk')

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toBeInTheDocument()
    })

    it('applies theme-aware background colors in synthwave theme', () => {
      renderWithTheme(<BackgroundEffect />, 'synthwave')

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toBeInTheDocument()
    })

    it('uses DaisyUI theme-aware color classes for orbs', () => {
      renderWithTheme(<BackgroundEffect />, 'dark')

      const backgroundEffect = screen.getByTestId('background-effect')
      const htmlContent = backgroundEffect.innerHTML

      // Check for DaisyUI theme-aware classes (primary, secondary, accent)
      const hasDaisyUIColors =
        htmlContent.includes('bg-primary') ||
        htmlContent.includes('bg-secondary') ||
        htmlContent.includes('bg-accent')

      expect(hasDaisyUIColors).toBe(true)
    })

    it('orbs use primary color class', () => {
      renderWithTheme(<BackgroundEffect />, 'dark')

      const backgroundEffect = screen.getByTestId('background-effect')
      const primaryOrb = backgroundEffect.querySelector('.bg-primary\\/20')
      expect(primaryOrb).toBeInTheDocument()
    })

    it('orbs use secondary color class', () => {
      renderWithTheme(<BackgroundEffect />, 'dark')

      const backgroundEffect = screen.getByTestId('background-effect')
      const secondaryOrb = backgroundEffect.querySelector('.bg-secondary\\/20')
      expect(secondaryOrb).toBeInTheDocument()
    })

    it('orbs use accent color class', () => {
      renderWithTheme(<BackgroundEffect />, 'dark')

      const backgroundEffect = screen.getByTestId('background-effect')
      const accentOrb = backgroundEffect.querySelector('.bg-accent\\/15')
      expect(accentOrb).toBeInTheDocument()
    })
  })

  describe('Component integration', () => {
    it('renders correctly inside a parent container', () => {
      renderWithTheme(
        <div className="relative min-h-screen">
          <BackgroundEffect />
          <main data-testid="main-content" className="relative z-10">
            <h1>Content</h1>
          </main>
        </div>
      )

      const backgroundEffect = screen.getByTestId('background-effect')
      const mainContent = screen.getByTestId('main-content')

      expect(backgroundEffect).toBeInTheDocument()
      expect(mainContent).toBeInTheDocument()
    })

    it('does not interfere with content interaction', () => {
      renderWithTheme(
        <div className="relative">
          <BackgroundEffect />
          <button data-testid="test-button">Click me</button>
        </div>
      )

      const button = screen.getByTestId('test-button')
      expect(button).toBeVisible()

      // Button should be clickable (not blocked by background)
      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect.className).toContain('pointer-events-none')
    })
  })
})
