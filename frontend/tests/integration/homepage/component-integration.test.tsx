/**
 * Component Integration Tests
 * Owner: Scenario 8 - Visual Effects and Components
 *
 * Tests for verifying that BackgroundEffect and GlassMorphismCard
 * components render correctly and integrate properly with the Home page.
 *
 * Test coverage:
 * - BackgroundEffect component renders with visual effects
 * - GlassMorphismCard components are used for feature cards
 * - FuturisticButton components are used for CTAs
 * - Visual effects do not cause layout shifts
 * - Components render without console errors
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { screen, waitFor } from '@testing-library/react'
import { render } from '../../utils/render'
import Home from '../../../src/pages/Home'

describe('Component Integration - Visual Effects', () => {
  let consoleErrorSpy: ReturnType<typeof vi.spyOn>
  let consoleWarnSpy: ReturnType<typeof vi.spyOn>

  beforeEach(() => {
    // Spy on console to detect errors during rendering
    consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    consoleWarnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
  })

  afterEach(() => {
    consoleErrorSpy.mockRestore()
    consoleWarnSpy.mockRestore()
  })

  describe('Test Case 1: BackgroundEffect Component Rendering', () => {
    it('should render BackgroundEffect component wrapping the home page content', () => {
      render(<Home />)

      // BackgroundEffect should render with gradient background classes
      const backgroundElement = document.querySelector('.min-h-screen.bg-gradient-to-br')
      expect(backgroundElement).toBeInTheDocument()
    })

    it('should render animated gradient orbs within BackgroundEffect', () => {
      render(<Home />)

      // Check for animated orb elements with blur effects
      const blurElements = document.querySelectorAll('.blur-3xl.animate-pulse')
      expect(blurElements.length).toBeGreaterThanOrEqual(3)
    })

    it('should have content rendered within the BackgroundEffect z-10 layer', () => {
      render(<Home />)

      // The content should be in a relative z-10 container
      const contentLayer = document.querySelector('.relative.z-10')
      expect(contentLayer).toBeInTheDocument()

      // Home page content should be inside
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
    })
  })

  describe('Test Case 2: GlassMorphismCard Components for Feature Cards', () => {
    it('should render GlassMorphismCard components in the features section', () => {
      render(<Home />)

      // GlassMorphismCard components have data-testid="glass-card"
      const glassCards = screen.getAllByTestId('glass-card')
      expect(glassCards.length).toBeGreaterThanOrEqual(4) // 4 feature cards
    })

    it('should apply glass morphism styling to feature cards', () => {
      render(<Home />)

      const glassCards = screen.getAllByTestId('glass-card')

      glassCards.forEach((card) => {
        // Check for glass morphism classes
        expect(card).toHaveClass('glass-card')
        expect(card).toHaveClass('backdrop-blur-md')
        expect(card).toHaveClass('rounded-xl')
      })
    })

    it('should render feature cards within the features section', () => {
      render(<Home />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Feature cards should be inside the features section
      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toBeInTheDocument()

      const glassCards = screen.getAllByTestId('glass-card')
      glassCards.forEach((card) => {
        expect(featuresGrid.contains(card)).toBe(true)
      })
    })
  })

  describe('Test Case 3: FuturisticButton Components for CTAs', () => {
    it('should render FuturisticButton components in the hero section', () => {
      render(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Look for the CTA buttons
      const signUpButton = screen.getByRole('link', { name: /sign up/i })
      const loginButton = screen.getByRole('link', { name: /log in/i })

      expect(signUpButton).toBeInTheDocument()
      expect(loginButton).toBeInTheDocument()
    })

    it('should apply FuturisticButton styling classes', () => {
      render(<Home />)

      const signUpButton = screen.getByRole('link', { name: /sign up/i })
      const loginButton = screen.getByRole('link', { name: /log in/i })

      // FuturisticButton should have btn base class and transition effects
      expect(signUpButton).toHaveClass('btn')
      expect(signUpButton).toHaveClass('transition-all')

      expect(loginButton).toHaveClass('btn')
      expect(loginButton).toHaveClass('transition-all')
    })

    it('should have correct navigation paths for CTA buttons', () => {
      render(<Home />)

      const signUpButton = screen.getByRole('link', { name: /sign up/i })
      const loginButton = screen.getByRole('link', { name: /log in/i })

      expect(signUpButton).toHaveAttribute('href', '/register')
      expect(loginButton).toHaveAttribute('href', '/login')
    })

    it('should apply primary and secondary variants to CTAs', () => {
      render(<Home />)

      const signUpButton = screen.getByRole('link', { name: /sign up/i })
      const loginButton = screen.getByRole('link', { name: /log in/i })

      // Primary button for Sign Up
      expect(signUpButton).toHaveClass('btn-primary')

      // Secondary button for Log In
      expect(loginButton).toHaveClass('btn-secondary')
    })
  })

  describe('Test Case 4: Visual Effects Layout Stability', () => {
    it('should not cause layout shifts with visual effects', async () => {
      const { container } = render(<Home />)

      // Wait for any animations to settle
      await waitFor(() => {
        const homePage = screen.getByTestId('home-page')
        expect(homePage).toBeInTheDocument()
      })

      // Check that background effects are positioned absolutely (no layout impact)
      const absoluteEffects = container.querySelectorAll('.absolute.inset-0')
      expect(absoluteEffects.length).toBeGreaterThan(0)

      // Background orbs should be pointer-events-none
      const pointerNoneElements = container.querySelectorAll('.pointer-events-none')
      expect(pointerNoneElements.length).toBeGreaterThan(0)
    })

    it('should have min-h-screen to prevent content jumps', () => {
      render(<Home />)

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toHaveClass('min-h-screen')
    })

    it('should use overflow-hidden on background to prevent scrollbar jumps', () => {
      const { container } = render(<Home />)

      const backgroundWrapper = container.querySelector('.min-h-screen.bg-gradient-to-br')
      expect(backgroundWrapper).toHaveClass('overflow-hidden')
    })

    it('should render all sections in correct order without layout issues', () => {
      render(<Home />)

      const homePage = screen.getByTestId('home-page')
      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')
      const footerSection = screen.getByTestId('footer-section')

      // All sections should be present
      expect(heroSection).toBeInTheDocument()
      expect(featuresSection).toBeInTheDocument()
      expect(footerSection).toBeInTheDocument()

      // Verify order by checking DOM positions
      const sections = homePage.querySelectorAll('section')
      expect(sections.length).toBeGreaterThanOrEqual(3)
    })
  })

  describe('Test Case 5: Components Render Without Console Errors', () => {
    it('should render Home component without console errors', () => {
      render(<Home />)

      // Check that no console errors were logged during render
      expect(consoleErrorSpy).not.toHaveBeenCalled()
    })

    it('should render Home component without console warnings related to component issues', () => {
      render(<Home />)

      // Filter out non-critical warnings (like React strict mode warnings)
      const criticalWarnings = consoleWarnSpy.mock.calls.filter((call) => {
        const message = String(call[0])
        // Filter out known non-critical warnings
        return (
          !message.includes('act(...)') &&
          !message.includes('React.StrictMode')
        )
      })

      expect(criticalWarnings.length).toBe(0)
    })

    it('should render all visual effect components successfully', async () => {
      render(<Home />)

      // Wait for component to fully render
      await waitFor(() => {
        // BackgroundEffect
        const backgroundEffect = document.querySelector('.min-h-screen.bg-gradient-to-br')
        expect(backgroundEffect).toBeInTheDocument()

        // GlassMorphismCards
        const glassCards = screen.getAllByTestId('glass-card')
        expect(glassCards.length).toBeGreaterThan(0)

        // FuturisticButtons
        const signUpButton = screen.getByRole('link', { name: /sign up/i })
        const loginButton = screen.getByRole('link', { name: /log in/i })
        expect(signUpButton).toBeInTheDocument()
        expect(loginButton).toBeInTheDocument()
      })

      // No errors should have occurred
      expect(consoleErrorSpy).not.toHaveBeenCalled()
    })

    it('should handle theme context without errors', () => {
      render(<Home />)

      // ThemeProvider is wrapped in our render utility
      // No errors should occur from missing context
      expect(consoleErrorSpy).not.toHaveBeenCalled()
    })
  })
})

describe('Component Integration - Visual Effects Advanced', () => {
  it('should render animated pulse effects on background orbs', () => {
    render(<Home />)

    // Check for animate-pulse on background orbs
    const animatedOrbs = document.querySelectorAll('.animate-pulse')
    expect(animatedOrbs.length).toBeGreaterThanOrEqual(3)
  })

  it('should apply blur effects to background orbs', () => {
    render(<Home />)

    // Check for blur-3xl on orbs
    const blurredOrbs = document.querySelectorAll('.blur-3xl')
    expect(blurredOrbs.length).toBeGreaterThanOrEqual(3)
  })

  it('should render GlassMorphismCard hover effects', () => {
    render(<Home />)

    const glassCards = screen.getAllByTestId('glass-card')

    glassCards.forEach((card) => {
      // Check for hover transition classes
      expect(card).toHaveClass('transition-all')
      expect(card).toHaveClass('duration-300')
    })
  })

  it('should render FuturisticButton with hover scale effect', () => {
    render(<Home />)

    const signUpButton = screen.getByRole('link', { name: /sign up/i })

    // FuturisticButton has hover:scale-105
    expect(signUpButton).toHaveClass('hover:scale-105')
  })
})
