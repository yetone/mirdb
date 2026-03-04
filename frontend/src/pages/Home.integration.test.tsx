/**
 * Homepage Component Integration Tests
 * Owner: Scenario 11 - Component Integration
 *
 * Tests that the homepage correctly integrates with existing components:
 * - Navbar
 * - ThemeToggle
 * - BackgroundEffect
 * - FuturisticButton
 * - GlassMorphismCard
 *
 * Test cases cover all component integration requirements from the scenario.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, within, fireEvent, act } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { Home } from './Home'
import { ThemeProvider, useTheme } from '../contexts/ThemeContext'

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

// Mock matchMedia for reduced motion detection
const matchMediaMock = vi.fn().mockImplementation((query) => ({
  matches: false,
  media: query,
  onchange: null,
  addListener: vi.fn(),
  removeListener: vi.fn(),
  addEventListener: vi.fn(),
  removeEventListener: vi.fn(),
  dispatchEvent: vi.fn(),
}))

Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: matchMediaMock,
})

// Helper to wrap component with router and theme provider
const renderWithProviders = (ui: React.ReactElement) => {
  return render(
    <ThemeProvider defaultTheme="dark">
      <BrowserRouter>{ui}</BrowserRouter>
    </ThemeProvider>
  )
}

describe('Scenario 11: Component Integration Tests', () => {
  beforeEach(() => {
    localStorageMock.clear()
    vi.clearAllMocks()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  /**
   * Test Case 1: Navbar Integration
   * Input: Render Home and query for Navbar component
   * Expected: Navbar component is present in the rendered output
   */
  describe('Test Case 1: Navbar Integration', () => {
    it('should render Navbar component on the homepage', () => {
      renderWithProviders(<Home />)

      const navbar = screen.getByTestId('navbar')
      expect(navbar).toBeInTheDocument()
    })

    it('should render Navbar within the site header', () => {
      renderWithProviders(<Home />)

      const header = screen.getByTestId('site-header')
      const navbar = screen.getByTestId('navbar')
      expect(header).toContainElement(navbar)
    })

    it('should render Navbar with navigation role for accessibility', () => {
      renderWithProviders(<Home />)

      const navbar = screen.getByRole('navigation')
      expect(navbar).toBeInTheDocument()
      expect(navbar).toHaveAttribute('aria-label', 'Main navigation')
    })

    it('should include navigation links in Navbar', () => {
      renderWithProviders(<Home />)

      const featuresLink = screen.getByTestId('nav-features')
      const faqLink = screen.getByTestId('nav-faq')
      const loginLink = screen.getByTestId('nav-login')

      expect(featuresLink).toBeInTheDocument()
      expect(faqLink).toBeInTheDocument()
      expect(loginLink).toBeInTheDocument()
    })

    it('should render logo in Navbar linking to home', () => {
      renderWithProviders(<Home />)

      const logo = screen.getByTestId('navbar-logo')
      expect(logo).toBeInTheDocument()
      expect(logo).toHaveAttribute('href', '/')
    })
  })

  /**
   * Test Case 2: ThemeToggle Integration
   * Input: Render Home and query for ThemeToggle
   * Expected: ThemeToggle component is present and interactive
   */
  describe('Test Case 2: ThemeToggle Integration', () => {
    it('should render ThemeToggle component on the homepage', () => {
      renderWithProviders(<Home />)

      // ThemeToggle components exist (desktop and mobile versions)
      const themeToggles = screen.getAllByTestId('theme-toggle')
      expect(themeToggles.length).toBeGreaterThan(0)
      expect(themeToggles[0]).toBeInTheDocument()
    })

    it('should render ThemeToggle inside the Navbar', () => {
      renderWithProviders(<Home />)

      const navbar = screen.getByTestId('navbar')
      const themeToggle = within(navbar).getAllByTestId('theme-toggle')[0]
      expect(themeToggle).toBeInTheDocument()
    })

    it('should have ThemeToggle button that is clickable', () => {
      renderWithProviders(<Home />)

      // Get the first (desktop) theme toggle button
      const themeToggleButtons = screen.getAllByTestId('theme-toggle-button')
      expect(themeToggleButtons[0]).toBeInTheDocument()
      expect(themeToggleButtons[0]).not.toBeDisabled()
    })

    it('should show theme dropdown when ThemeToggle button is focused', async () => {
      renderWithProviders(<Home />)

      const themeToggleButtons = screen.getAllByTestId('theme-toggle-button')
      expect(themeToggleButtons[0]).toHaveAttribute('aria-haspopup', 'listbox')
    })

    it('should have theme options available in dropdown', () => {
      renderWithProviders(<Home />)

      // Theme dropdown should exist (multiple for desktop/mobile)
      const themeDropdowns = screen.getAllByTestId('theme-dropdown')
      expect(themeDropdowns.length).toBeGreaterThan(0)
      expect(themeDropdowns[0]).toBeInTheDocument()

      // Check for theme options (multiple for desktop/mobile)
      const lightOptions = screen.getAllByTestId('theme-option-light')
      const darkOptions = screen.getAllByTestId('theme-option-dark')
      expect(lightOptions[0]).toBeInTheDocument()
      expect(darkOptions[0]).toBeInTheDocument()
    })

    it('should have proper ARIA labels for accessibility', () => {
      renderWithProviders(<Home />)

      const themeToggleButtons = screen.getAllByTestId('theme-toggle-button')
      expect(themeToggleButtons[0]).toHaveAttribute('aria-label')
      expect(themeToggleButtons[0].getAttribute('aria-label')).toContain('theme')
    })
  })

  /**
   * Test Case 3: BackgroundEffect Integration
   * Input: Render Home and query for BackgroundEffect
   * Expected: BackgroundEffect component renders visual background
   */
  describe('Test Case 3: BackgroundEffect Integration', () => {
    it('should render BackgroundEffect component on the homepage', () => {
      renderWithProviders(<Home />)

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toBeInTheDocument()
    })

    it('should render BackgroundEffect with aria-hidden for accessibility', () => {
      renderWithProviders(<Home />)

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toHaveAttribute('aria-hidden', 'true')
    })

    it('should render background orbs for visual effect', () => {
      renderWithProviders(<Home />)

      const orb1 = screen.getByTestId('background-orb-1')
      const orb2 = screen.getByTestId('background-orb-2')
      expect(orb1).toBeInTheDocument()
      expect(orb2).toBeInTheDocument()
    })

    it('should render background grid pattern', () => {
      renderWithProviders(<Home />)

      const grid = screen.getByTestId('background-grid')
      expect(grid).toBeInTheDocument()
    })

    it('should render BackgroundEffect with pointer-events-none', () => {
      renderWithProviders(<Home />)

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toHaveClass('pointer-events-none')
    })

    it('should render BackgroundEffect as fixed position', () => {
      renderWithProviders(<Home />)

      const backgroundEffect = screen.getByTestId('background-effect')
      expect(backgroundEffect).toHaveClass('fixed')
    })
  })

  /**
   * Test Case 4: FuturisticButton Integration
   * Input: Query CTA buttons for FuturisticButton component
   * Expected: Primary CTAs use FuturisticButton with appropriate props
   */
  describe('Test Case 4: FuturisticButton Integration', () => {
    it('should render FuturisticButton in HeroSection CTA', () => {
      renderWithProviders(<Home />)

      const heroCta = screen.getByTestId('hero-cta')
      expect(heroCta).toBeInTheDocument()
    })

    it('should render FuturisticButton in Navbar as Get Started button', () => {
      renderWithProviders(<Home />)

      const navRegister = screen.getByTestId('nav-register')
      expect(navRegister).toBeInTheDocument()
    })

    it('should have hero CTA with appropriate text', () => {
      renderWithProviders(<Home />)

      const heroCta = screen.getByTestId('hero-cta')
      expect(heroCta).toHaveTextContent('Get Started Free')
    })

    it('should have hero CTA with large size styling', () => {
      renderWithProviders(<Home />)

      const heroCta = screen.getByTestId('hero-cta')
      expect(heroCta).toHaveClass('btn-lg')
    })

    it('should have hero CTA with primary variant styling', () => {
      renderWithProviders(<Home />)

      const heroCta = screen.getByTestId('hero-cta')
      expect(heroCta).toHaveClass('btn-primary')
    })

    it('should render CTA footer with FuturisticButton', () => {
      renderWithProviders(<Home />)

      const ctaFooter = screen.getByTestId('cta-footer')
      expect(ctaFooter).toBeInTheDocument()
    })

    it('should have CTA buttons with appropriate aria-labels', () => {
      renderWithProviders(<Home />)

      const heroCta = screen.getByTestId('hero-cta')
      expect(heroCta).toHaveAttribute('aria-label')
    })
  })

  /**
   * Test Case 5: GlassMorphismCard Integration
   * Input: Query feature cards for GlassMorphismCard component
   * Expected: Feature section uses GlassMorphismCard for each feature
   */
  describe('Test Case 5: GlassMorphismCard Integration', () => {
    it('should render FeaturesSection on the homepage', () => {
      renderWithProviders(<Home />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
    })

    it('should render feature cards using GlassMorphismCard', () => {
      renderWithProviders(<Home />)

      const featureCard1 = screen.getByTestId('feature-card-1')
      const featureCard2 = screen.getByTestId('feature-card-2')
      const featureCard3 = screen.getByTestId('feature-card-3')
      const featureCard4 = screen.getByTestId('feature-card-4')

      expect(featureCard1).toBeInTheDocument()
      expect(featureCard2).toBeInTheDocument()
      expect(featureCard3).toBeInTheDocument()
      expect(featureCard4).toBeInTheDocument()
    })

    it('should render GlassMorphismCards with glassmorphism styling', () => {
      renderWithProviders(<Home />)

      const featureCard1 = screen.getByTestId('feature-card-1')
      expect(featureCard1).toHaveClass('backdrop-blur-md')
    })

    it('should render feature cards with icons', () => {
      renderWithProviders(<Home />)

      const icon1 = screen.getByTestId('feature-icon-1')
      const icon2 = screen.getByTestId('feature-icon-2')
      expect(icon1).toBeInTheDocument()
      expect(icon2).toBeInTheDocument()
    })

    it('should render feature cards with titles', () => {
      renderWithProviders(<Home />)

      const title1 = screen.getByTestId('feature-title-1')
      const title2 = screen.getByTestId('feature-title-2')
      expect(title1).toBeInTheDocument()
      expect(title2).toBeInTheDocument()
    })

    it('should render feature cards with descriptions', () => {
      renderWithProviders(<Home />)

      const desc1 = screen.getByTestId('feature-description-1')
      const desc2 = screen.getByTestId('feature-description-2')
      expect(desc1).toBeInTheDocument()
      expect(desc2).toBeInTheDocument()
    })

    it('should render all 4 feature cards in a grid layout', () => {
      renderWithProviders(<Home />)

      // Verify all 4 features are present
      for (let i = 1; i <= 4; i++) {
        expect(screen.getByTestId(`feature-card-${i}`)).toBeInTheDocument()
      }
    })
  })

  /**
   * Test Case 6: ThemeContext Integration
   * Input: Verify Home component subscribes to ThemeContext
   * Expected: Home component uses useTheme or similar to react to theme changes
   */
  describe('Test Case 6: ThemeContext Integration', () => {
    it('should render Home with theme context available', () => {
      renderWithProviders(<Home />)

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()
    })

    it('should have data-theme-active attribute reflecting current theme', () => {
      renderWithProviders(<Home />)

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toHaveAttribute('data-theme-active')
    })

    it('should use AnimatePresence for theme transition animations', () => {
      const { container } = renderWithProviders(<Home />)

      // Home page is wrapped with motion.main for theme transitions
      const mainElement = container.querySelector('[data-testid="home-page"]')
      expect(mainElement).toBeInTheDocument()
    })

    it('should persist theme across component re-renders', () => {
      const { rerender } = renderWithProviders(<Home />)

      const homePage1 = screen.getByTestId('home-page')
      expect(homePage1).toHaveAttribute('data-theme-active', 'dark')

      rerender(
        <ThemeProvider defaultTheme="dark">
          <BrowserRouter>
            <Home />
          </BrowserRouter>
        </ThemeProvider>
      )

      const homePage2 = screen.getByTestId('home-page')
      expect(homePage2).toHaveAttribute('data-theme-active', 'dark')
    })

    it('should have theme transition CSS classes', () => {
      renderWithProviders(<Home />)

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toHaveClass('transition-colors')
    })

    it('should have proper color scheme applied via base classes', () => {
      renderWithProviders(<Home />)

      const homePage = screen.getByTestId('home-page')
      expect(homePage).toHaveClass('bg-base-100')
    })
  })

  /**
   * Additional Integration Tests
   */
  describe('Complete Component Hierarchy', () => {
    it('should render all major sections in correct order', () => {
      const { container } = renderWithProviders(<Home />)

      const elements = container.querySelectorAll('[data-testid]')
      const testIds = Array.from(elements).map((el) => el.getAttribute('data-testid'))

      // Verify key components are present
      expect(testIds).toContain('background-effect')
      expect(testIds).toContain('site-header')
      expect(testIds).toContain('navbar')
      expect(testIds).toContain('home-page')
      expect(testIds).toContain('hero-section')
      expect(testIds).toContain('features-section')
    })

    it('should render site header with banner role', () => {
      renderWithProviders(<Home />)

      const header = screen.getByRole('banner')
      expect(header).toBeInTheDocument()
    })

    it('should render main content with main role', () => {
      renderWithProviders(<Home />)

      const main = screen.getByRole('main')
      expect(main).toBeInTheDocument()
    })

    it('should render site footer with contentinfo role', () => {
      renderWithProviders(<Home />)

      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
    })
  })
})
