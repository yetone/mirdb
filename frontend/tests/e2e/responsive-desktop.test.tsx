/**
 * E2E Tests for Desktop Responsive Design (1024px+ viewport)
 * Owner: Scenario 9 - Responsive Design - Desktop
 *
 * These tests validate the homepage displays correctly on desktop viewports
 * with full multi-column layouts and optimal spacing.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, within } from '../setup'
import Home from '@/pages/Home'

// Desktop viewport width for testing
const DESKTOP_VIEWPORT_WIDTH = 1440

/**
 * Sets up viewport simulation for desktop testing
 */
function setupDesktopViewport() {
  // Mock window.innerWidth for desktop viewport
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: DESKTOP_VIEWPORT_WIDTH,
  })

  Object.defineProperty(window, 'innerHeight', {
    writable: true,
    configurable: true,
    value: 900,
  })

  // Mock matchMedia for responsive breakpoints
  window.matchMedia = vi.fn().mockImplementation((query: string) => {
    // Parse common Tailwind breakpoints
    const breakpoints: Record<string, number> = {
      '(min-width: 640px)': 640,   // sm
      '(min-width: 768px)': 768,   // md
      '(min-width: 1024px)': 1024, // lg
      '(min-width: 1280px)': 1280, // xl
      '(min-width: 1536px)': 1536, // 2xl
    }

    const minWidthMatch = query.match(/\(min-width:\s*(\d+)px\)/)
    let matches = true

    if (minWidthMatch) {
      const minWidth = parseInt(minWidthMatch[1], 10)
      matches = DESKTOP_VIEWPORT_WIDTH >= minWidth
    }

    return {
      matches,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }
  })

  // Trigger resize event
  window.dispatchEvent(new Event('resize'))
}

describe('Responsive Design - Desktop (1440px viewport)', () => {
  beforeEach(() => {
    setupDesktopViewport()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  describe('Test Case 1: All content visible with max-width constraints', () => {
    it('renders all homepage sections at 1440px viewport width', () => {
      render(<Home />)

      // Verify all major sections are present
      expect(screen.getByTestId('home-page')).toBeInTheDocument()
      expect(screen.getByTestId('navbar')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()

      // Verify content sections are visible (use heading role to distinguish from nav link)
      const featuresHeading = screen.getByRole('heading', { name: 'Features' })
      expect(featuresHeading).toBeInTheDocument()
      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
      expect(screen.getByText('Click Analytics')).toBeInTheDocument()
      expect(screen.getByText('Real-time Tracking')).toBeInTheDocument()
    })

    it('applies appropriate max-width constraints to content containers', () => {
      render(<Home />)

      // Hero section should have centered content with max-width constraint
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Hero content should have max-width class for optimal reading
      const heroContent = heroSection.querySelector('.hero-content')
      expect(heroContent).toBeInTheDocument()

      // Verify hero has max-width constraint on inner content
      const maxWidthElement = heroSection.querySelector('.max-w-2xl')
      expect(maxWidthElement).toBeInTheDocument()
    })

    it('main page container has proper min-height', () => {
      render(<Home />)

      const mainContainer = screen.getByTestId('home-page')
      expect(mainContainer).toHaveClass('min-h-screen')
    })
  })

  describe('Test Case 2: Feature cards display in 3-4 column grid layout', () => {
    it('renders feature grid with correct responsive classes for desktop', () => {
      render(<Home />)

      // Find the features section grid container using the heading role
      const featuresHeading = screen.getByRole('heading', { name: 'Features' })
      const featuresSection = featuresHeading.closest('section')
      expect(featuresSection).toBeInTheDocument()

      // Find the grid container within features section
      const gridContainer = featuresSection?.querySelector('.grid')
      expect(gridContainer).toBeInTheDocument()

      // Verify desktop grid class (md:grid-cols-3 for 3 columns at medium+ viewport)
      expect(gridContainer).toHaveClass('grid-cols-1')
      expect(gridContainer).toHaveClass('md:grid-cols-3')
    })

    it('renders exactly 3 feature cards in the grid', () => {
      render(<Home />)

      // Find all feature cards by their card class using heading role
      const featuresHeading = screen.getByRole('heading', { name: 'Features' })
      const featuresSection = featuresHeading.closest('section')
      const featureCards = featuresSection?.querySelectorAll('.card')

      expect(featureCards).toBeDefined()
      expect(featureCards?.length).toBe(3)
    })

    it('each feature card has title and description', () => {
      render(<Home />)

      // Verify each feature card content
      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
      expect(screen.getByText('Click Analytics')).toBeInTheDocument()
      expect(screen.getByText('Real-time Tracking')).toBeInTheDocument()

      // Verify descriptions are present
      expect(
        screen.getByText(/Transform long URLs into short, memorable links/i)
      ).toBeInTheDocument()
      expect(
        screen.getByText(/Track every click with detailed analytics/i)
      ).toBeInTheDocument()
      expect(
        screen.getByText(/Monitor your links in real-time/i)
      ).toBeInTheDocument()
    })

    it('feature grid has proper gap spacing', () => {
      render(<Home />)

      // Find section using heading role
      const featuresHeading = screen.getByRole('heading', { name: 'Features' })
      const featuresSection = featuresHeading.closest('section')
      const gridContainer = featuresSection?.querySelector('.grid')

      expect(gridContainer).toHaveClass('gap-6')
    })
  })

  describe('Test Case 3: Navigation displays full inline menu', () => {
    it('renders full desktop navigation at 1440px viewport', () => {
      render(<Home />)

      const navbar = screen.getByTestId('navbar')
      expect(navbar).toBeInTheDocument()
    })

    it('displays logo in navbar', () => {
      render(<Home />)

      const logo = screen.getByTestId('navbar-logo')
      expect(logo).toBeInTheDocument()

      // Logo should link to homepage
      expect(logo).toHaveAttribute('href', '/')
    })

    it('shows desktop menu links at lg breakpoint', () => {
      render(<Home />)

      // Desktop menu should have lg:flex class (visible at 1024px+)
      const navbar = screen.getByTestId('navbar')
      const desktopMenuContainer = navbar.querySelector('.navbar-center')

      expect(desktopMenuContainer).toBeInTheDocument()
      expect(desktopMenuContainer).toHaveClass('hidden')
      expect(desktopMenuContainer).toHaveClass('lg:flex')
    })

    it('displays navigation links: Features, Pricing, About', () => {
      render(<Home />)

      // These links should be in the desktop menu
      expect(screen.getByTestId('nav-features')).toBeInTheDocument()
      expect(screen.getByTestId('nav-features')).toHaveTextContent('Features')

      expect(screen.getByTestId('nav-pricing')).toBeInTheDocument()
      expect(screen.getByTestId('nav-pricing')).toHaveTextContent('Pricing')

      expect(screen.getByTestId('nav-about')).toBeInTheDocument()
      expect(screen.getByTestId('nav-about')).toHaveTextContent('About')
    })

    it('displays CTA buttons in desktop navbar', () => {
      render(<Home />)

      // Login and Get Started buttons should be visible
      const loginButton = screen.getByTestId('nav-login')
      expect(loginButton).toBeInTheDocument()
      expect(loginButton).toHaveTextContent('Login')

      const getStartedButton = screen.getByTestId('nav-get-started')
      expect(getStartedButton).toBeInTheDocument()
      expect(getStartedButton).toHaveTextContent('Get Started')
      expect(getStartedButton).toHaveClass('btn-primary')
    })

    it('desktop auth section has lg:flex for desktop visibility', () => {
      render(<Home />)

      const navbar = screen.getByTestId('navbar')
      const authSection = navbar.querySelector('.navbar-end.hidden.lg\\:flex')

      expect(authSection).toBeInTheDocument()
    })

    it('mobile menu toggle is hidden at desktop viewport', () => {
      render(<Home />)

      // Mobile menu toggle should have lg:hidden class
      const mobileToggle = screen.getByTestId('mobile-menu-toggle')
      const mobileContainer = mobileToggle.closest('.navbar-end.lg\\:hidden')

      expect(mobileContainer).toBeInTheDocument()
      expect(mobileContainer).toHaveClass('lg:hidden')
    })
  })

  describe('Test Case 4: Hero section has proper desktop layout', () => {
    it('renders hero section with proper desktop styling', () => {
      render(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
      expect(heroSection).toHaveClass('hero')
      expect(heroSection).toHaveClass('min-h-[70vh]')
    })

    it('displays headline with desktop-appropriate text size', () => {
      render(<Home />)

      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveTextContent(/Shorten URLs/i)

      // Verify responsive text classes for desktop
      expect(headline).toHaveClass('text-4xl')
      expect(headline).toHaveClass('md:text-5xl')
      expect(headline).toHaveClass('lg:text-6xl')
    })

    it('displays subheading with proper styling', () => {
      render(<Home />)

      // Find the hero section first, then the h2 within it
      const heroSection = screen.getByTestId('hero-section')
      const subheading = within(heroSection).getByRole('heading', { level: 2 })

      expect(subheading).toBeInTheDocument()
      expect(subheading).toHaveClass('text-lg')
      expect(subheading).toHaveClass('md:text-xl')
    })

    it('CTA buttons display side-by-side on desktop (sm:flex-row)', () => {
      render(<Home />)

      const primaryCta = screen.getByTestId('primary-cta')
      const secondaryCta = screen.getByTestId('secondary-cta')

      expect(primaryCta).toBeInTheDocument()
      expect(secondaryCta).toBeInTheDocument()

      // Verify the CTA container has flex-row class for desktop
      const ctaContainer = primaryCta.closest('.flex')
      expect(ctaContainer).toBeInTheDocument()
      expect(ctaContainer).toHaveClass('flex-col')
      expect(ctaContainer).toHaveClass('sm:flex-row')
    })

    it('primary CTA has prominent styling', () => {
      render(<Home />)

      const primaryCta = screen.getByTestId('primary-cta')
      expect(primaryCta).toHaveClass('btn')
      expect(primaryCta).toHaveClass('btn-primary')
      expect(primaryCta).toHaveClass('btn-lg')
    })

    it('secondary CTA has ghost styling', () => {
      render(<Home />)

      const secondaryCta = screen.getByTestId('secondary-cta')
      expect(secondaryCta).toHaveClass('btn')
      expect(secondaryCta).toHaveClass('btn-ghost')
      expect(secondaryCta).toHaveClass('btn-lg')
    })

    it('CTAs link to correct routes', () => {
      render(<Home />)

      const primaryCta = screen.getByTestId('primary-cta')
      expect(primaryCta).toHaveAttribute('href', '/register')

      const secondaryCta = screen.getByTestId('secondary-cta')
      expect(secondaryCta).toHaveAttribute('href', '/login')
    })
  })

  describe('Additional Desktop Layout Verifications', () => {
    it('content is centered with container class', () => {
      render(<Home />)

      // Features section should have container with mx-auto
      const featuresHeading = screen.getByRole('heading', { name: 'Features' })
      const featuresSection = featuresHeading.closest('section')
      const container = featuresSection?.querySelector('.container')

      expect(container).toBeInTheDocument()
      expect(container).toHaveClass('mx-auto')
    })

    it('footer renders at bottom of page', () => {
      render(<Home />)

      const footer = document.querySelector('footer')
      expect(footer).toBeInTheDocument()
    })

    it('page background uses base-100 theme color', () => {
      render(<Home />)

      const mainContainer = screen.getByTestId('home-page')
      expect(mainContainer).toHaveClass('bg-base-100')
    })

    it('navbar is sticky at top', () => {
      render(<Home />)

      const navbar = screen.getByTestId('navbar')
      expect(navbar).toHaveClass('sticky')
      expect(navbar).toHaveClass('top-0')
      expect(navbar).toHaveClass('z-50')
    })
  })
})
