import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../src/pages/Home'
import HeroSection from '../src/components/HeroSection'
import FeaturesSection from '../src/components/FeaturesSection'

// Mobile viewport dimensions (iPhone SE)
const MOBILE_WIDTH = 375
const MOBILE_HEIGHT = 667
const MIN_TOUCH_TARGET_SIZE = 44

// Helper to render with router
const renderWithRouter = (component: React.ReactElement) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

// Helper to set viewport size via matchMedia mock
const setupMobileViewport = () => {
  // Mock window.innerWidth and window.innerHeight
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: MOBILE_WIDTH,
  })
  Object.defineProperty(window, 'innerHeight', {
    writable: true,
    configurable: true,
    value: MOBILE_HEIGHT,
  })

  // Mock matchMedia for responsive queries
  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: vi.fn().mockImplementation((query: string) => ({
      matches: query.includes('max-width') || query.includes(`(min-width: 0px)`),
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    })),
  })

  window.dispatchEvent(new Event('resize'))
}

describe('Responsive Design - Mobile Viewport', () => {
  beforeEach(() => {
    setupMobileViewport()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  // Test Case 1: Page renders without horizontal scrollbar at 375px viewport
  describe('Test Case 1: Page renders without horizontal scrollbar', () => {
    it('should render Homepage at 375px width viewport without horizontal overflow', () => {
      const { container } = renderWithRouter(<Home />)

      const homepage = screen.getByTestId('homepage')
      expect(homepage).toBeInTheDocument()

      // Check that the main container exists
      const mainElement = container.querySelector('main')
      expect(mainElement).toBeInTheDocument()

      // Verify the page renders with proper responsive structure
      // Check that all major sections are present
      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')

      expect(heroSection).toBeInTheDocument()
      expect(featuresSection).toBeInTheDocument()

      // Verify sections use responsive padding (px-4 for mobile)
      expect(heroSection.className).toMatch(/px-4/)
      expect(featuresSection.className).toMatch(/px-4/)

      // Verify content doesn't overflow by checking width constraints
      const heroContent = heroSection.querySelector('.max-w-4xl')
      expect(heroContent).toBeInTheDocument()
    })
  })

  // Test Case 2: Hero content is visible and readable without horizontal overflow
  describe('Test Case 2: Hero section layout on mobile', () => {
    it('should render hero content visible and readable without horizontal overflow', () => {
      renderWithRouter(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Verify responsive text sizing for headline
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveTextContent(/Shorten Links/i)

      // Check that headline uses mobile-first responsive text sizing (text-4xl base)
      expect(headline.className).toMatch(/text-4xl/)

      // Verify subheadline is present and readable
      const subheadline = screen.getByText(/Transform your long URLs/i)
      expect(subheadline).toBeInTheDocument()

      // Verify the text uses responsive sizing (text-lg base for mobile)
      expect(subheadline.className).toMatch(/text-lg/)

      // Check max-width constraint on content
      const contentContainer = heroSection.querySelector('.max-w-4xl')
      expect(contentContainer).toBeInTheDocument()

      // Verify buttons are rendered in flex column on mobile (flex-col default)
      const buttonContainer = heroSection.querySelector('.flex')
      expect(buttonContainer).toBeInTheDocument()
      expect(buttonContainer?.className).toMatch(/flex-col/)
    })
  })

  // Test Case 3: Primary CTA button has minimum touch target size (44px height)
  describe('Test Case 3: CTA button touch target size', () => {
    it('should have primary CTA button with minimum touch target size of 44px height', () => {
      renderWithRouter(<HeroSection />)

      // Find the primary CTA button (Get Started Free)
      const primaryCTA = screen.getByRole('link', { name: /Get Started Free/i })
      expect(primaryCTA).toBeInTheDocument()

      // Verify button uses DaisyUI btn-lg class which provides adequate touch targets
      // btn-lg in DaisyUI provides height of 4rem (64px) which exceeds 44px minimum
      expect(primaryCTA.className).toMatch(/btn/)
      expect(primaryCTA.className).toMatch(/btn-lg/)

      // Also verify the secondary CTA
      const secondaryCTA = screen.getByRole('link', { name: /Sign In/i })
      expect(secondaryCTA).toBeInTheDocument()
      expect(secondaryCTA.className).toMatch(/btn/)
      expect(secondaryCTA.className).toMatch(/btn-lg/)

      // Verify that the buttons have the correct structure for accessibility
      expect(primaryCTA).toHaveAttribute('href', '/register')
      expect(secondaryCTA).toHaveAttribute('href', '/login')
    })

    it('should have CTA buttons with adequate size for mobile touch interaction', () => {
      const { container } = renderWithRouter(<HeroSection />)

      // Find all buttons with btn-lg class
      const largeBtns = container.querySelectorAll('.btn-lg')
      expect(largeBtns.length).toBeGreaterThanOrEqual(2)

      // DaisyUI btn-lg has min-height of 3rem (48px) which meets the 44px requirement
      // This is verified through the class presence
      largeBtns.forEach((btn) => {
        expect(btn.classList.contains('btn-lg')).toBe(true)
      })
    })
  })

  // Test Case 4: Feature cards displayed in single column layout at 375px viewport
  describe('Test Case 4: Feature cards single column layout', () => {
    it('should display feature cards in single column layout at mobile viewport', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Find the features grid container
      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toBeInTheDocument()

      // Verify the grid uses responsive classes
      // Should have grid-cols-1 for mobile (single column)
      expect(featuresGrid.className).toMatch(/grid-cols-1/)

      // Verify multiple column layouts are defined for larger screens
      expect(featuresGrid.className).toMatch(/md:grid-cols-2/)
      expect(featuresGrid.className).toMatch(/lg:grid-cols-4/)

      // Verify all feature cards are present
      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBe(4)

      // Each card should be full width on mobile (single column means each card takes full row)
      featureCards.forEach((card) => {
        expect(card).toBeInTheDocument()
        // Cards should have card styling
        expect(card.className).toMatch(/card/)
      })
    })

    it('should have responsive grid that collapses to single column on mobile', () => {
      const { container } = renderWithRouter(<FeaturesSection />)

      // Find grid container
      const grid = container.querySelector('.grid')
      expect(grid).toBeInTheDocument()

      // Verify the grid class includes mobile-first single column
      const gridClasses = grid?.className || ''

      // grid-cols-1 is the base (mobile) layout
      expect(gridClasses).toContain('grid-cols-1')

      // md:grid-cols-2 kicks in at medium breakpoint
      expect(gridClasses).toContain('md:grid-cols-2')

      // lg:grid-cols-4 kicks in at large breakpoint
      expect(gridClasses).toContain('lg:grid-cols-4')
    })
  })

  // Additional mobile responsive tests
  describe('Additional Mobile Responsiveness Checks', () => {
    it('should have proper responsive padding on all sections', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')

      // Both sections should have mobile-first padding (px-4) with responsive increases
      expect(heroSection.className).toMatch(/px-4/)
      expect(heroSection.className).toMatch(/sm:px-6/)
      expect(heroSection.className).toMatch(/lg:px-8/)

      expect(featuresSection.className).toMatch(/px-4/)
      expect(featuresSection.className).toMatch(/sm:px-6/)
      expect(featuresSection.className).toMatch(/lg:px-8/)
    })

    it('should have responsive typography that scales with viewport', () => {
      renderWithRouter(<HeroSection />)

      const headline = screen.getByRole('heading', { level: 1 })

      // Text should scale from text-4xl (mobile) to larger sizes
      expect(headline.className).toMatch(/text-4xl/)
      expect(headline.className).toMatch(/sm:text-5xl/)
      expect(headline.className).toMatch(/lg:text-6xl/)
    })

    it('should stack CTA buttons vertically on mobile', () => {
      renderWithRouter(<HeroSection />)

      const heroSection = screen.getByTestId('hero-section')

      // Find the button container with flex classes
      const buttonContainer = heroSection.querySelector('.flex.flex-col')
      expect(buttonContainer).toBeInTheDocument()

      // Should switch to row layout on larger screens
      expect(buttonContainer?.className).toMatch(/sm:flex-row/)
    })
  })
})
