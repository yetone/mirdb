import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../src/pages/Home'
import FeaturesSection from '../src/components/FeaturesSection'

// Tablet viewport dimensions (iPad)
const TABLET_WIDTH = 768
const TABLET_HEIGHT = 1024

// Helper to render with router
const renderWithRouter = (component: React.ReactElement) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

// Helper to set viewport size via matchMedia mock for tablet
const setupTabletViewport = () => {
  // Mock window.innerWidth and window.innerHeight
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: TABLET_WIDTH,
  })
  Object.defineProperty(window, 'innerHeight', {
    writable: true,
    configurable: true,
    value: TABLET_HEIGHT,
  })

  // Mock matchMedia for responsive queries
  // At 768px, we should match 'md' breakpoint (min-width: 768px) but not 'lg' (min-width: 1024px)
  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: vi.fn().mockImplementation((query: string) => {
      // Parse the query to determine if it should match at tablet width
      let matches = false

      // Handle min-width queries
      const minWidthMatch = query.match(/\(min-width:\s*(\d+)px\)/)
      if (minWidthMatch) {
        const minWidth = parseInt(minWidthMatch[1], 10)
        matches = TABLET_WIDTH >= minWidth
      }

      // Handle max-width queries
      const maxWidthMatch = query.match(/\(max-width:\s*(\d+)px\)/)
      if (maxWidthMatch) {
        const maxWidth = parseInt(maxWidthMatch[1], 10)
        matches = TABLET_WIDTH <= maxWidth
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
    }),
  })

  window.dispatchEvent(new Event('resize'))
}

describe('Responsive Design - Tablet Viewport', () => {
  beforeEach(() => {
    setupTabletViewport()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  // Test Case 1: Page renders with appropriate tablet layout at 768px viewport
  describe('Test Case 1: Page renders at 768px width viewport', () => {
    it('should render Homepage with appropriate tablet layout', () => {
      const { container } = renderWithRouter(<Home />)

      const homepage = screen.getByTestId('homepage')
      expect(homepage).toBeInTheDocument()

      // Check that the main container exists
      const mainElement = container.querySelector('main')
      expect(mainElement).toBeInTheDocument()

      // Verify the page renders with proper responsive structure
      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')

      expect(heroSection).toBeInTheDocument()
      expect(featuresSection).toBeInTheDocument()

      // Verify sections use appropriate tablet padding (sm:px-6 at tablet sizes)
      expect(heroSection.className).toMatch(/sm:px-6/)
      expect(featuresSection.className).toMatch(/sm:px-6/)

      // Verify content uses max-width constraints for proper layout
      const heroContent = heroSection.querySelector('.max-w-4xl')
      expect(heroContent).toBeInTheDocument()
    })

    it('should have all major sections rendered at tablet viewport', () => {
      renderWithRouter(<Home />)

      // Verify all major homepage sections are present
      expect(screen.getByTestId('homepage')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()

      // Check for presence of hero content
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveTextContent(/Shorten Links/i)
    })

    it('should have responsive text sizing for tablet viewport', () => {
      renderWithRouter(<Home />)

      const headline = screen.getByRole('heading', { level: 1 })

      // At tablet size (768px >= 640px), should use sm:text-5xl
      expect(headline.className).toMatch(/text-4xl/) // Base mobile size
      expect(headline.className).toMatch(/sm:text-5xl/) // Tablet size
      expect(headline.className).toMatch(/lg:text-6xl/) // Desktop size
    })
  })

  // Test Case 2: Feature cards display in 2-column grid at 768px viewport
  describe('Test Case 2: Feature cards in 2-column tablet grid', () => {
    it('should display feature cards in 2-column or appropriate tablet grid', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Find the features grid container
      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toBeInTheDocument()

      // Verify the grid has responsive classes for tablet layout
      // At 768px (md breakpoint), should show 2 columns
      expect(featuresGrid.className).toMatch(/grid-cols-1/) // Base mobile
      expect(featuresGrid.className).toMatch(/md:grid-cols-2/) // Tablet (md) breakpoint
      expect(featuresGrid.className).toMatch(/lg:grid-cols-4/) // Desktop (lg) breakpoint

      // Verify all 4 feature cards are present
      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBe(4)
    })

    it('should have the correct grid structure for 2-column tablet layout', () => {
      const { container } = renderWithRouter(<FeaturesSection />)

      // Find grid container
      const grid = container.querySelector('.grid')
      expect(grid).toBeInTheDocument()

      // Verify grid classes include tablet breakpoint (md:grid-cols-2)
      const gridClasses = grid?.className || ''

      // grid-cols-1 is the base (mobile) layout
      expect(gridClasses).toContain('grid-cols-1')

      // md:grid-cols-2 kicks in at medium (768px) breakpoint for tablet
      expect(gridClasses).toContain('md:grid-cols-2')

      // lg:grid-cols-4 kicks in at large (1024px) breakpoint for desktop
      expect(gridClasses).toContain('lg:grid-cols-4')
    })

    it('should have proper gap between feature cards for tablet layout', () => {
      const { container } = renderWithRouter(<FeaturesSection />)

      const grid = container.querySelector('.grid')
      expect(grid).toBeInTheDocument()

      // Verify gap class is present for spacing between cards
      expect(grid?.className).toContain('gap-8')
    })
  })

  // Additional tablet responsiveness tests
  describe('Additional Tablet Responsiveness Checks', () => {
    it('should have CTA buttons in horizontal row layout at tablet viewport', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')

      // Find the button container with flex classes
      // At 768px (>= sm breakpoint 640px), buttons should be in row layout
      const buttonContainer = heroSection.querySelector('.flex')
      expect(buttonContainer).toBeInTheDocument()

      // Should have flex-col for mobile and sm:flex-row for tablet+
      expect(buttonContainer?.className).toMatch(/flex-col/)
      expect(buttonContainer?.className).toMatch(/sm:flex-row/)
    })

    it('should have proper responsive padding on all sections at tablet', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')

      // Both sections should have responsive padding classes
      expect(heroSection.className).toMatch(/px-4/) // Mobile
      expect(heroSection.className).toMatch(/sm:px-6/) // Tablet
      expect(heroSection.className).toMatch(/lg:px-8/) // Desktop

      expect(featuresSection.className).toMatch(/px-4/) // Mobile
      expect(featuresSection.className).toMatch(/sm:px-6/) // Tablet
      expect(featuresSection.className).toMatch(/lg:px-8/) // Desktop
    })

    it('should have max-width container for proper content centering', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')

      // Should have max-w-7xl container for content width constraint
      const contentContainer = featuresSection.querySelector('.max-w-7xl')
      expect(contentContainer).toBeInTheDocument()

      // Container should be centered
      expect(contentContainer?.className).toContain('mx-auto')
    })

    it('should render feature cards with proper styling at tablet viewport', () => {
      renderWithRouter(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')

      // Each card should have proper card styling
      featureCards.forEach((card) => {
        expect(card.className).toContain('card')
        expect(card.className).toContain('bg-base-100')
        expect(card.className).toContain('shadow-xl')

        // Verify card has icon container
        const iconContainer = card.querySelector('.rounded-full')
        expect(iconContainer).toBeInTheDocument()

        // Verify card has title and description
        const title = card.querySelector('[data-testid="feature-title"]')
        const description = card.querySelector('[data-testid="feature-description"]')
        expect(title).toBeInTheDocument()
        expect(description).toBeInTheDocument()
      })
    })

    it('should display section headings correctly at tablet viewport', () => {
      renderWithRouter(<Home />)

      // Features section heading
      const featuresHeading = screen.getByRole('heading', { name: /powerful features/i })
      expect(featuresHeading).toBeInTheDocument()

      // Should have responsive text sizing
      expect(featuresHeading.className).toMatch(/text-3xl/) // Base
      expect(featuresHeading.className).toMatch(/sm:text-4xl/) // Tablet+
    })
  })
})
