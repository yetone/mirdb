import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../src/pages/Home'
import FeaturesSection from '../src/components/FeaturesSection'

// Desktop viewport dimensions (1920x1080)
const DESKTOP_WIDTH = 1920
const DESKTOP_HEIGHT = 1080

// Helper to render with router
const renderWithRouter = (component: React.ReactElement) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

// Helper to set viewport size via matchMedia mock
const setupDesktopViewport = () => {
  // Mock window.innerWidth and window.innerHeight
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: DESKTOP_WIDTH,
  })
  Object.defineProperty(window, 'innerHeight', {
    writable: true,
    configurable: true,
    value: DESKTOP_HEIGHT,
  })

  // Mock matchMedia for responsive queries
  // At 1920px, both md (768px) and lg (1024px) breakpoints should match
  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: vi.fn().mockImplementation((query: string) => {
      // Check for min-width queries (e.g., min-width: 768px, min-width: 1024px)
      const minWidthMatch = query.match(/\(min-width:\s*(\d+)px\)/)
      if (minWidthMatch) {
        const minWidth = parseInt(minWidthMatch[1], 10)
        return {
          matches: DESKTOP_WIDTH >= minWidth,
          media: query,
          onchange: null,
          addListener: vi.fn(),
          removeListener: vi.fn(),
          addEventListener: vi.fn(),
          removeEventListener: vi.fn(),
          dispatchEvent: vi.fn(),
        }
      }

      // Check for max-width queries
      const maxWidthMatch = query.match(/\(max-width:\s*(\d+)px\)/)
      if (maxWidthMatch) {
        const maxWidth = parseInt(maxWidthMatch[1], 10)
        return {
          matches: DESKTOP_WIDTH <= maxWidth,
          media: query,
          onchange: null,
          addListener: vi.fn(),
          removeListener: vi.fn(),
          addEventListener: vi.fn(),
          removeEventListener: vi.fn(),
          dispatchEvent: vi.fn(),
        }
      }

      return {
        matches: false,
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

describe('Responsive Design - Desktop Viewport', () => {
  beforeEach(() => {
    setupDesktopViewport()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  // Test Case 1: Page renders with centered content and appropriate max-width constraints at 1920px
  describe('Test Case 1: Centered content with max-width constraints', () => {
    it('should render Homepage at 1920px width viewport with centered content', () => {
      const { container } = renderWithRouter(<Home />)

      const homepage = screen.getByTestId('homepage')
      expect(homepage).toBeInTheDocument()

      // Check that the main container exists
      const mainElement = container.querySelector('main')
      expect(mainElement).toBeInTheDocument()

      // Verify all major sections are present
      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')

      expect(heroSection).toBeInTheDocument()
      expect(featuresSection).toBeInTheDocument()
    })

    it('should have max-width constraints for content centering on desktop', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')

      // Hero section should contain max-w-4xl for content centering
      const heroContent = heroSection.querySelector('.max-w-4xl')
      expect(heroContent).toBeInTheDocument()

      // Features section should contain max-w-7xl for wider content area
      const featuresContent = featuresSection.querySelector('.max-w-7xl')
      expect(featuresContent).toBeInTheDocument()
    })

    it('should have mx-auto class for horizontal centering on max-width containers', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')

      // Hero content should be centered with mx-auto
      const heroContent = heroSection.querySelector('.max-w-4xl.mx-auto')
      expect(heroContent).toBeInTheDocument()

      // Features container should be centered with mx-auto
      const featuresContainer = featuresSection.querySelector('.max-w-7xl.mx-auto')
      expect(featuresContainer).toBeInTheDocument()
    })

    it('should use responsive padding that scales for desktop (lg:px-8)', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')

      // Both sections should have lg:px-8 for desktop padding
      expect(heroSection.className).toMatch(/lg:px-8/)
      expect(featuresSection.className).toMatch(/lg:px-8/)
    })
  })

  // Test Case 2: Feature cards display in multi-column grid (3-4 columns) at 1920px
  describe('Test Case 2: Feature cards multi-column grid layout', () => {
    it('should display feature cards in multi-column grid at desktop viewport', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Find the features grid container
      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toBeInTheDocument()

      // Verify the grid uses lg:grid-cols-4 for desktop layout (4 columns)
      expect(featuresGrid.className).toMatch(/lg:grid-cols-4/)
    })

    it('should have responsive grid classes for all breakpoints', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresGrid = screen.getByTestId('features-grid')

      // Verify grid classes progression from mobile to desktop
      // Mobile: grid-cols-1 (1 column)
      expect(featuresGrid.className).toMatch(/grid-cols-1/)
      // Medium screens: md:grid-cols-2 (2 columns)
      expect(featuresGrid.className).toMatch(/md:grid-cols-2/)
      // Large screens (desktop): lg:grid-cols-4 (4 columns)
      expect(featuresGrid.className).toMatch(/lg:grid-cols-4/)
    })

    it('should render exactly 4 feature cards for the 4-column desktop layout', () => {
      renderWithRouter(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')
      expect(featureCards.length).toBe(4)
    })

    it('should have gap between feature cards in grid layout', () => {
      renderWithRouter(<FeaturesSection />)

      const featuresGrid = screen.getByTestId('features-grid')

      // Verify gap class for spacing between grid items
      expect(featuresGrid.className).toMatch(/gap-8/)
    })

    it('should have feature cards with proper card styling for desktop', () => {
      renderWithRouter(<FeaturesSection />)

      const featureCards = screen.getAllByTestId('feature-card')

      featureCards.forEach((card) => {
        // Verify card has DaisyUI card classes
        expect(card.className).toMatch(/card/)
        expect(card.className).toMatch(/bg-base-100/)
        expect(card.className).toMatch(/shadow-xl/)
      })
    })
  })

  // Additional desktop-specific tests
  describe('Additional Desktop Viewport Tests', () => {
    it('should have text scaling for desktop (lg:text-6xl for headline)', () => {
      renderWithRouter(<Home />)

      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()

      // Desktop should use lg:text-6xl for larger headline
      expect(headline.className).toMatch(/lg:text-6xl/)
    })

    it('should have CTA buttons in horizontal row layout on desktop', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')

      // Find the button container - should have sm:flex-row for horizontal layout
      const buttonContainer = heroSection.querySelector('.flex')
      expect(buttonContainer).toBeInTheDocument()
      expect(buttonContainer?.className).toMatch(/sm:flex-row/)
    })

    it('should have proper text centering in hero section on desktop', () => {
      renderWithRouter(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      const heroContent = heroSection.querySelector('.max-w-4xl')

      // Content should have text-center for centered alignment
      expect(heroContent?.className).toMatch(/text-center/)
    })

    it('should display all sections visible at desktop viewport', () => {
      renderWithRouter(<Home />)

      // Verify all major sections are present and visible
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer')).toBeInTheDocument()
    })

    it('should have feature cards taking advantage of wider viewport with even distribution', () => {
      const { container } = renderWithRouter(<FeaturesSection />)

      // With lg:grid-cols-4, each card should take approximately 1/4 of available width
      // The grid class presence confirms the layout structure
      const grid = container.querySelector('.grid')
      expect(grid).toBeInTheDocument()
      expect(grid?.className).toContain('lg:grid-cols-4')

      // Verify all 4 cards are rendered for even distribution across 4 columns
      const cards = container.querySelectorAll('[data-testid="feature-card"]')
      expect(cards.length).toBe(4)
    })
  })
})
