/**
 * Component Integration Tests - GlassMorphismCard
 * Owner: Scenario 17 - Component Integration - GlassMorphismCard
 * Owner: Scenario 18 - Component Integration - FuturisticButton
 *
 * Integration tests to verify:
 * - GlassMorphismCard component integrates correctly in homepage feature section
 * - Cards render with glass morphism styling (backdrop-blur class)
 * - Card content (icons, titles, descriptions) renders correctly
 * - FuturisticButton integrates correctly in homepage sections
 */

// Mock IntersectionObserver for framer-motion's whileInView
const mockIntersectionObserver = vi.fn()
mockIntersectionObserver.mockReturnValue({
  observe: () => null,
  unobserve: () => null,
  disconnect: () => null,
})
window.IntersectionObserver = mockIntersectionObserver

import { describe, it, expect, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { ThemeProvider } from '../../../src/contexts/ThemeContext'
import Home from '../../../src/pages/Home'
import { FeaturesSection } from '../../../src/components/homepage/FeaturesSection'
import { GlassMorphismCard } from '../../../src/components/GlassMorphismCard'
import '@testing-library/jest-dom'

// Helper to render with all providers
function renderWithProviders(ui: React.ReactElement) {
  return render(
    <BrowserRouter>
      <ThemeProvider>{ui}</ThemeProvider>
    </BrowserRouter>
  )
}

describe('GlassMorphismCard Integration Tests', () => {
  describe('Test Case 1: Feature section with GlassMorphismCard components', () => {
    it('renders feature cards with glass morphism styling (backdrop-blur class)', () => {
      renderWithProviders(<FeaturesSection />)

      // Find all cards by their parent containers that use GlassMorphismCard
      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toBeInTheDocument()

      // Get the card containers - GlassMorphismCard wraps the card-body divs
      const featureCard0 = screen.getByTestId('feature-card-0')
      const featureCard1 = screen.getByTestId('feature-card-1')
      const featureCard2 = screen.getByTestId('feature-card-2')

      expect(featureCard0).toBeInTheDocument()
      expect(featureCard1).toBeInTheDocument()
      expect(featureCard2).toBeInTheDocument()

      // Verify that the parent (GlassMorphismCard) has backdrop-blur class
      // GlassMorphismCard wraps the card-body, so we check the parent element
      const card0Parent = featureCard0.parentElement
      const card1Parent = featureCard1.parentElement
      const card2Parent = featureCard2.parentElement

      expect(card0Parent).toHaveClass('backdrop-blur-md')
      expect(card1Parent).toHaveClass('backdrop-blur-md')
      expect(card2Parent).toHaveClass('backdrop-blur-md')
    })

    it('GlassMorphismCard components have proper glass effect styling classes', () => {
      renderWithProviders(<FeaturesSection />)

      // Get the card-body elements and check their parent (GlassMorphismCard)
      const featureCards = [
        screen.getByTestId('feature-card-0'),
        screen.getByTestId('feature-card-1'),
        screen.getByTestId('feature-card-2'),
      ]

      featureCards.forEach((cardBody) => {
        const glassMorphismCard = cardBody.parentElement
        expect(glassMorphismCard).not.toBeNull()

        // GlassMorphismCard has these key classes for glass effect
        expect(glassMorphismCard).toHaveClass('backdrop-blur-md')
        expect(glassMorphismCard).toHaveClass('shadow-xl')
        expect(glassMorphismCard).toHaveClass('card')

        // Verify transparency styling (bg-base-100/70 means 70% opacity)
        expect(glassMorphismCard?.className).toMatch(/bg-base-100\/70/)

        // Verify border styling
        expect(glassMorphismCard?.className).toMatch(/border/)
      })
    })

    it('renders all three feature cards within GlassMorphismCard wrappers', () => {
      renderWithProviders(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Find all elements with backdrop-blur-md (GlassMorphismCard instances)
      const glassMorphismCards = featuresSection.querySelectorAll('.backdrop-blur-md')

      // Should have exactly 3 GlassMorphismCard instances in features section
      expect(glassMorphismCards.length).toBe(3)
    })

    it('feature section integrates correctly within full Home page', () => {
      renderWithProviders(<Home />)

      // Verify features section exists on homepage
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Verify GlassMorphismCard instances are present
      const glassMorphismCards = featuresSection.querySelectorAll('.backdrop-blur-md')
      expect(glassMorphismCards.length).toBe(3)

      // Each card should have the proper glass morphism styling
      glassMorphismCards.forEach((card) => {
        expect(card).toHaveClass('backdrop-blur-md')
        expect(card).toHaveClass('shadow-xl')
        expect(card).toHaveClass('card')
      })
    })
  })

  describe('Test Case 2: GlassMorphismCard content rendering', () => {
    it('renders icon correctly within GlassMorphismCard', () => {
      renderWithProviders(<FeaturesSection />)

      // Check each feature card has an icon
      for (let i = 0; i < 3; i++) {
        const iconContainer = screen.getByTestId(`feature-icon-${i}`)
        expect(iconContainer).toBeInTheDocument()

        // Icon container should contain an SVG
        const svg = iconContainer.querySelector('svg')
        expect(svg).toBeInTheDocument()

        // Verify icon container is within the card structure
        const cardBody = screen.getByTestId(`feature-card-${i}`)
        expect(cardBody.contains(iconContainer)).toBe(true)
      }
    })

    it('renders title correctly within GlassMorphismCard boundaries', () => {
      renderWithProviders(<FeaturesSection />)

      // Check each feature card has a title
      const expectedTitles = ['Fast URL Shortening', 'Detailed Analytics', 'Dashboard Management']

      for (let i = 0; i < 3; i++) {
        const title = screen.getByTestId(`feature-title-${i}`)
        expect(title).toBeInTheDocument()
        expect(title).toHaveTextContent(expectedTitles[i])

        // Verify title is within the card structure
        const cardBody = screen.getByTestId(`feature-card-${i}`)
        expect(cardBody.contains(title)).toBe(true)
      }
    })

    it('renders description correctly within GlassMorphismCard boundaries', () => {
      renderWithProviders(<FeaturesSection />)

      for (let i = 0; i < 3; i++) {
        const description = screen.getByTestId(`feature-description-${i}`)
        expect(description).toBeInTheDocument()

        // Description should have meaningful content
        expect(description.textContent?.length).toBeGreaterThan(20)

        // Verify description is within the card structure
        const cardBody = screen.getByTestId(`feature-card-${i}`)
        expect(cardBody.contains(description)).toBe(true)
      }
    })

    it('all content (icon, title, description) renders together within single GlassMorphismCard', () => {
      renderWithProviders(<FeaturesSection />)

      for (let i = 0; i < 3; i++) {
        const cardBody = screen.getByTestId(`feature-card-${i}`)
        const iconContainer = screen.getByTestId(`feature-icon-${i}`)
        const title = screen.getByTestId(`feature-title-${i}`)
        const description = screen.getByTestId(`feature-description-${i}`)

        // Verify all three elements are within the same card-body
        expect(cardBody.contains(iconContainer)).toBe(true)
        expect(cardBody.contains(title)).toBe(true)
        expect(cardBody.contains(description)).toBe(true)

        // Verify card-body is within a GlassMorphismCard (has backdrop-blur parent)
        const glassMorphismCard = cardBody.parentElement
        expect(glassMorphismCard).toHaveClass('backdrop-blur-md')
      }
    })

    it('passes custom children content correctly to GlassMorphismCard', () => {
      const { container } = render(
        <BrowserRouter>
          <ThemeProvider>
            <GlassMorphismCard>
              <div data-testid="custom-icon" className="custom-icon">
                <span>Icon Content</span>
              </div>
              <h3 data-testid="custom-title">Custom Title</h3>
              <p data-testid="custom-description">Custom Description Text</p>
            </GlassMorphismCard>
          </ThemeProvider>
        </BrowserRouter>
      )

      const customIcon = screen.getByTestId('custom-icon')
      const customTitle = screen.getByTestId('custom-title')
      const customDescription = screen.getByTestId('custom-description')

      expect(customIcon).toBeInTheDocument()
      expect(customTitle).toBeInTheDocument()
      expect(customDescription).toBeInTheDocument()

      expect(customTitle).toHaveTextContent('Custom Title')
      expect(customDescription).toHaveTextContent('Custom Description Text')

      // Verify all content is within the GlassMorphismCard container
      const glassMorphismCard = container.querySelector('.backdrop-blur-md')
      expect(glassMorphismCard).not.toBeNull()
      expect(glassMorphismCard?.contains(customIcon)).toBe(true)
      expect(glassMorphismCard?.contains(customTitle)).toBe(true)
      expect(glassMorphismCard?.contains(customDescription)).toBe(true)
    })
  })

  describe('GlassMorphismCard Styling Verification', () => {
    it('GlassMorphismCard has correct base styling classes', () => {
      const { container } = render(
        <BrowserRouter>
          <ThemeProvider>
            <GlassMorphismCard>
              <div>Test Content</div>
            </GlassMorphismCard>
          </ThemeProvider>
        </BrowserRouter>
      )

      const card = container.querySelector('.backdrop-blur-md')
      expect(card).not.toBeNull()

      // Verify essential glass morphism classes
      expect(card).toHaveClass('card')
      expect(card).toHaveClass('backdrop-blur-md')
      expect(card).toHaveClass('shadow-xl')

      // Verify transparency and border classes are present in className
      expect(card?.className).toMatch(/bg-base-100\/70/)
      expect(card?.className).toMatch(/border/)
    })

    it('GlassMorphismCard accepts and applies custom className', () => {
      const { container } = render(
        <BrowserRouter>
          <ThemeProvider>
            <GlassMorphismCard className="h-full custom-class">
              <div>Test Content</div>
            </GlassMorphismCard>
          </ThemeProvider>
        </BrowserRouter>
      )

      const card = container.querySelector('.backdrop-blur-md')
      expect(card).not.toBeNull()
      expect(card).toHaveClass('h-full')
      expect(card).toHaveClass('custom-class')
    })

    it('GlassMorphismCard maintains glass morphism styling with custom className', () => {
      const { container } = render(
        <BrowserRouter>
          <ThemeProvider>
            <GlassMorphismCard className="custom-test-class">
              <div>Test Content</div>
            </GlassMorphismCard>
          </ThemeProvider>
        </BrowserRouter>
      )

      const card = container.querySelector('.backdrop-blur-md')
      expect(card).not.toBeNull()

      // Custom class should be added
      expect(card).toHaveClass('custom-test-class')

      // Original glass morphism classes should still be present
      expect(card).toHaveClass('backdrop-blur-md')
      expect(card).toHaveClass('shadow-xl')
      expect(card).toHaveClass('card')
    })
  })

  describe('GlassMorphismCard Animation Integration', () => {
    it('GlassMorphismCard uses framer-motion for animations', () => {
      const { container } = render(
        <BrowserRouter>
          <ThemeProvider>
            <GlassMorphismCard>
              <div>Animation Test</div>
            </GlassMorphismCard>
          </ThemeProvider>
        </BrowserRouter>
      )

      // The card should be wrapped in a motion.div
      // framer-motion adds specific attributes/styles
      const card = container.querySelector('.backdrop-blur-md')
      expect(card).not.toBeNull()

      // Card should be a div element (motion.div renders as div)
      expect(card?.tagName.toLowerCase()).toBe('div')
    })

    it('feature cards within FeaturesSection have animation container', () => {
      renderWithProviders(<FeaturesSection />)

      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toBeInTheDocument()

      // Each GlassMorphismCard should be wrapped in a motion.div
      const glassMorphismCards = featuresGrid.querySelectorAll('.backdrop-blur-md')
      expect(glassMorphismCards.length).toBe(3)

      // Each card should be a div (motion.div renders as div)
      glassMorphismCards.forEach((card) => {
        expect(card.tagName.toLowerCase()).toBe('div')
      })
    })
  })
})
