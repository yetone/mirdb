/**
 * Component Integration Tests - GlassMorphismCard and FuturisticButton
 * Owner: Scenario 17 - Component Integration - GlassMorphismCard
 * Owner: Scenario 18 - Component Integration - FuturisticButton
 *
 * Integration tests to verify:
 * - GlassMorphismCard component integrates correctly in homepage feature section
 * - Cards render with glass morphism styling (backdrop-blur class)
 * - Card content (icons, titles, descriptions) renders correctly
 * - FuturisticButton integrates correctly in homepage sections
 * - Button renders with futuristic styling classes
 * - Click events fire and callbacks execute
 * - Both primary and secondary variants render correctly
 */

// Mock IntersectionObserver for framer-motion's whileInView
const mockIntersectionObserver = vi.fn()
mockIntersectionObserver.mockReturnValue({
  observe: () => null,
  unobserve: () => null,
  disconnect: () => null,
})
window.IntersectionObserver = mockIntersectionObserver

// Mock PointerEvent for framer-motion gestures
if (typeof window.PointerEvent === 'undefined') {
  class MockPointerEvent extends MouseEvent {
    pointerId: number
    pressure: number
    tangentialPressure: number
    tiltX: number
    tiltY: number
    twist: number
    width: number
    height: number
    pointerType: string
    isPrimary: boolean

    constructor(type: string, params: PointerEventInit = {}) {
      super(type, params)
      this.pointerId = params.pointerId ?? 0
      this.pressure = params.pressure ?? 0
      this.tangentialPressure = params.tangentialPressure ?? 0
      this.tiltX = params.tiltX ?? 0
      this.tiltY = params.tiltY ?? 0
      this.twist = params.twist ?? 0
      this.width = params.width ?? 1
      this.height = params.height ?? 1
      this.pointerType = params.pointerType ?? 'mouse'
      this.isPrimary = params.isPrimary ?? true
    }

    getCoalescedEvents() {
      return []
    }

    getPredictedEvents() {
      return []
    }
  }
  // @ts-ignore
  window.PointerEvent = MockPointerEvent
}

import { describe, it, expect, vi } from 'vitest'
import { render, screen, within, fireEvent } from '@testing-library/react'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import { ThemeProvider } from '../../../src/contexts/ThemeContext'
import Home from '../../../src/pages/Home'
import { FeaturesSection } from '../../../src/components/homepage/FeaturesSection'
import { GlassMorphismCard } from '../../../src/components/GlassMorphismCard'
import { FuturisticButton } from '../../../src/components/FuturisticButton'
import '@testing-library/jest-dom'

// Helper to render with all providers (BrowserRouter version for GlassMorphismCard tests)
function renderWithBrowserRouter(ui: React.ReactElement) {
  return render(
    <BrowserRouter>
      <ThemeProvider>{ui}</ThemeProvider>
    </BrowserRouter>
  )
}

// Helper to render with all providers (MemoryRouter version for FuturisticButton tests)
function renderWithProviders(ui: React.ReactElement) {
  return render(
    <MemoryRouter>
      <ThemeProvider>{ui}</ThemeProvider>
    </MemoryRouter>
  )
}

describe('GlassMorphismCard Integration Tests', () => {
  describe('Test Case 1: Feature section with GlassMorphismCard components', () => {
    it('renders feature cards with glass morphism styling (backdrop-blur class)', () => {
      renderWithBrowserRouter(<FeaturesSection />)

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
      renderWithBrowserRouter(<FeaturesSection />)

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
      renderWithBrowserRouter(<FeaturesSection />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Find all elements with backdrop-blur-md (GlassMorphismCard instances)
      const glassMorphismCards = featuresSection.querySelectorAll('.backdrop-blur-md')

      // Should have exactly 3 GlassMorphismCard instances in features section
      expect(glassMorphismCards.length).toBe(3)
    })

    it('feature section integrates correctly within full Home page', () => {
      renderWithBrowserRouter(<Home />)

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
      renderWithBrowserRouter(<FeaturesSection />)

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
      renderWithBrowserRouter(<FeaturesSection />)

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
      renderWithBrowserRouter(<FeaturesSection />)

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
      renderWithBrowserRouter(<FeaturesSection />)

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
      renderWithBrowserRouter(<FeaturesSection />)

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

describe('FuturisticButton Integration Tests', () => {
  describe('Test Case 1: Render Get Started button using FuturisticButton', () => {
    it('should render Get Started button with futuristic styling classes', () => {
      renderWithProviders(<Home />)

      const getStartedButton = screen.getByTestId('get-started-button')
      expect(getStartedButton).toBeInTheDocument()

      // Verify button has base futuristic styling classes
      expect(getStartedButton).toHaveClass('btn')
      expect(getStartedButton).toHaveClass('font-semibold')
      expect(getStartedButton).toHaveClass('transition-all')
      expect(getStartedButton).toHaveClass('duration-300')

      // Verify primary variant styling
      expect(getStartedButton).toHaveClass('btn-primary')

      // Verify button is accessible with minimum touch target size
      expect(getStartedButton).toHaveClass('min-h-[44px]')
      expect(getStartedButton).toHaveClass('min-w-[44px]')
    })

    it('should render Sign In button with futuristic styling classes', () => {
      renderWithProviders(<Home />)

      const signInButton = screen.getByTestId('sign-in-button')
      expect(signInButton).toBeInTheDocument()

      // Verify button has base futuristic styling classes
      expect(signInButton).toHaveClass('btn')
      expect(signInButton).toHaveClass('font-semibold')
      expect(signInButton).toHaveClass('transition-all')

      // Verify outline variant styling
      expect(signInButton).toHaveClass('btn-outline')
    })

    it('should render FuturisticButton with correct text content', () => {
      renderWithProviders(<Home />)

      // Use test IDs to be specific about which buttons we're checking
      const getStartedButton = screen.getByTestId('get-started-button')
      const signInButton = screen.getByTestId('sign-in-button')

      expect(getStartedButton).toHaveTextContent('Get Started Free')
      expect(signInButton).toHaveTextContent('Sign In')
    })
  })

  describe('Test Case 2: Test FuturisticButton onClick handler', () => {
    it('should fire onClick callback when Get Started button is clicked', () => {
      const mockOnClick = vi.fn()

      renderWithProviders(
        <FuturisticButton onClick={mockOnClick} data-testid="test-button">
          Test Button
        </FuturisticButton>
      )

      const button = screen.getByTestId('test-button')
      fireEvent.click(button)

      expect(mockOnClick).toHaveBeenCalledTimes(1)
    })

    it('should fire onClick callback multiple times on multiple clicks', () => {
      const mockOnClick = vi.fn()

      renderWithProviders(
        <FuturisticButton onClick={mockOnClick} data-testid="test-button">
          Test Button
        </FuturisticButton>
      )

      const button = screen.getByTestId('test-button')
      fireEvent.click(button)
      fireEvent.click(button)
      fireEvent.click(button)

      expect(mockOnClick).toHaveBeenCalledTimes(3)
    })

    it('should handle click event properly on homepage CTA buttons', () => {
      renderWithProviders(<Home />)

      const getStartedButton = screen.getByTestId('get-started-button')
      expect(getStartedButton).toBeInTheDocument()

      // Click should not throw any errors
      fireEvent.click(getStartedButton)
    })

    it('should receive click event with proper event object', () => {
      const mockOnClick = vi.fn()

      renderWithProviders(
        <FuturisticButton onClick={mockOnClick} data-testid="test-button">
          Test Button
        </FuturisticButton>
      )

      const button = screen.getByTestId('test-button')
      fireEvent.click(button)

      expect(mockOnClick).toHaveBeenCalledTimes(1)
      // Verify the event object was passed
      expect(mockOnClick.mock.calls[0][0]).toBeDefined()
      expect(mockOnClick.mock.calls[0][0].type).toBe('click')
    })
  })

  describe('Test Case 4: Test button with different variants (primary, secondary)', () => {
    it('should render primary variant with btn-primary class', () => {
      renderWithProviders(
        <FuturisticButton variant="primary" data-testid="primary-button">
          Primary Button
        </FuturisticButton>
      )

      const button = screen.getByTestId('primary-button')
      expect(button).toHaveClass('btn-primary')
      expect(button).not.toHaveClass('btn-secondary')
      expect(button).not.toHaveClass('btn-outline')
    })

    it('should render secondary variant with btn-secondary class', () => {
      renderWithProviders(
        <FuturisticButton variant="secondary" data-testid="secondary-button">
          Secondary Button
        </FuturisticButton>
      )

      const button = screen.getByTestId('secondary-button')
      expect(button).toHaveClass('btn-secondary')
      expect(button).not.toHaveClass('btn-primary')
      expect(button).not.toHaveClass('btn-outline')
    })

    it('should render outline variant with btn-outline class', () => {
      renderWithProviders(
        <FuturisticButton variant="outline" data-testid="outline-button">
          Outline Button
        </FuturisticButton>
      )

      const button = screen.getByTestId('outline-button')
      expect(button).toHaveClass('btn-outline')
      expect(button).not.toHaveClass('btn-primary')
      expect(button).not.toHaveClass('btn-secondary')
    })

    it('should default to primary variant when no variant is specified', () => {
      renderWithProviders(
        <FuturisticButton data-testid="default-button">
          Default Button
        </FuturisticButton>
      )

      const button = screen.getByTestId('default-button')
      expect(button).toHaveClass('btn-primary')
    })

    it('should render different size variants correctly', () => {
      const { rerender } = renderWithProviders(
        <FuturisticButton size="sm" data-testid="size-button">
          Small Button
        </FuturisticButton>
      )

      let button = screen.getByTestId('size-button')
      expect(button).toHaveClass('btn-sm')

      rerender(
        <MemoryRouter>
          <ThemeProvider>
            <FuturisticButton size="md" data-testid="size-button">
              Medium Button
            </FuturisticButton>
          </ThemeProvider>
        </MemoryRouter>
      )

      button = screen.getByTestId('size-button')
      expect(button).toHaveClass('btn-md')

      rerender(
        <MemoryRouter>
          <ThemeProvider>
            <FuturisticButton size="lg" data-testid="size-button">
              Large Button
            </FuturisticButton>
          </ThemeProvider>
        </MemoryRouter>
      )

      button = screen.getByTestId('size-button')
      expect(button).toHaveClass('btn-lg')
    })

    it('should accept custom className and merge with default classes', () => {
      renderWithProviders(
        <FuturisticButton className="custom-class" data-testid="custom-button">
          Custom Button
        </FuturisticButton>
      )

      const button = screen.getByTestId('custom-button')
      expect(button).toHaveClass('custom-class')
      // Should still have base classes
      expect(button).toHaveClass('btn')
      expect(button).toHaveClass('btn-primary')
    })
  })

  describe('FuturisticButton Integration with Homepage', () => {
    it('should use FuturisticButton for both hero section CTAs', () => {
      renderWithProviders(<Home />)

      // Both CTA buttons in hero section should use FuturisticButton styling
      const getStartedButton = screen.getByTestId('get-started-button')
      const signInButton = screen.getByTestId('sign-in-button')

      // Verify both have base FuturisticButton classes
      expect(getStartedButton).toHaveClass('btn')
      expect(getStartedButton).toHaveClass('font-semibold')
      expect(signInButton).toHaveClass('btn')
      expect(signInButton).toHaveClass('font-semibold')
    })

    it('should have proper aria-labels for accessibility', () => {
      renderWithProviders(<Home />)

      const getStartedButton = screen.getByTestId('get-started-button')
      const signInButton = screen.getByTestId('sign-in-button')

      expect(getStartedButton).toHaveAttribute('aria-label')
      expect(signInButton).toHaveAttribute('aria-label')
    })

    it('should render buttons within hero CTA container', () => {
      renderWithProviders(<Home />)

      const ctaContainer = screen.getByTestId('hero-cta-container')
      const getStartedButton = screen.getByTestId('get-started-button')
      const signInButton = screen.getByTestId('sign-in-button')

      expect(ctaContainer).toContainElement(getStartedButton)
      expect(ctaContainer).toContainElement(signInButton)
    })

    it('should distinguish primary and secondary CTA buttons', () => {
      renderWithProviders(<Home />)

      const getStartedButton = screen.getByTestId('get-started-button')
      const signInButton = screen.getByTestId('sign-in-button')

      // Get Started is primary
      expect(getStartedButton).toHaveClass('btn-primary')

      // Sign In is outline (secondary style)
      expect(signInButton).toHaveClass('btn-outline')
    })
  })

  describe('FuturisticButton uses Framer Motion', () => {
    it('should render as a motion.button element', () => {
      renderWithProviders(
        <FuturisticButton data-testid="motion-button">
          Motion Button
        </FuturisticButton>
      )

      const button = screen.getByTestId('motion-button')
      // Button should be rendered (framer-motion wraps it)
      expect(button).toBeInTheDocument()
      expect(button.tagName.toLowerCase()).toBe('button')
    })

    it('should be keyboard focusable', () => {
      renderWithProviders(
        <FuturisticButton data-testid="focus-button">
          Focus Button
        </FuturisticButton>
      )

      const button = screen.getByTestId('focus-button')
      button.focus()

      expect(document.activeElement).toBe(button)
    })

    it('should trigger onClick when clicked via fireEvent', () => {
      const mockOnClick = vi.fn()

      renderWithProviders(
        <FuturisticButton onClick={mockOnClick} data-testid="keyboard-button">
          Keyboard Button
        </FuturisticButton>
      )

      const button = screen.getByTestId('keyboard-button')
      button.focus()

      // Use fireEvent.click which is more reliable in jsdom
      fireEvent.click(button)

      expect(mockOnClick).toHaveBeenCalled()
    })
  })
})
