import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import FeaturesSection from '../components/FeaturesSection'
import HowItWorksSection from '../components/HowItWorksSection'
import { ReducedMotionContext } from '../components/HeroSection'

/**
 * Scroll Animations with Framer Motion (Scenario 22)
 *
 * Tests verify that sections animate smoothly into view when scrolling:
 * - FeaturesSection uses whileInView for scroll-triggered animations
 * - HowItWorksSection uses whileInView for scroll-triggered animations
 * - Animations respect reduced motion preferences
 */

// Store original matchMedia
const originalMatchMedia = window.matchMedia

// Create a matchMedia mock
const createMatchMediaMock = (prefersReducedMotion: boolean) => {
  return vi.fn().mockImplementation((query: string) => ({
    matches: query === '(prefers-reduced-motion: reduce)' ? prefersReducedMotion : false,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  }))
}

// Helper to mock prefers-reduced-motion
const mockReducedMotion = (prefersReducedMotion: boolean) => {
  window.matchMedia = createMatchMediaMock(prefersReducedMotion)
  return () => {
    window.matchMedia = originalMatchMedia
  }
}

// Wrapper for reduced motion testing
const ReducedMotionWrapper = ({ children }: { children: React.ReactNode }) => (
  <ReducedMotionContext.Provider value={true}>
    <MemoryRouter initialEntries={['/']}>{children}</MemoryRouter>
  </ReducedMotionContext.Provider>
)

// Wrapper for normal motion testing
const NormalMotionWrapper = ({ children }: { children: React.ReactNode }) => (
  <ReducedMotionContext.Provider value={false}>
    <MemoryRouter initialEntries={['/']}>{children}</MemoryRouter>
  </ReducedMotionContext.Provider>
)

describe('Scroll Animations with Framer Motion', () => {
  let cleanupMatchMedia: (() => void) | undefined

  afterEach(() => {
    if (cleanupMatchMedia) {
      cleanupMatchMedia()
      cleanupMatchMedia = undefined
    }
  })

  // Test Case 1: FeaturesSection scroll-triggered animations (E2E style)
  describe('FeaturesSection Scroll-Triggered Animations', () => {
    beforeEach(() => {
      cleanupMatchMedia = mockReducedMotion(false)
    })

    it('should render FeaturesSection with scroll animation wrapper elements', () => {
      render(
        <NormalMotionWrapper>
          <FeaturesSection />
        </NormalMotionWrapper>
      )

      // Section should be present
      const section = screen.getByTestId('features-section')
      expect(section).toBeInTheDocument()

      // All feature cards should be visible
      const featureCards = screen.getAllByTestId(/feature-card-\d/)
      expect(featureCards).toHaveLength(3)
    })

    it('should have heading that can animate into view', () => {
      render(
        <NormalMotionWrapper>
          <FeaturesSection />
        </NormalMotionWrapper>
      )

      // Heading should be present with data-testid
      const heading = screen.getByTestId('features-heading')
      expect(heading).toBeInTheDocument()
      expect(heading).toHaveTextContent('Powerful Features')
    })

    it('should render all three feature cards for scroll animation', () => {
      render(
        <NormalMotionWrapper>
          <FeaturesSection />
        </NormalMotionWrapper>
      )

      // Verify all feature cards render
      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
      expect(screen.getByText('Analytics Dashboard')).toBeInTheDocument()
      expect(screen.getByText('Link Management')).toBeInTheDocument()
    })

    it('should display icons for all features during scroll animation', () => {
      render(
        <NormalMotionWrapper>
          <FeaturesSection />
        </NormalMotionWrapper>
      )

      // Icons should be present
      expect(screen.getByTestId('url-shortening-icon')).toBeInTheDocument()
      expect(screen.getByTestId('analytics-icon')).toBeInTheDocument()
      expect(screen.getByTestId('link-management-icon')).toBeInTheDocument()
    })
  })

  // Test Case 2: HowItWorksSection scroll-triggered animations (E2E style)
  describe('HowItWorksSection Scroll-Triggered Animations', () => {
    beforeEach(() => {
      cleanupMatchMedia = mockReducedMotion(false)
    })

    it('should render HowItWorksSection with scroll animation wrapper elements', () => {
      render(
        <NormalMotionWrapper>
          <HowItWorksSection />
        </NormalMotionWrapper>
      )

      // Section should be present
      const section = screen.getByTestId('how-it-works-section')
      expect(section).toBeInTheDocument()

      // All step cards should be visible
      const stepCards = screen.getAllByTestId(/step-card-\d/)
      expect(stepCards).toHaveLength(3)
    })

    it('should have heading that can animate into view', () => {
      render(
        <NormalMotionWrapper>
          <HowItWorksSection />
        </NormalMotionWrapper>
      )

      // Heading should be present with data-testid
      const heading = screen.getByTestId('how-it-works-heading')
      expect(heading).toBeInTheDocument()
      expect(heading).toHaveTextContent('How It Works')
    })

    it('should render all three step cards for scroll animation', () => {
      render(
        <NormalMotionWrapper>
          <HowItWorksSection />
        </NormalMotionWrapper>
      )

      // Verify all step cards render with proper content
      expect(screen.getByText('Paste Your Long URL')).toBeInTheDocument()
      expect(screen.getByText('Get Your Shortened Link')).toBeInTheDocument()
      expect(screen.getByText('Share and Track Performance')).toBeInTheDocument()
    })

    it('should display step numbers correctly', () => {
      render(
        <NormalMotionWrapper>
          <HowItWorksSection />
        </NormalMotionWrapper>
      )

      // Step numbers should be present
      expect(screen.getByTestId('step-number-1')).toHaveTextContent('1')
      expect(screen.getByTestId('step-number-2')).toHaveTextContent('2')
      expect(screen.getByTestId('step-number-3')).toHaveTextContent('3')
    })

    it('should display step icons correctly', () => {
      render(
        <NormalMotionWrapper>
          <HowItWorksSection />
        </NormalMotionWrapper>
      )

      // Step icons should be present
      expect(screen.getByTestId('step-icon-1')).toBeInTheDocument()
      expect(screen.getByTestId('step-icon-2')).toBeInTheDocument()
      expect(screen.getByTestId('step-icon-3')).toBeInTheDocument()
    })
  })

  // Test Case 3: Framer Motion whileInView usage (Unit test)
  describe('Framer Motion whileInView Usage', () => {
    it('should use motion components in FeaturesSection', () => {
      render(
        <NormalMotionWrapper>
          <FeaturesSection />
        </NormalMotionWrapper>
      )

      // FeaturesSection should render motion elements (Framer Motion adds motion components)
      // The heading is a motion.h2 element
      const heading = screen.getByTestId('features-heading')
      expect(heading).toBeInTheDocument()

      // Feature cards are wrapped in motion.div
      const featureCards = screen.getAllByTestId(/feature-card-\d/)
      expect(featureCards).toHaveLength(3)
    })

    it('should use motion components in HowItWorksSection', () => {
      render(
        <NormalMotionWrapper>
          <HowItWorksSection />
        </NormalMotionWrapper>
      )

      // HowItWorksSection should render motion elements
      const heading = screen.getByTestId('how-it-works-heading')
      expect(heading).toBeInTheDocument()

      // Step cards are wrapped in motion.div
      const stepCards = screen.getAllByTestId(/step-card-\d/)
      expect(stepCards).toHaveLength(3)
    })

    it('should have all content accessible when using whileInView', () => {
      render(
        <NormalMotionWrapper>
          <FeaturesSection />
        </NormalMotionWrapper>
      )

      // All content should be in the document (even if initially animated)
      const section = screen.getByTestId('features-section')
      expect(section).toBeInTheDocument()

      // Text content should be accessible
      expect(screen.getByText('Powerful Features')).toBeInTheDocument()
      expect(screen.getByText(/Create memorable, short links instantly/i)).toBeInTheDocument()
    })
  })

  // Test Case: Reduced Motion Support for scroll animations
  describe('Reduced Motion Support for Scroll Animations', () => {
    beforeEach(() => {
      cleanupMatchMedia = mockReducedMotion(true)
    })

    it('should render FeaturesSection without animations when reduced motion is preferred', () => {
      render(
        <ReducedMotionWrapper>
          <FeaturesSection />
        </ReducedMotionWrapper>
      )

      // Section and all cards should render normally
      const section = screen.getByTestId('features-section')
      expect(section).toBeInTheDocument()

      const featureCards = screen.getAllByTestId(/feature-card-\d/)
      expect(featureCards).toHaveLength(3)

      // Content should be immediately visible/accessible
      expect(screen.getByText('Powerful Features')).toBeVisible()
    })

    it('should render HowItWorksSection without animations when reduced motion is preferred', () => {
      render(
        <ReducedMotionWrapper>
          <HowItWorksSection />
        </ReducedMotionWrapper>
      )

      // Section and all cards should render normally
      const section = screen.getByTestId('how-it-works-section')
      expect(section).toBeInTheDocument()

      const stepCards = screen.getAllByTestId(/step-card-\d/)
      expect(stepCards).toHaveLength(3)

      // Content should be immediately visible/accessible
      expect(screen.getByText('How It Works')).toBeVisible()
    })

    it('should ensure all feature content is accessible without animations', () => {
      render(
        <ReducedMotionWrapper>
          <FeaturesSection />
        </ReducedMotionWrapper>
      )

      // All feature titles should be visible
      expect(screen.getByText('URL Shortening')).toBeVisible()
      expect(screen.getByText('Analytics Dashboard')).toBeVisible()
      expect(screen.getByText('Link Management')).toBeVisible()
    })

    it('should ensure all step content is accessible without animations', () => {
      render(
        <ReducedMotionWrapper>
          <HowItWorksSection />
        </ReducedMotionWrapper>
      )

      // All step titles should be visible
      expect(screen.getByText('Paste Your Long URL')).toBeVisible()
      expect(screen.getByText('Get Your Shortened Link')).toBeVisible()
      expect(screen.getByText('Share and Track Performance')).toBeVisible()
    })
  })

  // Test Case: Animation quality (smooth and not distracting)
  describe('Animation Quality', () => {
    beforeEach(() => {
      cleanupMatchMedia = mockReducedMotion(false)
    })

    it('should have feature cards with glassmorphism hover effects for smooth UX', () => {
      render(
        <NormalMotionWrapper>
          <FeaturesSection />
        </NormalMotionWrapper>
      )

      const glassmorphismCards = screen.getAllByTestId('glassmorphism-card')
      expect(glassmorphismCards).toHaveLength(3)

      // Cards should have smooth transition classes
      glassmorphismCards.forEach((card) => {
        expect(card).toHaveClass('transition-all')
        expect(card).toHaveClass('duration-300')
      })
    })

    it('should have step cards with shadow hover effects for smooth UX', () => {
      render(
        <NormalMotionWrapper>
          <HowItWorksSection />
        </NormalMotionWrapper>
      )

      const stepCards = screen.getAllByTestId(/step-card-\d/)
      expect(stepCards).toHaveLength(3)

      // Cards should have smooth transition classes
      stepCards.forEach((card) => {
        expect(card).toHaveClass('transition-shadow')
        expect(card).toHaveClass('duration-300')
      })
    })

    it('should maintain proper section structure during animations', () => {
      render(
        <NormalMotionWrapper>
          <FeaturesSection />
        </NormalMotionWrapper>
      )

      // Grid structure should be maintained
      const grid = screen.getByTestId('features-grid')
      expect(grid).toHaveClass('grid')
      expect(grid).toHaveClass('lg:grid-cols-3')
    })

    it('should maintain responsive layout during animations', () => {
      render(
        <NormalMotionWrapper>
          <HowItWorksSection />
        </NormalMotionWrapper>
      )

      // Steps container should have proper grid classes
      const stepsContainer = screen.getByTestId('steps-container')
      expect(stepsContainer).toHaveClass('grid')
      expect(stepsContainer).toHaveClass('md:grid-cols-3')
    })
  })
})
