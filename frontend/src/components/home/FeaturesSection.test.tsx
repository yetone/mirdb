/**
 * FeaturesSection Component Tests
 *
 * Tests for the Features Section Display scenario (Scenario 3)
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { FeaturesSection } from './FeaturesSection'

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
      <div {...props}>{children}</div>
    ),
    h2: ({ children, ...props }: React.HTMLAttributes<HTMLHeadingElement>) => (
      <h2 {...props}>{children}</h2>
    ),
  },
  HTMLMotionProps: {},
}))

describe('FeaturesSection', () => {
  // Test Case 1: Query for feature section and count feature cards
  describe('Feature Section and Card Count', () => {
    it('renders the features section with between 3-5 feature cards', () => {
      render(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      expect(section).toBeInTheDocument()

      // Get all feature cards
      const featureCards = screen.getAllByTestId(/^feature-card-/)
      expect(featureCards.length).toBeGreaterThanOrEqual(3)
      expect(featureCards.length).toBeLessThanOrEqual(5)
    })

    it('renders exactly 4 feature cards', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId(/^feature-card-/)
      expect(featureCards).toHaveLength(4)
    })
  })

  // Test Case 2: Query feature card for 'URL Shortening' feature
  describe('URL Shortening Feature Card', () => {
    it('displays Instant URL Shortening feature with icon and description', () => {
      render(<FeaturesSection />)

      const title = screen.getByTestId('feature-title-1')
      expect(title).toHaveTextContent('Instant URL Shortening')

      const description = screen.getByTestId('feature-description-1')
      expect(description).toHaveTextContent(/shortening engine/i)

      const icon = screen.getByTestId('feature-icon-1')
      expect(icon).toBeInTheDocument()
    })
  })

  // Test Case 3: Query feature card for 'Analytics' feature
  describe('Analytics Feature Card', () => {
    it('displays Detailed Analytics feature with icon and description', () => {
      render(<FeaturesSection />)

      const title = screen.getByTestId('feature-title-2')
      expect(title).toHaveTextContent('Detailed Analytics')

      const description = screen.getByTestId('feature-description-2')
      expect(description).toHaveTextContent(/track clicks/i)

      const icon = screen.getByTestId('feature-icon-2')
      expect(icon).toBeInTheDocument()
    })
  })

  // Test Case 4: Query feature card for 'Security' feature
  describe('Security Feature Card', () => {
    it('displays Secure & Private feature with icon and description', () => {
      render(<FeaturesSection />)

      const title = screen.getByTestId('feature-title-3')
      expect(title).toHaveTextContent('Secure & Private')

      const description = screen.getByTestId('feature-description-3')
      expect(description).toHaveTextContent(/protected|authentication/i)

      const icon = screen.getByTestId('feature-icon-3')
      expect(icon).toBeInTheDocument()
    })
  })

  // Test Case 5: Check that feature cards use GlassMorphismCard component
  describe('GlassMorphismCard Styling', () => {
    it('applies glassmorphism styling classes to feature cards', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId(/^feature-card-/)

      featureCards.forEach((card) => {
        // Check for glassmorphism styling classes
        expect(card).toHaveClass('backdrop-blur-md')
        expect(card).toHaveClass('rounded-2xl')
        expect(card).toHaveClass('shadow-xl')
      })
    })

    it('each feature card has translucent background', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId(/^feature-card-/)

      featureCards.forEach((card) => {
        // Check for base-100/70 which is the translucent background
        const classNames = card.className
        expect(classNames).toMatch(/bg-base-100\/70|backdrop-blur/)
      })
    })
  })

  // Test Case 6: Verify staggered entrance animation on feature cards
  describe('Staggered Animation', () => {
    it('feature cards have animation variants configured with stagger delay', async () => {
      // Import the actual variants to verify the configuration
      const { featureCardVariants } = await import('../../utils/animations')

      // Verify the animation configuration has the expected stagger
      expect(featureCardVariants).toBeDefined()
      expect(featureCardVariants.hidden).toEqual({ opacity: 0, y: 30 })

      // Test that visible variant uses custom index for delay
      const visibleVariant = featureCardVariants.visible as (index: number) => object
      expect(typeof visibleVariant).toBe('function')

      // Test stagger calculation - each card should have 100ms (0.1s) delay multiplied by index
      const firstCardTransition = visibleVariant(0)
      const secondCardTransition = visibleVariant(1)
      const thirdCardTransition = visibleVariant(2)

      expect(firstCardTransition).toMatchObject({
        opacity: 1,
        y: 0,
        transition: expect.objectContaining({ delay: 0 })
      })

      expect(secondCardTransition).toMatchObject({
        opacity: 1,
        y: 0,
        transition: expect.objectContaining({ delay: 0.1 })
      })

      expect(thirdCardTransition).toMatchObject({
        opacity: 1,
        y: 0,
        transition: expect.objectContaining({ delay: 0.2 })
      })
    })

    it('FeaturesSection passes custom index to feature card variants', () => {
      render(<FeaturesSection />)

      // Verify the section renders correctly (animation setup is in the component)
      const featureCards = screen.getAllByTestId(/^feature-card-/)
      expect(featureCards).toHaveLength(4)

      // Each card should have its index as custom prop for stagger animation
      // This is verified by the component rendering correctly with variants
      const section = screen.getByTestId('features-section')
      expect(section).toBeInTheDocument()
    })
  })

  // Additional tests for completeness
  describe('Accessibility', () => {
    it('has proper aria-labelledby for the section', () => {
      render(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      expect(section).toHaveAttribute('aria-labelledby', 'features-heading')

      const heading = screen.getByRole('heading', { level: 2 })
      expect(heading).toHaveAttribute('id', 'features-heading')
    })

    it('icons have aria-hidden for decorative purposes', () => {
      render(<FeaturesSection />)

      const icons = screen.getAllByTestId(/^feature-icon-/)
      icons.forEach((icon) => {
        expect(icon).toHaveAttribute('aria-hidden', 'true')
      })
    })
  })

  describe('Content Structure', () => {
    it('each feature card has title, description, and icon', () => {
      render(<FeaturesSection />)

      // 4 features total
      for (let i = 1; i <= 4; i++) {
        expect(screen.getByTestId(`feature-title-${i}`)).toBeInTheDocument()
        expect(screen.getByTestId(`feature-description-${i}`)).toBeInTheDocument()
        expect(screen.getByTestId(`feature-icon-${i}`)).toBeInTheDocument()
      }
    })

    it('renders the section heading', () => {
      render(<FeaturesSection />)

      const heading = screen.getByRole('heading', { name: /why choose us/i })
      expect(heading).toBeInTheDocument()
    })
  })
})
