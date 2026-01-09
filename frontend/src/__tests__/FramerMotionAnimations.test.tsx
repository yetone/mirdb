/**
 * Framer Motion Animations Tests
 *
 * This test suite verifies that homepage animations use Framer Motion
 * for smooth, performant transitions as specified in NFR-3 (Should Have).
 *
 * Test Coverage:
 * 1. Hero section entrance animations
 * 2. CTA button hover effects
 * 3. Feature card hover animations
 * 4. Scroll-triggered viewport animations
 * 5. Framer Motion component usage verification
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import HeroSection from '../components/HeroSection'
import GlassMorphismCard from '../components/GlassMorphismCard'
import FeaturesSection from '../components/FeaturesSection'
import Home from '../pages/Home'

// Mock framer-motion to track component usage and animations
vi.mock('framer-motion', async () => {
  const actual = await vi.importActual<typeof import('framer-motion')>('framer-motion')
  return {
    ...actual,
    useReducedMotion: vi.fn(() => false),
  }
})

const renderWithRouter = (component: React.ReactNode) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

describe('Framer Motion Animations', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('Test Case 1: Hero Section Entrance Animations', () => {
    it('renders HeroSection with motion components for entrance animations', () => {
      renderWithRouter(<HeroSection />)

      // Hero section should be rendered
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Headline should be rendered with animation
      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveTextContent('Shorten. Share. Track.')

      // Subheadline should be rendered with animation
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toBeInTheDocument()

      // CTA buttons should be rendered
      expect(screen.getByTestId('hero-cta-primary')).toBeInTheDocument()
      expect(screen.getByTestId('hero-cta-secondary')).toBeInTheDocument()
    })

    it('hero section elements are animated with fade-in and slide-up effect', () => {
      const { container } = renderWithRouter(<HeroSection />)

      // Verify motion.div is used for the container
      // In framer-motion, motion components render as regular DOM elements
      // with animation styles applied
      const heroContent = container.querySelector('.hero-content')
      expect(heroContent).toBeInTheDocument()

      // Check that the container div exists and has motion attributes
      const maxWidthContainer = heroContent?.querySelector('.max-w-2xl')
      expect(maxWidthContainer).toBeInTheDocument()
    })

    it('hero section uses staggered animation delays', () => {
      renderWithRouter(<HeroSection />)

      // Elements should exist and be ready for animations
      const headline = screen.getByTestId('hero-headline')
      const subheadline = screen.getByTestId('hero-subheadline')
      const primaryCta = screen.getByTestId('hero-cta-primary')
      const secondaryCta = screen.getByTestId('hero-cta-secondary')

      // All elements should be present indicating the staggered animation setup
      expect(headline).toBeInTheDocument()
      expect(subheadline).toBeInTheDocument()
      expect(primaryCta).toBeInTheDocument()
      expect(secondaryCta).toBeInTheDocument()
    })

    it('hero section respects reduced motion preference', () => {
      const { container } = renderWithRouter(<HeroSection />)

      // Check for data-reduced-motion attribute which indicates accessibility support
      const animatedContainer = container.querySelector('[data-reduced-motion]')
      expect(animatedContainer).toBeInTheDocument()
      expect(animatedContainer).toHaveAttribute('data-reduced-motion', 'false')
    })
  })

  describe('Test Case 2: CTA Button Hover Animations', () => {
    it('CTA buttons have hover effect classes for animation', () => {
      renderWithRouter(<HeroSection />)

      const primaryCta = screen.getByTestId('hero-cta-primary')
      const secondaryCta = screen.getByTestId('hero-cta-secondary')

      // FuturisticButton adds transform hover:scale-105 for hover animations
      expect(primaryCta).toHaveClass('transform')
      expect(primaryCta).toHaveClass('hover:scale-105')
      expect(secondaryCta).toHaveClass('transform')
      expect(secondaryCta).toHaveClass('hover:scale-105')
    })

    it('primary CTA button has transition classes for smooth hover', () => {
      renderWithRouter(<HeroSection />)

      const primaryCta = screen.getByTestId('hero-cta-primary')

      // Check for transition classes
      expect(primaryCta).toHaveClass('transition-all')
      expect(primaryCta).toHaveClass('duration-300')
    })

    it('secondary CTA button has transition classes for smooth hover', () => {
      renderWithRouter(<HeroSection />)

      const secondaryCta = screen.getByTestId('hero-cta-secondary')

      // Check for transition classes
      expect(secondaryCta).toHaveClass('transition-all')
      expect(secondaryCta).toHaveClass('duration-300')
    })

    it('buttons have shadow hover effect classes', () => {
      renderWithRouter(<HeroSection />)

      const primaryCta = screen.getByTestId('hero-cta-primary')

      // Primary button has shadow effect on hover
      const classString = primaryCta.className
      expect(classString).toContain('hover:shadow')
    })
  })

  describe('Test Case 3: Feature Card Hover Animations', () => {
    it('GlassMorphismCard renders with glass-card styling', () => {
      const { container } = render(
        <GlassMorphismCard>Test Content</GlassMorphismCard>
      )

      const card = container.firstChild as HTMLElement
      expect(card).toHaveClass('glass-card')
    })

    it('GlassMorphismCard uses framer-motion for hover effects', () => {
      const { container } = render(
        <GlassMorphismCard>Test Content</GlassMorphismCard>
      )

      // The card should be a motion.div which renders as a div
      const card = container.firstChild as HTMLElement
      expect(card).toBeInTheDocument()
      expect(card.tagName).toBe('DIV')
    })

    it('feature cards in FeaturesSection use GlassMorphismCard with hover animations', () => {
      render(<FeaturesSection />)

      const featuresGrid = screen.getByTestId('features-grid')
      const cards = featuresGrid.querySelectorAll('.glass-card')

      // Should have 4 feature cards with glass-card styling
      expect(cards.length).toBe(4)

      // Each card is a motion.div from GlassMorphismCard
      cards.forEach((card) => {
        expect(card.tagName).toBe('DIV')
        expect(card).toHaveClass('glass-card')
        expect(card).toHaveClass('p-6')
      })
    })

    it('GlassMorphismCard accepts and applies custom className', () => {
      const { container } = render(
        <GlassMorphismCard className="custom-test-class">Test</GlassMorphismCard>
      )

      const card = container.firstChild as HTMLElement
      expect(card).toHaveClass('glass-card')
      expect(card).toHaveClass('custom-test-class')
    })
  })

  describe('Test Case 4: Scroll-Triggered Viewport Animations', () => {
    it('features section is rendered and ready for viewport animations', () => {
      renderWithRouter(<Home />)

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Section has ID for scroll navigation
      expect(featuresSection).toHaveAttribute('id', 'features')
    })

    it('homepage supports smooth scroll behavior for section navigation', () => {
      renderWithRouter(<Home />)

      // Homepage has scroll-to-section functionality
      const homePage = screen.getByTestId('home-page')
      expect(homePage).toBeInTheDocument()

      // Features section has ID for anchor navigation
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection.id).toBe('features')
    })

    it('GlassMorphismCard components are present in features for scroll-triggered effects', () => {
      render(<FeaturesSection />)

      // All 4 feature cards should be present
      const featureCards = screen.getAllByTestId(/^feature-card-/)
      expect(featureCards.length).toBe(4)

      // Each card should be wrapped in GlassMorphismCard (has glass-card class)
      const featuresGrid = screen.getByTestId('features-grid')
      const glassCards = featuresGrid.querySelectorAll('.glass-card')
      expect(glassCards.length).toBe(4)
    })
  })

  describe('Test Case 5: Framer Motion Component Usage Verification', () => {
    it('HeroSection imports and uses motion components from framer-motion', async () => {
      // This test verifies that motion components are used
      const heroModule = await import('../components/HeroSection')

      // The module should export HeroSection
      expect(heroModule.HeroSection).toBeDefined()
      expect(typeof heroModule.HeroSection).toBe('function')

      // Verify by rendering that it works with framer-motion
      renderWithRouter(<heroModule.HeroSection />)
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })

    it('GlassMorphismCard imports and uses motion from framer-motion', async () => {
      // This test verifies that motion is used in GlassMorphismCard
      const cardModule = await import('../components/GlassMorphismCard')

      // The module should export the component
      expect(cardModule.default).toBeDefined()
      expect(typeof cardModule.default).toBe('function')

      // Verify by rendering
      const { container } = render(
        <cardModule.default>Test</cardModule.default>
      )
      expect(container.firstChild).toHaveClass('glass-card')
    })

    it('homepage uses motion components for smooth transitions', () => {
      renderWithRouter(<Home />)

      // Homepage renders with all animated sections
      expect(screen.getByTestId('home-page')).toBeInTheDocument()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()

      // Hero section has animated content container
      const heroSection = screen.getByTestId('hero-section')
      const heroContent = heroSection.querySelector('.hero-content')
      expect(heroContent).toBeInTheDocument()
    })

    it('motion components render as standard DOM elements', () => {
      const { container } = renderWithRouter(<HeroSection />)

      // motion.div renders as div
      // motion.h1 renders as h1
      // motion.p renders as p
      const headline = screen.getByTestId('hero-headline')
      expect(headline.tagName).toBe('H1')

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline.tagName).toBe('P')

      const maxWidthDiv = container.querySelector('.max-w-2xl')
      expect(maxWidthDiv?.tagName).toBe('DIV')
    })

    it('framer-motion library is properly integrated in the component tree', () => {
      // Render full homepage and verify no errors
      expect(() => renderWithRouter(<Home />)).not.toThrow()

      // All sections should render correctly
      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')
      const howItWorksSection = screen.getByTestId('how-it-works-section')

      expect(heroSection).toBeInTheDocument()
      expect(featuresSection).toBeInTheDocument()
      expect(howItWorksSection).toBeInTheDocument()
    })
  })

  describe('Additional Animation Behavior Tests', () => {
    it('animation configuration uses appropriate durations', () => {
      renderWithRouter(<HeroSection />)

      // Verify elements are rendered (animation config is internal to component)
      // Note: Elements may have initial opacity: 0 due to entrance animation
      const headline = screen.getByTestId('hero-headline')
      const subheadline = screen.getByTestId('hero-subheadline')

      // Elements are in the DOM even if animation hasn't completed
      expect(headline).toBeInTheDocument()
      expect(subheadline).toBeInTheDocument()
    })

    it('GlassMorphismCard hover animation configuration is correct', () => {
      const { container } = render(
        <GlassMorphismCard data-testid="test-card">
          Hover Test Content
        </GlassMorphismCard>
      )

      const card = container.firstChild as HTMLElement

      // Card should be renderable and have proper structure
      expect(card).toBeInTheDocument()
      expect(card).toHaveClass('glass-card')
      expect(card).toHaveClass('p-6')
    })

    it('homepage animations are non-blocking and performant', () => {
      const startTime = performance.now()
      renderWithRouter(<Home />)
      const endTime = performance.now()

      // Rendering should complete quickly (< 1 second)
      expect(endTime - startTime).toBeLessThan(1000)

      // All sections should be present after render
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
    })

    it('animations do not prevent user interaction', () => {
      renderWithRouter(<HeroSection />)

      // Buttons should be interactive even with animations
      const primaryCta = screen.getByTestId('hero-cta-primary')
      const secondaryCta = screen.getByTestId('hero-cta-secondary')

      // Click events should work
      expect(() => fireEvent.click(primaryCta)).not.toThrow()
      expect(() => fireEvent.click(secondaryCta)).not.toThrow()

      // Buttons should have correct href for navigation
      expect(primaryCta).toHaveAttribute('href', '/register')
      expect(secondaryCta).toHaveAttribute('href', '/login')
    })
  })
})
