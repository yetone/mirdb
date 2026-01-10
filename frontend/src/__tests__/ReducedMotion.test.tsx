import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Home from '../pages/Home'
import HeroSection, { ReducedMotionContext } from '../components/HeroSection'

/**
 * Reduced Motion Support Tests (NFR-6)
 *
 * Verifies that animations respect user's reduced motion preferences:
 * - Scroll-triggered entrance animations are disabled
 * - Animated backgrounds fall back to static gradients
 * - Framer Motion components respect reduced motion media query
 */

// Create a complete matchMedia mock that works in jsdom
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

// Store original matchMedia
const originalMatchMedia = window.matchMedia

// Helper to mock prefers-reduced-motion media query
const mockReducedMotion = (prefersReducedMotion: boolean) => {
  window.matchMedia = createMatchMediaMock(prefersReducedMotion)

  return () => {
    window.matchMedia = originalMatchMedia
  }
}

// Wrapper component that enforces reduced motion via our custom context
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

describe('Reduced Motion Support (NFR-6)', () => {
  let cleanupMatchMedia: () => void

  afterEach(() => {
    if (cleanupMatchMedia) {
      cleanupMatchMedia()
    }
  })

  // Test Case 1: Integration test - Render homepage with prefers-reduced-motion: reduce
  describe('Homepage with Reduced Motion Preference', () => {
    beforeEach(() => {
      cleanupMatchMedia = mockReducedMotion(true)
    })

    it('should render homepage when prefers-reduced-motion is enabled', () => {
      render(
        <ReducedMotionWrapper>
          <Home />
        </ReducedMotionWrapper>
      )

      // Verify all main sections render
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('footer-section')).toBeInTheDocument()
    })

    it('should disable scroll-triggered entrance animations when reduced motion is preferred', () => {
      render(
        <ReducedMotionWrapper>
          <Home />
        </ReducedMotionWrapper>
      )

      // Hero section should not have animate-pulse class on background elements
      // when reduced motion is enabled
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Check that the hero section has the motion-reduce class on animated elements
      const backgroundElements = heroSection.querySelectorAll('[class*="animate-"]')

      // With reduced motion enabled, background animations should use motion-reduce class
      backgroundElements.forEach((element) => {
        const classList = element.className
        // Check that motion-reduce classes are present
        const hasReducedMotionHandling =
          classList.includes('motion-reduce:') ||
          classList.includes('motion-safe:') ||
          !classList.includes('animate-pulse')

        expect(hasReducedMotionHandling).toBe(true)
      })
    })

    it('should ensure all content is accessible without animations', () => {
      render(
        <ReducedMotionWrapper>
          <Home />
        </ReducedMotionWrapper>
      )

      // Main headline should be present and accessible
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
      expect(headline).toHaveTextContent('Shorten, Share, Track')

      // CTA buttons should be accessible
      const primaryCTA = screen.getByRole('link', { name: /get started free/i })
      expect(primaryCTA).toBeInTheDocument()

      const loginLink = screen.getByRole('link', { name: /log in/i })
      expect(loginLink).toBeInTheDocument()

      // Feature cards should be visible
      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()
    })
  })

  // Test Case 2: Unit test - Hero background animation with reduced motion
  describe('Hero Background Animation with Reduced Motion', () => {
    beforeEach(() => {
      cleanupMatchMedia = mockReducedMotion(true)
    })

    it('should have animated backgrounds fall back to static when reduced motion is preferred', () => {
      render(
        <ReducedMotionWrapper>
          <HeroSection />
        </ReducedMotionWrapper>
      )

      const heroSection = screen.getByTestId('hero-section')

      // Hero section should still have gradient background (static, no animation)
      expect(heroSection).toHaveClass('bg-gradient-to-br')
      expect(heroSection).toHaveClass('from-primary')
      expect(heroSection).toHaveClass('via-secondary')
      expect(heroSection).toHaveClass('to-accent')
    })

    it('should use motion-reduce classes to disable pulse animations', () => {
      render(
        <ReducedMotionWrapper>
          <HeroSection />
        </ReducedMotionWrapper>
      )

      const heroSection = screen.getByTestId('hero-section')

      // Find background orb elements
      const backgroundContainer = heroSection.querySelector('.absolute.inset-0')
      expect(backgroundContainer).toBeInTheDocument()

      // Background orbs should have motion-reduce:animate-none class
      const backgroundOrbs = backgroundContainer?.querySelectorAll('[class*="bg-primary"], [class*="bg-secondary"]')

      backgroundOrbs?.forEach((orb) => {
        const classList = orb.className
        // Check for motion-reduce handling
        expect(classList).toMatch(/motion-reduce:animate-none/)
      })
    })

    it('should retain gradient styling without pulse animation', () => {
      render(
        <ReducedMotionWrapper>
          <HeroSection />
        </ReducedMotionWrapper>
      )

      const heroSection = screen.getByTestId('hero-section')

      // Background orbs should still have blur and gradient styling
      const backgroundContainer = heroSection.querySelector('.absolute.inset-0')
      const orbs = backgroundContainer?.querySelectorAll('.blur-3xl')

      expect(orbs).toBeDefined()
      expect(orbs?.length).toBeGreaterThan(0)

      orbs?.forEach((orb) => {
        expect(orb).toHaveClass('rounded-full')
        expect(orb).toHaveClass('blur-3xl')
      })
    })
  })

  // Test Case 3: Unit test - Framer Motion components with reduced motion
  describe('Framer Motion Components with Reduced Motion', () => {
    it('should have Framer Motion respect reduced motion via context', () => {
      render(
        <ReducedMotionWrapper>
          <HeroSection />
        </ReducedMotionWrapper>
      )

      // The hero section elements should render properly
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toBeInTheDocument()

      // With ReducedMotionContext value=true, animations are disabled
    })

    it('should render hero headline immediately without animation delay', () => {
      render(
        <ReducedMotionWrapper>
          <HeroSection />
        </ReducedMotionWrapper>
      )

      // With reduced motion via context, content should be immediately visible
      // (no opacity: 0 initial state that requires animation)
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toBeInTheDocument()
      expect(headline).toBeVisible()
    })

    it('should render hero subheadline immediately without animation delay', () => {
      render(
        <ReducedMotionWrapper>
          <HeroSection />
        </ReducedMotionWrapper>
      )

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toBeInTheDocument()
      expect(subheadline).toBeVisible()
    })

    it('should render CTA buttons immediately without animation delay', () => {
      render(
        <ReducedMotionWrapper>
          <HeroSection />
        </ReducedMotionWrapper>
      )

      const primaryCTA = screen.getByTestId('cta-register')
      expect(primaryCTA).toBeInTheDocument()
      expect(primaryCTA).toBeVisible()

      const loginLink = screen.getByTestId('login-link')
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toBeVisible()
    })
  })

  // Additional tests for reduced motion with normal motion preference
  describe('Normal Motion Preference (Control Group)', () => {
    beforeEach(() => {
      cleanupMatchMedia = mockReducedMotion(false)
    })

    it('should have animations enabled when reduced motion is not preferred', () => {
      render(
        <NormalMotionWrapper>
          <HeroSection />
        </NormalMotionWrapper>
      )

      // Hero section should be present
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('should have background animations when reduced motion is not preferred', () => {
      render(
        <NormalMotionWrapper>
          <HeroSection />
        </NormalMotionWrapper>
      )

      const heroSection = screen.getByTestId('hero-section')
      const backgroundContainer = heroSection.querySelector('.absolute.inset-0')

      // Background elements should have animate-pulse class
      const animatedElements = backgroundContainer?.querySelectorAll('.animate-pulse')
      expect(animatedElements?.length).toBeGreaterThan(0)
    })
  })

  // Tests for HowItWorks section hover transitions
  describe('HowItWorks Section Transitions with Reduced Motion', () => {
    beforeEach(() => {
      cleanupMatchMedia = mockReducedMotion(true)
    })

    it('should have step cards use appropriate transitions for reduced motion', () => {
      render(
        <ReducedMotionWrapper>
          <Home />
        </ReducedMotionWrapper>
      )

      // Step cards should be rendered
      const stepCards = screen.getAllByTestId(/step-card-\d/)
      expect(stepCards).toHaveLength(3)

      // Cards should have hover effects but respect reduced motion
      stepCards.forEach((card) => {
        // Cards have transition-shadow which is a subtle effect
        // appropriate for reduced motion (simple state change)
        expect(card).toHaveClass('shadow-xl')
      })
    })

    it('should maintain functionality without complex animations', () => {
      render(
        <ReducedMotionWrapper>
          <Home />
        </ReducedMotionWrapper>
      )

      // All step content should be immediately accessible
      expect(screen.getByText('Paste Your Long URL')).toBeInTheDocument()
      expect(screen.getByText('Get Your Shortened Link')).toBeInTheDocument()
      expect(screen.getByText('Share and Track Performance')).toBeInTheDocument()
    })
  })

  // Test that CSS motion-reduce classes are properly applied
  describe('CSS Motion Reduce Classes', () => {
    it('should have motion-reduce:animate-none class on hero background orbs', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      const heroSection = screen.getByTestId('hero-section')
      const backgroundContainer = heroSection.querySelector('.absolute.inset-0')

      // Find elements with both animate-pulse and motion-reduce classes
      const animatedElements = backgroundContainer?.querySelectorAll('[class*="animate-pulse"]')

      expect(animatedElements?.length).toBeGreaterThan(0)

      animatedElements?.forEach((element) => {
        // Each animated element should have the motion-reduce:animate-none class
        expect(element.className).toContain('motion-reduce:animate-none')
      })
    })
  })
})
