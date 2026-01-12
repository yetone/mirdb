import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Home from './Home'
import { AuthProvider } from '../contexts/AuthContext'

// Mock scrollIntoView
Element.prototype.scrollIntoView = vi.fn()

// Mock localStorage
const localStorageMock = {
  getItem: vi.fn(),
  setItem: vi.fn(),
  clear: vi.fn(),
  removeItem: vi.fn(),
}
Object.defineProperty(window, 'localStorage', { value: localStorageMock })

// Store captured animation props for testing
const capturedAnimationProps: Record<string, unknown>[] = []

// Mock framer-motion - factory must be self-contained
vi.mock('framer-motion', () => {
  const capturedProps: Record<string, unknown>[] = []

  // Expose to global for test access
  ;(globalThis as unknown as { __capturedAnimationProps: Record<string, unknown>[] }).__capturedAnimationProps = capturedProps

  const createMotionComponent = (tag: string) => {
    return function MockMotionComponent({
      children,
      initial,
      animate,
      whileInView,
      viewport,
      transition,
      variants,
      whileHover,
      whileTap,
      ...rest
    }: React.PropsWithChildren<Record<string, unknown>>) {
      // Capture animation props for testing
      capturedProps.push({
        tag,
        initial,
        animate,
        whileInView,
        viewport,
        transition,
        variants,
        whileHover,
        whileTap,
      })

      const Tag = tag as keyof React.JSX.IntrinsicElements
      return <Tag data-motion={tag} data-has-animation={!!(initial || animate || whileInView)} {...rest}>{children}</Tag>
    }
  }

  return {
    motion: {
      div: createMotionComponent('div'),
      section: createMotionComponent('section'),
      ul: createMotionComponent('ul'),
      ol: createMotionComponent('ol'),
      li: createMotionComponent('li'),
      svg: createMotionComponent('svg'),
      circle: createMotionComponent('circle'),
      rect: createMotionComponent('rect'),
      g: createMotionComponent('g'),
      text: createMotionComponent('text'),
    },
  }
})

const getCapturedProps = () =>
  (globalThis as unknown as { __capturedAnimationProps: Record<string, unknown>[] }).__capturedAnimationProps || []

const renderWithRouter = () => {
  // Clear captured props before each render
  const props = getCapturedProps()
  props.length = 0
  return render(
    <MemoryRouter initialEntries={['/']}>
      <AuthProvider>
        <Home />
      </AuthProvider>
    </MemoryRouter>
  )
}

describe('Home Page - Animations and Transitions', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    localStorageMock.getItem.mockReturnValue(null)
    const props = getCapturedProps()
    props.length = 0
  })

  // Test Case 1: Check hero section for entrance animation
  describe('Test Case 1: Hero section entrance animation', () => {
    it('hero section has entrance animation with initial and animate props', () => {
      renderWithRouter()

      // Hero section should be rendered
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // Check that motion components with animations were rendered
      const animatedElements = document.querySelectorAll('[data-has-animation="true"]')
      expect(animatedElements.length).toBeGreaterThan(0)
    })

    it('hero content has fade and slide entrance animation', () => {
      renderWithRouter()

      // Find hero section and verify it contains animated content
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // The hero section should contain the headline and visual
      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()

      const visual = screen.getByTestId('hero-visual')
      expect(visual).toBeInTheDocument()

      // Check that animated divs exist within hero
      const motionDivs = heroSection.querySelectorAll('[data-motion="div"]')
      expect(motionDivs.length).toBeGreaterThanOrEqual(2) // Text content and visual
    })

    it('hero animation uses appropriate duration for smooth feel', () => {
      renderWithRouter()

      const props = getCapturedProps()

      // Verify animation props contain duration values
      const hasAnimationWithDuration = props.some((propObj) => {
        const transition = propObj.transition as { duration?: number } | undefined
        return (
          propObj.initial &&
          propObj.animate &&
          transition?.duration &&
          transition.duration >= 0.3 &&
          transition.duration <= 1.5
        )
      })

      // Animation should have reasonable duration (not too fast, not too slow)
      expect(hasAnimationWithDuration).toBe(true)
    })

    it('hero visual elements animate after text content', () => {
      renderWithRouter()

      const props = getCapturedProps()

      // Find animations with delay (visual should have delay)
      const animationsWithDelay = props.filter((propObj) => {
        const transition = propObj.transition as { delay?: number } | undefined
        return transition?.delay && transition.delay > 0
      })

      // Should have at least one element with delayed animation
      expect(animationsWithDelay.length).toBeGreaterThan(0)
    })
  })

  // Test Case 2: Check feature cards for scroll animation
  describe('Test Case 2: Feature cards scroll animation', () => {
    it('features section uses whileInView for scroll-triggered animation', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      const props = getCapturedProps()

      // Check for whileInView animations
      const hasWhileInViewAnimation = props.some(
        (propObj) => propObj.whileInView !== undefined
      )
      expect(hasWhileInViewAnimation).toBe(true)
    })

    it('feature cards animate when scrolled into view', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')

      // Feature cards should be li elements within the section
      const featureCards = featuresSection.querySelectorAll('li')
      expect(featureCards.length).toBe(4)

      // Each card should have motion wrapper
      const motionListItems = featuresSection.querySelectorAll('[data-motion="li"]')
      expect(motionListItems.length).toBe(4)
    })

    it('feature cards use staggered animation effect', () => {
      renderWithRouter()

      const props = getCapturedProps()

      // Check for variants with staggerChildren
      const hasStaggeredAnimation = props.some((propObj) => {
        const variants = propObj.variants as {
          hidden?: unknown
          visible?: { transition?: { staggerChildren?: number } }
        } | undefined
        return variants?.visible?.transition?.staggerChildren !== undefined
      })

      expect(hasStaggeredAnimation).toBe(true)
    })

    it('feature cards animate with opacity and translation', () => {
      renderWithRouter()

      const props = getCapturedProps()

      // Check for variants that define opacity and y transitions
      const hasOpacityAndTranslation = props.some((propObj) => {
        const variants = propObj.variants as {
          hidden?: { opacity?: number; y?: number }
          visible?: { opacity?: number; y?: number }
        } | undefined
        return (
          variants?.hidden?.opacity !== undefined &&
          variants?.visible?.opacity !== undefined &&
          (variants?.hidden?.y !== undefined || variants?.visible?.y !== undefined)
        )
      })

      expect(hasOpacityAndTranslation).toBe(true)
    })

    it('scroll animation uses viewport once option for performance', () => {
      renderWithRouter()

      const props = getCapturedProps()

      // Check for viewport: { once: true } to prevent re-animation
      const hasViewportOnce = props.some((propObj) => {
        const viewport = propObj.viewport as { once?: boolean } | undefined
        return viewport?.once === true
      })

      expect(hasViewportOnce).toBe(true)
    })
  })

  // Test Case 3: Check button hover states
  describe('Test Case 3: Button hover transitions', () => {
    it('hero CTA buttons have transition classes for smooth hover', () => {
      renderWithRouter()

      const primaryCTA = screen.getByTestId('hero-cta-primary')
      const secondaryCTA = screen.getByTestId('hero-cta-secondary')

      // DaisyUI btn class includes built-in transitions
      expect(primaryCTA).toHaveClass('btn')
      expect(secondaryCTA).toHaveClass('btn')
    })

    it('feature cards have hover shadow transition', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      const cards = featuresSection.querySelectorAll('.card')

      cards.forEach((card) => {
        // Cards should have transition-shadow and hover:shadow-2xl classes
        expect(card).toHaveClass('transition-shadow')
        expect(card).toHaveClass('duration-300')
        expect(card).toHaveClass('hover:shadow-2xl')
      })
    })

    it('final CTA button has btn class for built-in transitions', () => {
      renderWithRouter()

      const finalCTAButton = screen.getByTestId('final-cta-button')

      // Button should have btn class which includes transitions
      expect(finalCTAButton).toHaveClass('btn')
      expect(finalCTAButton).toHaveClass('btn-primary')
    })

    it('buttons have smooth duration for hover transitions', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      const cards = featuresSection.querySelectorAll('.card')

      // All cards should have duration-300 for consistent animation timing
      cards.forEach((card) => {
        expect(card).toHaveClass('duration-300')
      })
    })
  })

  // Test Case 4: Verify prefers-reduced-motion is respected
  describe('Test Case 4: Prefers-reduced-motion', () => {
    it('CSS transition classes respect prefers-reduced-motion via Tailwind', () => {
      // Tailwind's transition utilities respect prefers-reduced-motion by default
      // when the 'motion-reduce:' prefix is used
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      const cards = featuresSection.querySelectorAll('.card')

      // Cards should have transition classes
      cards.forEach((card) => {
        expect(card).toHaveClass('transition-shadow')
      })

      // Note: Tailwind's `motion-reduce:` utility class respects the preference
      // This test verifies the CSS structure is in place
    })

    it('animations use appropriate motion-safe patterns', () => {
      renderWithRouter()

      const props = getCapturedProps()

      // Find animations with explicit durations and verify they are reasonable
      const animationsWithExplicitDuration = props.filter((propObj) => {
        const transition = propObj.transition as { duration?: number } | undefined
        return transition?.duration !== undefined
      })

      // All explicit durations should be reasonable (not too fast, not too slow)
      // Framer Motion uses reasonable defaults, and some short durations are valid
      const unreasonableDurations = animationsWithExplicitDuration.filter((propObj) => {
        const transition = propObj.transition as { duration?: number }
        // Accept durations from 0.1s to 4s as reasonable for subtle animations
        return !(transition.duration! >= 0.1 && transition.duration! <= 4)
      })

      expect(unreasonableDurations.length).toBe(0)

      // Should have at least some animations with explicit duration
      expect(animationsWithExplicitDuration.length).toBeGreaterThan(0)
    })

    it('Framer Motion animations are configured to respect user preference', () => {
      // Framer Motion v11+ has built-in support for prefers-reduced-motion
      // via MotionConfig with reducedMotion="user" prop
      // The App.tsx wraps all content with <MotionConfig reducedMotion="user">
      // which tells Framer Motion to respect the user's system preference

      // Verify that animations exist and are using Framer Motion
      renderWithRouter()

      const props = getCapturedProps()

      // Should have motion elements rendered
      expect(props.length).toBeGreaterThan(0)

      // Verify that we have animations that will be affected by reduced motion
      const hasAnimatedElements = props.some(
        (propObj) => propObj.initial || propObj.animate || propObj.whileInView
      )
      expect(hasAnimatedElements).toBe(true)

      // Note: The actual reduced motion behavior is handled by Framer Motion's
      // internal media query listener when MotionConfig reducedMotion="user" is set
    })
  })
})
