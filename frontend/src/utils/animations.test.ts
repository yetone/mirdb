/**
 * Animation Tests - Scenario 8: Animations and Micro-interactions
 *
 * Tests Framer Motion animations for:
 * - TC1: Hero fade-in animation presence (motion.div with initial/animate)
 * - TC2: Hero animation configuration (opacity 0->1, duration 0.5-1s)
 * - TC3: Feature cards stagger animation (staggerChildren: 0.1)
 * - TC4: CTA button hover effects (whileHover scale)
 * - TC5: Scroll-triggered animations (whileInView prop)
 * - TC6: Reduced motion handling (useReducedMotion/CSS media query)
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  fadeIn,
  heroFadeIn,
  staggerChildren,
  featureCardVariants,
  scaleOnHover,
  buttonHoverProps,
  scrollTriggeredProps,
  sectionScrollVariants,
  heroStaggerContainer,
  prefersReducedMotion,
  getReducedMotionVariants,
  REDUCED_MOTION_DURATION,
  getMotionSafeDuration,
} from './animations'

describe('Scenario 8: Animations and Micro-interactions', () => {
  // Save original matchMedia
  const originalMatchMedia = window.matchMedia

  beforeEach(() => {
    // Reset matchMedia mock
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      value: vi.fn().mockImplementation((query: string) => ({
        matches: false,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      })),
    })
  })

  afterEach(() => {
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      value: originalMatchMedia,
    })
  })

  describe('Test Case 1: Hero Section Framer Motion Presence', () => {
    it('should have fadeIn variants with hidden and visible states', () => {
      expect(fadeIn).toBeDefined()
      expect(fadeIn.hidden).toBeDefined()
      expect(fadeIn.visible).toBeDefined()
    })

    it('should have heroFadeIn variants with initial and animate props pattern', () => {
      expect(heroFadeIn).toBeDefined()
      expect(heroFadeIn.hidden).toBeDefined()
      expect(heroFadeIn.visible).toBeDefined()
    })

    it('should have heroStaggerContainer for staggered child animations', () => {
      expect(heroStaggerContainer).toBeDefined()
      expect(heroStaggerContainer.hidden).toBeDefined()
      expect(heroStaggerContainer.visible).toBeDefined()
    })

    it('fadeIn hidden state should have opacity 0', () => {
      expect(fadeIn.hidden).toMatchObject({ opacity: 0 })
    })

    it('heroFadeIn hidden state should have opacity 0 and y offset', () => {
      expect(heroFadeIn.hidden).toMatchObject({ opacity: 0, y: 20 })
    })
  })

  describe('Test Case 2: Hero Animation Configuration (0.5-1s Duration)', () => {
    it('fadeIn should have opacity animation from 0 to 1', () => {
      expect(fadeIn.hidden).toMatchObject({ opacity: 0 })
      expect(fadeIn.visible).toMatchObject({ opacity: 1 })
    })

    it('fadeIn duration should be between 0.5s and 1s', () => {
      const visible = fadeIn.visible as { transition: { duration: number } }
      expect(visible.transition.duration).toBeGreaterThanOrEqual(0.5)
      expect(visible.transition.duration).toBeLessThanOrEqual(1)
    })

    it('heroFadeIn should have opacity animation from 0 to 1', () => {
      expect(heroFadeIn.hidden).toMatchObject({ opacity: 0 })
      expect(heroFadeIn.visible).toMatchObject({ opacity: 1 })
    })

    it('heroFadeIn duration should be 0.8s (within 0.5-1s spec)', () => {
      const visible = heroFadeIn.visible as { transition: { duration: number } }
      expect(visible.transition.duration).toBe(0.8)
      expect(visible.transition.duration).toBeGreaterThanOrEqual(0.5)
      expect(visible.transition.duration).toBeLessThanOrEqual(1)
    })

    it('heroFadeIn should use easeOut timing function', () => {
      const visible = heroFadeIn.visible as { transition: { ease: string } }
      expect(visible.transition.ease).toBe('easeOut')
    })
  })

  describe('Test Case 3: Feature Cards Stagger Animation (100ms)', () => {
    it('staggerChildren should have staggerChildren: 0.1 (100ms)', () => {
      const visible = staggerChildren.visible as { transition: { staggerChildren: number } }
      expect(visible.transition.staggerChildren).toBe(0.1)
    })

    it('featureCardVariants should be defined', () => {
      expect(featureCardVariants).toBeDefined()
      expect(featureCardVariants.hidden).toBeDefined()
      expect(featureCardVariants.visible).toBeDefined()
    })

    it('featureCardVariants.hidden should have opacity 0 and y offset', () => {
      expect(featureCardVariants.hidden).toMatchObject({ opacity: 0, y: 30 })
    })

    it('featureCardVariants.visible should be a function for custom index', () => {
      expect(typeof featureCardVariants.visible).toBe('function')
    })

    it('featureCardVariants should calculate 100ms delay per card index', () => {
      const visibleFn = featureCardVariants.visible as (index: number) => object

      const card0 = visibleFn(0) as { transition: { delay: number } }
      const card1 = visibleFn(1) as { transition: { delay: number } }
      const card2 = visibleFn(2) as { transition: { delay: number } }
      const card3 = visibleFn(3) as { transition: { delay: number } }

      // Use toBeCloseTo for floating point comparisons
      expect(card0.transition.delay).toBeCloseTo(0, 5)
      expect(card1.transition.delay).toBeCloseTo(0.1, 5)
      expect(card2.transition.delay).toBeCloseTo(0.2, 5)
      expect(card3.transition.delay).toBeCloseTo(0.3, 5)
    })

    it('featureCardVariants.visible should return opacity 1 and y 0', () => {
      const visibleFn = featureCardVariants.visible as (index: number) => object
      const result = visibleFn(0) as { opacity: number; y: number }

      expect(result.opacity).toBe(1)
      expect(result.y).toBe(0)
    })
  })

  describe('Test Case 4: CTA Button Hover Effects', () => {
    it('scaleOnHover should have initial and hover states', () => {
      expect(scaleOnHover).toBeDefined()
      expect(scaleOnHover.initial).toBeDefined()
      expect(scaleOnHover.hover).toBeDefined()
    })

    it('scaleOnHover should scale to 1.05 on hover', () => {
      expect(scaleOnHover.initial).toMatchObject({ scale: 1 })
      expect(scaleOnHover.hover).toMatchObject({ scale: 1.05 })
    })

    it('buttonHoverProps should provide whileHover and whileTap configs', () => {
      expect(buttonHoverProps).toBeDefined()
      expect(buttonHoverProps.whileHover).toBeDefined()
      expect(buttonHoverProps.whileTap).toBeDefined()
    })

    it('buttonHoverProps.whileHover should scale to 1.05', () => {
      expect(buttonHoverProps.whileHover).toMatchObject({ scale: 1.05 })
    })

    it('buttonHoverProps.whileTap should scale to 0.95 for press feedback', () => {
      expect(buttonHoverProps.whileTap).toMatchObject({ scale: 0.95 })
    })
  })

  describe('Test Case 5: Scroll-Triggered Animations (whileInView)', () => {
    it('scrollTriggeredProps should have whileInView configuration', () => {
      expect(scrollTriggeredProps).toBeDefined()
      expect(scrollTriggeredProps.whileInView).toBeDefined()
    })

    it('scrollTriggeredProps should have initial state with opacity 0', () => {
      expect(scrollTriggeredProps.initial).toMatchObject({ opacity: 0 })
    })

    it('scrollTriggeredProps.whileInView should animate to opacity 1', () => {
      expect(scrollTriggeredProps.whileInView).toMatchObject({ opacity: 1 })
    })

    it('scrollTriggeredProps should have viewport config with once: true', () => {
      expect(scrollTriggeredProps.viewport).toBeDefined()
      expect(scrollTriggeredProps.viewport.once).toBe(true)
    })

    it('scrollTriggeredProps should have viewport margin for early trigger', () => {
      expect(scrollTriggeredProps.viewport.margin).toBe('-100px')
    })

    it('sectionScrollVariants should have hidden and visible states', () => {
      expect(sectionScrollVariants).toBeDefined()
      expect(sectionScrollVariants.hidden).toBeDefined()
      expect(sectionScrollVariants.visible).toBeDefined()
    })

    it('sectionScrollVariants.hidden should have opacity 0 and y offset', () => {
      expect(sectionScrollVariants.hidden).toMatchObject({ opacity: 0, y: 30 })
    })

    it('sectionScrollVariants.visible should animate to opacity 1 and y 0', () => {
      expect(sectionScrollVariants.visible).toMatchObject({ opacity: 1, y: 0 })
    })
  })

  describe('Test Case 6: Reduced Motion Handling', () => {
    it('prefersReducedMotion function should be exported', () => {
      expect(prefersReducedMotion).toBeDefined()
      expect(typeof prefersReducedMotion).toBe('function')
    })

    it('prefersReducedMotion should return false when motion is not reduced', () => {
      Object.defineProperty(window, 'matchMedia', {
        writable: true,
        value: vi.fn().mockImplementation(() => ({
          matches: false,
          media: '',
          addListener: vi.fn(),
          removeListener: vi.fn(),
          addEventListener: vi.fn(),
          removeEventListener: vi.fn(),
          dispatchEvent: vi.fn(),
        })),
      })

      expect(prefersReducedMotion()).toBe(false)
    })

    it('prefersReducedMotion should return true when prefers-reduced-motion: reduce', () => {
      Object.defineProperty(window, 'matchMedia', {
        writable: true,
        value: vi.fn().mockImplementation((query: string) => ({
          matches: query === '(prefers-reduced-motion: reduce)',
          media: query,
          addListener: vi.fn(),
          removeListener: vi.fn(),
          addEventListener: vi.fn(),
          removeEventListener: vi.fn(),
          dispatchEvent: vi.fn(),
        })),
      })

      expect(prefersReducedMotion()).toBe(true)
    })

    it('REDUCED_MOTION_DURATION should be a very small value', () => {
      expect(REDUCED_MOTION_DURATION).toBeDefined()
      expect(REDUCED_MOTION_DURATION).toBeLessThan(0.1)
    })

    it('getMotionSafeDuration should return normal duration when motion is not reduced', () => {
      Object.defineProperty(window, 'matchMedia', {
        writable: true,
        value: vi.fn().mockImplementation(() => ({
          matches: false,
          media: '',
          addListener: vi.fn(),
          removeListener: vi.fn(),
          addEventListener: vi.fn(),
          removeEventListener: vi.fn(),
          dispatchEvent: vi.fn(),
        })),
      })

      expect(getMotionSafeDuration(0.5)).toBe(0.5)
      expect(getMotionSafeDuration(1)).toBe(1)
    })

    it('getMotionSafeDuration should return reduced duration when motion is reduced', () => {
      Object.defineProperty(window, 'matchMedia', {
        writable: true,
        value: vi.fn().mockImplementation((query: string) => ({
          matches: query === '(prefers-reduced-motion: reduce)',
          media: query,
          addListener: vi.fn(),
          removeListener: vi.fn(),
          addEventListener: vi.fn(),
          removeEventListener: vi.fn(),
          dispatchEvent: vi.fn(),
        })),
      })

      expect(getMotionSafeDuration(0.5)).toBe(REDUCED_MOTION_DURATION)
      expect(getMotionSafeDuration(1)).toBe(REDUCED_MOTION_DURATION)
    })

    it('getReducedMotionVariants should be exported', () => {
      expect(getReducedMotionVariants).toBeDefined()
      expect(typeof getReducedMotionVariants).toBe('function')
    })

    it('getReducedMotionVariants should return original variants when motion is not reduced', () => {
      Object.defineProperty(window, 'matchMedia', {
        writable: true,
        value: vi.fn().mockImplementation(() => ({
          matches: false,
          media: '',
          addListener: vi.fn(),
          removeListener: vi.fn(),
          addEventListener: vi.fn(),
          removeEventListener: vi.fn(),
          dispatchEvent: vi.fn(),
        })),
      })

      const result = getReducedMotionVariants(fadeIn)
      expect(result).toBe(fadeIn)
    })

    it('getReducedMotionVariants should reduce duration when motion is reduced', () => {
      Object.defineProperty(window, 'matchMedia', {
        writable: true,
        value: vi.fn().mockImplementation((query: string) => ({
          matches: query === '(prefers-reduced-motion: reduce)',
          media: query,
          addListener: vi.fn(),
          removeListener: vi.fn(),
          addEventListener: vi.fn(),
          removeEventListener: vi.fn(),
          dispatchEvent: vi.fn(),
        })),
      })

      const result = getReducedMotionVariants(fadeIn)
      const visible = result.visible as { transition: { duration: number } }
      expect(visible.transition.duration).toBe(REDUCED_MOTION_DURATION)
    })
  })

  describe('Animation Configuration Consistency', () => {
    it('all duration values should be reasonable (< 2s)', () => {
      const fadeVisible = fadeIn.visible as { transition: { duration: number } }
      const heroVisible = heroFadeIn.visible as { transition: { duration: number } }
      const sectionVisible = sectionScrollVariants.visible as { transition: { duration: number } }

      expect(fadeVisible.transition.duration).toBeLessThan(2)
      expect(heroVisible.transition.duration).toBeLessThan(2)
      expect(sectionVisible.transition.duration).toBeLessThan(2)
    })

    it('stagger delays should be consistent at 100ms (0.1s)', () => {
      const staggerVisible = staggerChildren.visible as { transition: { staggerChildren: number } }
      expect(staggerVisible.transition.staggerChildren).toBe(0.1)

      const visibleFn = featureCardVariants.visible as (index: number) => object
      const card1 = visibleFn(1) as { transition: { delay: number } }
      expect(card1.transition.delay).toBe(0.1)
    })
  })
})
