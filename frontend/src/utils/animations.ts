/**
 * Animation Configurations
 * Owner: Scenario 8 - Animations and Micro-interactions
 *
 * Shared Framer Motion animation variants and configurations.
 *
 * Expected exports:
 * - fadeIn: Variants (opacity 0 -> 1, duration 0.5-1s)
 * - staggerChildren: Variants (staggerChildren: 0.1)
 * - scaleOnHover: Variants (scale on hover)
 * - slideInFromBottom: Variants
 * - heroFadeIn: Hero-specific fade animation (0.8s duration)
 *
 * Requirements:
 * - Respect prefers-reduced-motion
 * - Consistent timing across components
 */

import { Variants } from 'framer-motion'

/**
 * Check if user prefers reduced motion
 * Returns true if the user has requested reduced motion in their system settings
 */
export const prefersReducedMotion = (): boolean => {
  if (typeof window === 'undefined') return false
  return window.matchMedia('(prefers-reduced-motion: reduce)').matches
}

/**
 * Reduced motion duration - use 0 for instant transitions when reduced motion is preferred
 */
export const REDUCED_MOTION_DURATION = 0.01

/**
 * Get animation duration respecting reduced motion preference
 */
export const getMotionSafeDuration = (normalDuration: number): number => {
  return prefersReducedMotion() ? REDUCED_MOTION_DURATION : normalDuration
}

/**
 * Hero fade-in animation - opacity 0 to 1 with 0.8s duration (within 0.5-1s spec)
 * Used for hero section entrance animation on page load
 */
export const fadeIn: Variants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: { duration: 0.8 }
  }
}

/**
 * Hero-specific fade-in variants matching HeroSection implementation
 * Duration: 0.8s (within 0.5-1s requirement per PRD)
 */
export const heroFadeIn: Variants = {
  hidden: { opacity: 0, y: 20 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.8,
      ease: 'easeOut',
    },
  },
}

/**
 * Stagger children container variant
 * Used to animate child elements with 100ms (0.1s) delay between each
 */
export const staggerChildren: Variants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.1
    }
  }
}

/**
 * Hero stagger container - staggers children with 200ms delay
 */
export const heroStaggerContainer: Variants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.2,
      delayChildren: 0.1,
    },
  },
}

export const slideInFromBottom: Variants = {
  hidden: { opacity: 0, y: 20 },
  visible: {
    opacity: 1,
    y: 0,
    transition: { duration: 0.5 }
  }
}

/**
 * Scale on hover variant - provides subtle scale feedback
 * Used for buttons and interactive elements
 */
export const scaleOnHover: Variants = {
  initial: { scale: 1 },
  hover: { scale: 1.05 }
}

/**
 * Button hover animation props
 * Provides whileHover and whileTap configurations for buttons
 */
export const buttonHoverProps = {
  whileHover: { scale: 1.05 },
  whileTap: { scale: 0.95 },
}

/**
 * Feature card stagger animation variants
 * Each card has a 100ms (0.1s) delay multiplied by its index
 */
export const featureCardVariants: Variants = {
  hidden: { opacity: 0, y: 30 },
  visible: (index: number) => ({
    opacity: 1,
    y: 0,
    transition: {
      delay: index * 0.1,
      duration: 0.5,
      ease: 'easeOut'
    }
  })
}

/**
 * Scroll-triggered animation props
 * Used with whileInView for viewport-triggered animations
 */
export const scrollTriggeredProps = {
  initial: { opacity: 0, y: 20 },
  whileInView: { opacity: 1, y: 0 },
  viewport: { once: true, margin: '-100px' },
  transition: { duration: 0.5 },
}

/**
 * Section scroll animation variants
 * Used for sections that animate when entering viewport
 */
export const sectionScrollVariants: Variants = {
  hidden: { opacity: 0, y: 30 },
  visible: {
    opacity: 1,
    y: 0,
    transition: {
      duration: 0.6,
      ease: 'easeOut',
    },
  },
}

/**
 * Theme transition variants for smooth theme changes
 * Used for homepage and other theme-aware components
 */
export const themeTransition: Variants = {
  initial: { opacity: 0.9 },
  animate: {
    opacity: 1,
    transition: {
      duration: 0.3,
      ease: 'easeOut'
    }
  },
  exit: { opacity: 0.9 }
}

/**
 * Background color transition for theme changes
 * Applied to main containers that need smooth color transitions
 */
export const backgroundTransition = {
  transition: {
    backgroundColor: {
      duration: 0.3,
      ease: 'easeInOut'
    }
  }
}

/**
 * Reduced motion safe wrapper
 * Returns variants that respect prefers-reduced-motion
 */
export const getReducedMotionVariants = (variants: Variants): Variants => {
  if (typeof window !== 'undefined' && prefersReducedMotion()) {
    const reducedVariants: Variants = {}
    for (const key in variants) {
      const variant = variants[key]
      if (typeof variant === 'function') {
        reducedVariants[key] = (custom: number) => {
          const result = variant(custom)
          return {
            ...result,
            transition: { duration: REDUCED_MOTION_DURATION }
          }
        }
      } else if (typeof variant === 'object' && variant !== null) {
        reducedVariants[key] = {
          ...variant,
          transition: { duration: REDUCED_MOTION_DURATION }
        }
      } else {
        reducedVariants[key] = variant
      }
    }
    return reducedVariants
  }
  return variants
}
