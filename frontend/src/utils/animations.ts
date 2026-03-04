/**
 * Animation Configurations
 *
 * Shared Framer Motion animation variants and configurations.
 */

import { Variants } from 'framer-motion'

export const fadeIn: Variants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: { duration: 0.5 }
  }
}

export const staggerChildren: Variants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: {
      staggerChildren: 0.1
    }
  }
}

export const slideInFromBottom: Variants = {
  hidden: { opacity: 0, y: 20 },
  visible: {
    opacity: 1,
    y: 0,
    transition: { duration: 0.5 }
  }
}

export const scaleOnHover: Variants = {
  initial: { scale: 1 },
  hover: { scale: 1.05 }
}

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
