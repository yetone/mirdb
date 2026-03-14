/**
 * Custom hook for smooth scroll navigation.
 * Owner: Scenario 11 - User Interaction Patterns
 *
 * Requirements:
 * - Smooth scroll to anchor links
 * - Respect prefers-reduced-motion
 * - Configurable offset for sticky header
 *
 * Expected exports:
 * - useSmoothScroll: (offset?: number) => {
 *     scrollTo: (targetId: string) => void
 *     scrollToTop: () => void
 *     prefersReducedMotion: boolean
 *   }
 */

import { useCallback, useEffect, useState } from 'react'

/**
 * Check if user prefers reduced motion.
 * Returns true if the user has requested minimal animation.
 */
function getPrefersReducedMotion(): boolean {
  if (typeof window === 'undefined') return false
  const mediaQuery = window.matchMedia('(prefers-reduced-motion: reduce)')
  return mediaQuery.matches
}

export interface UseSmoothScrollOptions {
  /** Offset in pixels from top of viewport (for sticky header) */
  offset?: number
  /** Default scroll behavior */
  behavior?: ScrollBehavior
}

export interface UseSmoothScrollReturn {
  /** Scroll to an element by its ID */
  scrollTo: (targetId: string) => void
  /** Scroll to top of page */
  scrollToTop: () => void
  /** Whether user prefers reduced motion */
  prefersReducedMotion: boolean
}

/**
 * Custom hook for smooth scroll navigation with reduced motion support.
 *
 * @param options - Configuration options
 * @returns Object with scrollTo, scrollToTop, and prefersReducedMotion
 *
 * @example
 * ```tsx
 * const { scrollTo, scrollToTop, prefersReducedMotion } = useSmoothScroll({ offset: 80 });
 *
 * // Scroll to features section
 * scrollTo('features');
 *
 * // Scroll to top
 * scrollToTop();
 * ```
 */
export function useSmoothScroll(
  options: UseSmoothScrollOptions = {}
): UseSmoothScrollReturn {
  const { offset = 0, behavior = 'smooth' } = options
  const [prefersReducedMotion, setPrefersReducedMotion] = useState(false)

  // Listen for reduced motion preference changes
  useEffect(() => {
    if (typeof window === 'undefined') return

    const mediaQuery = window.matchMedia('(prefers-reduced-motion: reduce)')
    setPrefersReducedMotion(mediaQuery.matches)

    const handleChange = (event: MediaQueryListEvent) => {
      setPrefersReducedMotion(event.matches)
    }

    // Modern browsers support addEventListener
    if (mediaQuery.addEventListener) {
      mediaQuery.addEventListener('change', handleChange)
      return () => mediaQuery.removeEventListener('change', handleChange)
    } else {
      // Fallback for older browsers
      mediaQuery.addListener(handleChange)
      return () => mediaQuery.removeListener(handleChange)
    }
  }, [])

  /**
   * Get the scroll behavior based on user preference.
   * If user prefers reduced motion, use 'auto' (instant) instead of 'smooth'.
   */
  const getScrollBehavior = useCallback((): ScrollBehavior => {
    return prefersReducedMotion ? 'auto' : behavior
  }, [prefersReducedMotion, behavior])

  /**
   * Scroll to an element by its ID.
   * Accounts for offset (e.g., sticky header height).
   */
  const scrollTo = useCallback(
    (targetId: string) => {
      if (typeof window === 'undefined') return

      // Remove leading # if present
      const cleanId = targetId.replace(/^#/, '')
      const targetElement = document.getElementById(cleanId)

      if (!targetElement) {
        console.warn(`[useSmoothScroll] Element with id "${cleanId}" not found`)
        return
      }

      const targetPosition = targetElement.getBoundingClientRect().top
      const offsetPosition = targetPosition + window.pageYOffset - offset

      window.scrollTo({
        top: offsetPosition,
        behavior: getScrollBehavior(),
      })
    },
    [offset, getScrollBehavior]
  )

  /**
   * Scroll to the top of the page.
   */
  const scrollToTop = useCallback(() => {
    if (typeof window === 'undefined') return

    window.scrollTo({
      top: 0,
      behavior: getScrollBehavior(),
    })
  }, [getScrollBehavior])

  return {
    scrollTo,
    scrollToTop,
    prefersReducedMotion,
  }
}

/**
 * Utility hook to check if user prefers reduced motion.
 * Can be used independently of scrolling functionality.
 */
export function usePrefersReducedMotion(): boolean {
  const [prefersReducedMotion, setPrefersReducedMotion] = useState(() =>
    getPrefersReducedMotion()
  )

  useEffect(() => {
    if (typeof window === 'undefined') return

    const mediaQuery = window.matchMedia('(prefers-reduced-motion: reduce)')

    const handleChange = (event: MediaQueryListEvent) => {
      setPrefersReducedMotion(event.matches)
    }

    if (mediaQuery.addEventListener) {
      mediaQuery.addEventListener('change', handleChange)
      return () => mediaQuery.removeEventListener('change', handleChange)
    } else {
      mediaQuery.addListener(handleChange)
      return () => mediaQuery.removeListener(handleChange)
    }
  }, [])

  return prefersReducedMotion
}

export default useSmoothScroll
