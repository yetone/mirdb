/**
 * Smooth Scroll Hook.
 * Owner: Scenario 14 - Smooth Scroll Navigation
 *
 * Custom hook for smooth scrolling:
 * - scrollTo(elementId): Scrolls to element by ID
 * - scrollToTop(): Scrolls to top of page
 *
 * This hook provides a consistent API for smooth scrolling behavior
 * that works with the CSS scroll-behavior: smooth property defined
 * in globals.css.
 */

import { useCallback } from 'react'

interface UseSmoothScrollReturn {
  scrollTo: (id: string) => void
  scrollToTop: () => void
}

/**
 * Custom hook for smooth scrolling navigation
 * @returns Object with scrollTo and scrollToTop functions
 */
export function useSmoothScroll(): UseSmoothScrollReturn {
  /**
   * Scrolls to an element by its ID
   * @param id - The ID of the element to scroll to (with or without #)
   */
  const scrollTo = useCallback((id: string) => {
    // Remove leading # if present
    const elementId = id.startsWith('#') ? id.slice(1) : id
    const element = document.getElementById(elementId)

    if (element) {
      element.scrollIntoView({ behavior: 'smooth', block: 'start' })
    }
  }, [])

  /**
   * Scrolls to the top of the page
   */
  const scrollToTop = useCallback(() => {
    window.scrollTo({ top: 0, behavior: 'smooth' })
  }, [])

  return { scrollTo, scrollToTop }
}
