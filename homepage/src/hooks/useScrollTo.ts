/**
 * Custom hook for smooth scroll navigation.
 * Owner: Scenario 14 - Smooth Scroll Navigation
 *
 * Features:
 * - Smooth scroll to element by ID
 * - Account for fixed header offset
 * - Support for scroll behavior configuration
 */

import { useCallback } from 'react'

interface UseScrollToOptions {
  /** Offset to account for fixed headers (default: 64px for the site header) */
  headerOffset?: number
  /** Scroll behavior: 'smooth' or 'auto' (default: 'smooth') */
  behavior?: ScrollBehavior
}

interface UseScrollToReturn {
  scrollTo: (elementId: string) => void
}

const DEFAULT_HEADER_OFFSET = 64

export function useScrollTo(options: UseScrollToOptions = {}): UseScrollToReturn {
  const {
    headerOffset = DEFAULT_HEADER_OFFSET,
    behavior = 'smooth'
  } = options

  const scrollTo = useCallback((elementId: string) => {
    // Remove leading hash if present
    const cleanId = elementId.startsWith('#') ? elementId.slice(1) : elementId

    const element = document.getElementById(cleanId)

    if (!element) {
      console.warn(`useScrollTo: Element with id "${cleanId}" not found`)
      return
    }

    const elementPosition = element.getBoundingClientRect().top
    const offsetPosition = elementPosition + window.scrollY - headerOffset

    window.scrollTo({
      top: offsetPosition,
      behavior,
    })
  }, [headerOffset, behavior])

  return { scrollTo }
}

/**
 * Standalone scroll function for use outside React components.
 * Useful for event handlers that don't need React hook lifecycle.
 */
export function scrollToElement(
  elementId: string,
  options: UseScrollToOptions = {}
): void {
  const {
    headerOffset = DEFAULT_HEADER_OFFSET,
    behavior = 'smooth'
  } = options

  const cleanId = elementId.startsWith('#') ? elementId.slice(1) : elementId
  const element = document.getElementById(cleanId)

  if (!element) {
    console.warn(`scrollToElement: Element with id "${cleanId}" not found`)
    return
  }

  const elementPosition = element.getBoundingClientRect().top
  const offsetPosition = elementPosition + window.scrollY - headerOffset

  window.scrollTo({
    top: offsetPosition,
    behavior,
  })
}
