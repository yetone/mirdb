/**
 * Custom hook for responsive media queries.
 * Owner: Scenario 7 - Responsive Design Implementation
 *
 * Requirements:
 * - Breakpoints at 320px, 768px, 1024px, 1440px (NFR-3)
 * - SSR-safe implementation
 * - Debounced resize handling
 */

import { useState, useEffect, useCallback } from 'react'
import { BREAKPOINTS } from '../utils/constants'

type BreakpointKey = 'mobile' | 'tablet' | 'desktop' | 'wide'

/**
 * Check if window is defined (for SSR safety)
 */
const isClient = typeof window !== 'undefined'

/**
 * Hook to check if a media query matches
 * @param query - CSS media query string
 * @returns boolean indicating if the query matches
 */
export function useMediaQuery(query: string): boolean {
  const [matches, setMatches] = useState<boolean>(() => {
    if (!isClient) return false
    return window.matchMedia(query).matches
  })

  useEffect(() => {
    if (!isClient) return

    const mediaQueryList = window.matchMedia(query)

    // Update the state when the media query changes
    const handleChange = (event: MediaQueryListEvent) => {
      setMatches(event.matches)
    }

    // Set initial value
    setMatches(mediaQueryList.matches)

    // Add listener for changes
    mediaQueryList.addEventListener('change', handleChange)

    return () => {
      mediaQueryList.removeEventListener('change', handleChange)
    }
  }, [query])

  return matches
}

/**
 * Hook to get the current breakpoint name
 * @returns Current breakpoint: 'mobile' | 'tablet' | 'desktop' | 'wide'
 */
export function useBreakpoint(): BreakpointKey {
  const [breakpoint, setBreakpoint] = useState<BreakpointKey>(() => {
    if (!isClient) return 'mobile'
    return getBreakpointFromWidth(window.innerWidth)
  })

  const handleResize = useCallback(() => {
    const newBreakpoint = getBreakpointFromWidth(window.innerWidth)
    setBreakpoint(newBreakpoint)
  }, [])

  useEffect(() => {
    if (!isClient) return

    // Debounced resize handler
    let timeoutId: ReturnType<typeof setTimeout>
    const debouncedResize = () => {
      clearTimeout(timeoutId)
      timeoutId = setTimeout(handleResize, 100)
    }

    // Set initial value
    handleResize()

    window.addEventListener('resize', debouncedResize)

    return () => {
      window.removeEventListener('resize', debouncedResize)
      clearTimeout(timeoutId)
    }
  }, [handleResize])

  return breakpoint
}

/**
 * Helper function to determine breakpoint from viewport width
 */
function getBreakpointFromWidth(width: number): BreakpointKey {
  if (width >= BREAKPOINTS.wide) return 'wide'
  if (width >= BREAKPOINTS.desktop) return 'desktop'
  if (width >= BREAKPOINTS.tablet) return 'tablet'
  return 'mobile'
}

/**
 * Hook to check if the current viewport is mobile-sized
 * @returns boolean indicating if viewport is below tablet breakpoint
 */
export function useIsMobile(): boolean {
  return useMediaQuery(`(max-width: ${BREAKPOINTS.tablet - 1}px)`)
}

/**
 * Hook to check if the current viewport is tablet-sized
 * @returns boolean indicating if viewport is between tablet and desktop breakpoints
 */
export function useIsTablet(): boolean {
  return useMediaQuery(
    `(min-width: ${BREAKPOINTS.tablet}px) and (max-width: ${BREAKPOINTS.desktop - 1}px)`
  )
}

/**
 * Hook to check if the current viewport is desktop-sized or larger
 * @returns boolean indicating if viewport is at or above desktop breakpoint
 */
export function useIsDesktop(): boolean {
  return useMediaQuery(`(min-width: ${BREAKPOINTS.desktop}px)`)
}

/**
 * Hook to get viewport dimensions
 * @returns Object with width and height of the viewport
 */
export function useViewportSize(): { width: number; height: number } {
  const [size, setSize] = useState<{ width: number; height: number }>(() => {
    if (!isClient) return { width: 0, height: 0 }
    return { width: window.innerWidth, height: window.innerHeight }
  })

  useEffect(() => {
    if (!isClient) return

    let timeoutId: ReturnType<typeof setTimeout>

    const handleResize = () => {
      clearTimeout(timeoutId)
      timeoutId = setTimeout(() => {
        setSize({ width: window.innerWidth, height: window.innerHeight })
      }, 100)
    }

    window.addEventListener('resize', handleResize)

    return () => {
      window.removeEventListener('resize', handleResize)
      clearTimeout(timeoutId)
    }
  }, [])

  return size
}
