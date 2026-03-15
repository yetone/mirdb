/**
 * Smooth Scroll hook for anchor navigation.
 * Owner: Scenario 5 - Navigation Header and Footer
 *
 * Returns:
 * - scrollTo: (elementId: string) => void
 *
 * Features:
 * - Smooth scroll behavior
 * - Accounts for sticky header offset
 * - Updates URL hash without page jump
 */

import { useCallback } from 'react'

const HEADER_OFFSET = 80 // Height of sticky header plus some padding

export interface UseSmoothScrollReturn {
  scrollTo: (elementId: string) => void
}

export function useSmoothScroll(): UseSmoothScrollReturn {
  const scrollTo = useCallback((elementId: string) => {
    const element = document.getElementById(elementId)

    if (element) {
      const elementPosition = element.getBoundingClientRect().top
      const offsetPosition = elementPosition + window.scrollY - HEADER_OFFSET

      window.scrollTo({
        top: offsetPosition,
        behavior: 'smooth',
      })

      // Update URL hash without causing a jump
      if (history.pushState) {
        history.pushState(null, '', `#${elementId}`)
      } else {
        // Fallback for older browsers
        window.location.hash = elementId
      }
    }
  }, [])

  return { scrollTo }
}

export default useSmoothScroll
