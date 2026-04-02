/**
 * Hook for smooth scroll navigation.
 * Owner: Scenario 15 - Smooth Scroll Navigation
 *
 * Provides smooth scroll functionality with URL hash updates.
 */

import { useCallback } from 'react';

export interface UseSmoothScrollOptions {
  offset?: number;
  behavior?: ScrollBehavior;
}

export interface UseSmoothScrollReturn {
  scrollTo: (elementId: string) => void;
}

/**
 * Scrolls smoothly to an element by its ID and updates the URL hash.
 *
 * @param elementId - The ID of the target element (without the # prefix)
 * @param options - Optional scroll configuration
 */
export function scrollToSection(
  elementId: string,
  options: UseSmoothScrollOptions = {}
): void {
  const { offset = 0, behavior = 'smooth' } = options;

  if (typeof window === 'undefined') {
    return;
  }

  const element = document.getElementById(elementId);

  if (element) {
    const elementPosition = element.getBoundingClientRect().top;
    const offsetPosition = elementPosition + window.scrollY - offset;

    window.scrollTo({
      top: offsetPosition,
      behavior,
    });

    // Update URL hash without triggering scroll
    if (typeof window.history !== 'undefined' && window.history.pushState) {
      window.history.pushState(null, '', `#${elementId}`);
    } else {
      // Fallback for older browsers
      window.location.hash = elementId;
    }
  }
}

/**
 * Hook for smooth scroll navigation with URL hash updates.
 *
 * @param options - Optional configuration for scroll behavior
 * @returns Object containing scrollTo function
 *
 * @example
 * ```tsx
 * const { scrollTo } = useSmoothScroll();
 *
 * // Scroll to features section
 * scrollTo('features');
 * ```
 */
export function useSmoothScroll(
  options: UseSmoothScrollOptions = {}
): UseSmoothScrollReturn {
  const { offset = 0, behavior = 'smooth' } = options;

  const scrollTo = useCallback(
    (elementId: string) => {
      scrollToSection(elementId, { offset, behavior });
    },
    [offset, behavior]
  );

  return { scrollTo };
}
