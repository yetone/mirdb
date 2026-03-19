/**
 * useSmoothScroll hook for navigation.
 * Owner: Scenario 8 - Navigation Header
 *
 * Returns:
 * - scrollTo: (elementId: string) => void
 *
 * Features:
 * - Smooth scroll behavior
 * - Header offset accounting
 *
 * Requirements: REQ-13
 */

import { useCallback } from 'react';

const HEADER_HEIGHT = 80; // Height of fixed header in pixels

/**
 * Hook that provides smooth scrolling functionality with header offset.
 */
export function useSmoothScroll() {
  const scrollTo = useCallback((elementId: string) => {
    // Remove the # prefix if present
    const id = elementId.startsWith('#') ? elementId.slice(1) : elementId;
    const element = document.getElementById(id);

    if (element) {
      const elementPosition = element.getBoundingClientRect().top;
      const offsetPosition = elementPosition + window.scrollY - HEADER_HEIGHT;

      window.scrollTo({
        top: offsetPosition,
        behavior: 'smooth',
      });
    }
  }, []);

  return { scrollTo };
}
