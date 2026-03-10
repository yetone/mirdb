/**
 * Reduced Motion Hook
 * Owner: Scenario 12 - Visual Effects and Animations
 *
 * Custom hook to detect user's prefers-reduced-motion preference.
 * Respects accessibility settings and allows components to disable
 * or minimize animations for users who prefer reduced motion.
 *
 * @returns boolean - true if user prefers reduced motion, false otherwise
 *
 * Usage:
 * const prefersReducedMotion = useReducedMotion();
 * // Disable animations when true
 */

import { useState, useEffect } from 'react';

/**
 * Hook to detect if the user prefers reduced motion.
 * Listens for changes to the prefers-reduced-motion media query.
 */
export function useReducedMotion(): boolean {
  const [prefersReducedMotion, setPrefersReducedMotion] = useState<boolean>(() => {
    // Check if we're in a browser environment
    if (typeof window === 'undefined') {
      return false;
    }

    // Check the initial preference
    const mediaQuery = window.matchMedia('(prefers-reduced-motion: reduce)');
    return mediaQuery.matches;
  });

  useEffect(() => {
    // Early return if not in browser
    if (typeof window === 'undefined') {
      return;
    }

    const mediaQuery = window.matchMedia('(prefers-reduced-motion: reduce)');

    // Handler for preference changes
    const handleChange = (event: MediaQueryListEvent) => {
      setPrefersReducedMotion(event.matches);
    };

    // Add listener for changes
    mediaQuery.addEventListener('change', handleChange);

    // Cleanup listener on unmount
    return () => {
      mediaQuery.removeEventListener('change', handleChange);
    };
  }, []);

  return prefersReducedMotion;
}

export default useReducedMotion;
