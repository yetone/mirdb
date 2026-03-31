/**
 * Media Query Hook
 * Owner: Scenario 8 - Responsive Design
 *
 * Provides responsive breakpoint detection.
 *
 * Expected exports:
 * - useMediaQuery(query: string): boolean
 * - useBreakpoint(): 'mobile' | 'tablet' | 'desktop'
 */

import { useState, useEffect, useCallback } from 'react';

export type Breakpoint = 'mobile' | 'tablet' | 'desktop';

// Tailwind CSS default breakpoints
export const BREAKPOINTS = {
  sm: 640,
  md: 768,
  lg: 1024,
  xl: 1280,
  '2xl': 1536,
} as const;

/**
 * Hook to detect if a media query matches the current viewport.
 * @param query - CSS media query string (e.g., '(min-width: 768px)')
 * @returns boolean indicating if the query matches
 */
export function useMediaQuery(query: string): boolean {
  const getMatches = useCallback((mediaQuery: string): boolean => {
    // Prevent SSR issues
    if (typeof window === 'undefined') {
      return false;
    }
    return window.matchMedia(mediaQuery).matches;
  }, []);

  const [matches, setMatches] = useState<boolean>(() => getMatches(query));

  useEffect(() => {
    if (typeof window === 'undefined') {
      return;
    }

    const mediaQueryList = window.matchMedia(query);

    // Set initial value
    setMatches(mediaQueryList.matches);

    // Create event listener
    const handleChange = (event: MediaQueryListEvent) => {
      setMatches(event.matches);
    };

    // Add event listener (with fallback for older browsers)
    if (mediaQueryList.addEventListener) {
      mediaQueryList.addEventListener('change', handleChange);
    } else {
      // Deprecated but needed for older browsers
      mediaQueryList.addListener(handleChange);
    }

    return () => {
      if (mediaQueryList.removeEventListener) {
        mediaQueryList.removeEventListener('change', handleChange);
      } else {
        mediaQueryList.removeListener(handleChange);
      }
    };
  }, [query, getMatches]);

  return matches;
}

/**
 * Hook to get the current breakpoint name based on viewport width.
 * @returns 'mobile' | 'tablet' | 'desktop'
 *
 * Breakpoint definitions:
 * - mobile: < 768px (below md breakpoint)
 * - tablet: 768px - 1023px (md to lg-1)
 * - desktop: >= 1024px (lg and above)
 */
export function useBreakpoint(): Breakpoint {
  const isDesktop = useMediaQuery(`(min-width: ${BREAKPOINTS.lg}px)`);
  const isTablet = useMediaQuery(`(min-width: ${BREAKPOINTS.md}px)`);

  if (isDesktop) {
    return 'desktop';
  }
  if (isTablet) {
    return 'tablet';
  }
  return 'mobile';
}

/**
 * Hook to check if the current viewport is mobile
 * @returns boolean indicating if viewport is mobile (< 768px)
 */
export function useIsMobile(): boolean {
  return useMediaQuery(`(max-width: ${BREAKPOINTS.md - 1}px)`);
}

/**
 * Hook to check if the current viewport is tablet
 * @returns boolean indicating if viewport is tablet (768px - 1023px)
 */
export function useIsTablet(): boolean {
  const isAboveMobile = useMediaQuery(`(min-width: ${BREAKPOINTS.md}px)`);
  const isBelowDesktop = useMediaQuery(`(max-width: ${BREAKPOINTS.lg - 1}px)`);
  return isAboveMobile && isBelowDesktop;
}

/**
 * Hook to check if the current viewport is desktop
 * @returns boolean indicating if viewport is desktop (>= 1024px)
 */
export function useIsDesktop(): boolean {
  return useMediaQuery(`(min-width: ${BREAKPOINTS.lg}px)`);
}
