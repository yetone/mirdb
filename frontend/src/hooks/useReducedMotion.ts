/**
 * Reduced Motion Detection Hook.
 * Owner: Scenario 14 - Reduced Motion Support
 *
 * Detects user's prefers-reduced-motion preference:
 * - Returns boolean indicating reduced motion preference
 * - Updates on preference change
 * - Used by animation components to disable/minimize motion
 *
 * Expected exports:
 * - useReducedMotion(): boolean
 */
import { useState, useEffect } from 'react';

/**
 * Media query string for detecting reduced motion preference
 */
const REDUCED_MOTION_QUERY = '(prefers-reduced-motion: reduce)';

/**
 * Custom hook that detects whether the user prefers reduced motion.
 *
 * Uses the `prefers-reduced-motion` media query to detect system/browser
 * settings for motion sensitivity. This enables components to disable
 * or minimize animations for users who have requested reduced motion.
 *
 * The hook:
 * - Returns true when prefers-reduced-motion: reduce is active
 * - Returns false when no preference or prefers-reduced-motion: no-preference
 * - Automatically updates when the user's preference changes
 * - Cleans up event listeners on unmount
 *
 * @returns {boolean} True if the user prefers reduced motion, false otherwise
 *
 * @example
 * ```tsx
 * function AnimatedComponent() {
 *   const prefersReducedMotion = useReducedMotion();
 *
 *   return (
 *     <motion.div
 *       animate={{ opacity: 1 }}
 *       transition={prefersReducedMotion ? { duration: 0 } : { duration: 0.5 }}
 *     >
 *       Content
 *     </motion.div>
 *   );
 * }
 * ```
 */
export function useReducedMotion(): boolean {
  // Initialize with current preference if available (SSR-safe)
  const [prefersReducedMotion, setPrefersReducedMotion] = useState<boolean>(() => {
    // Check if window is available (for SSR compatibility)
    if (typeof window === 'undefined') {
      return false;
    }
    return window.matchMedia(REDUCED_MOTION_QUERY).matches;
  });

  useEffect(() => {
    // Create media query list
    const mediaQuery = window.matchMedia(REDUCED_MOTION_QUERY);

    // Update state with current value
    setPrefersReducedMotion(mediaQuery.matches);

    // Handler for preference changes
    const handleChange = (event: MediaQueryListEvent) => {
      setPrefersReducedMotion(event.matches);
    };

    // Add event listener for changes
    // Modern browsers support addEventListener, older browsers use addListener
    if (mediaQuery.addEventListener) {
      mediaQuery.addEventListener('change', handleChange);
    } else {
      // Fallback for older browsers (Safari < 14)
      mediaQuery.addListener(handleChange);
    }

    // Cleanup on unmount
    return () => {
      if (mediaQuery.removeEventListener) {
        mediaQuery.removeEventListener('change', handleChange);
      } else {
        // Fallback for older browsers
        mediaQuery.removeListener(handleChange);
      }
    };
  }, []);

  return prefersReducedMotion;
}
