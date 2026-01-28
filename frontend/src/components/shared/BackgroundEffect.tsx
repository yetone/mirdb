/**
 * Background Effect Component
 * Shared component for animated background effects
 *
 * Requirements covered:
 * - REQ-5: Implement animated background effects to enhance visual appeal
 * - NFR-6: Ensure animations respect user motion preferences (prefers-reduced-motion)
 *
 * Features:
 * - Animated gradient background
 * - Floating particles/orbs effect
 * - Respects prefers-reduced-motion
 */

import { useEffect, useState } from 'react';

export function BackgroundEffect() {
  const [prefersReducedMotion, setPrefersReducedMotion] = useState(false);

  useEffect(() => {
    const mediaQuery = window.matchMedia('(prefers-reduced-motion: reduce)');
    setPrefersReducedMotion(mediaQuery.matches);

    const handler = (e: MediaQueryListEvent) => setPrefersReducedMotion(e.matches);
    mediaQuery.addEventListener('change', handler);
    return () => mediaQuery.removeEventListener('change', handler);
  }, []);

  if (prefersReducedMotion) {
    return (
      <div
        className="fixed inset-0 -z-10 bg-gradient-to-br from-base-300 via-base-100 to-base-300"
        data-testid="background-effect"
        aria-hidden="true"
      />
    );
  }

  return (
    <div
      className="fixed inset-0 -z-10 overflow-hidden"
      data-testid="background-effect"
      aria-hidden="true"
    >
      {/* Base gradient */}
      <div className="absolute inset-0 bg-gradient-to-br from-base-300 via-base-100 to-base-300" />

      {/* Animated orbs */}
      <div className="absolute top-1/4 left-1/4 w-96 h-96 bg-primary/20 rounded-full blur-3xl animate-pulse" />
      <div className="absolute bottom-1/4 right-1/4 w-96 h-96 bg-secondary/20 rounded-full blur-3xl animate-pulse delay-1000" />
      <div className="absolute top-1/2 left-1/2 w-64 h-64 bg-accent/10 rounded-full blur-2xl animate-pulse delay-500" />
    </div>
  );
}
