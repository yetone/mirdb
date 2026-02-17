/**
 * Animated background effect component.
 * Owner: Scenario 13 - Component Integration
 *
 * Features:
 * - Subtle animated visual effects
 * - Theme-aware colors
 * - Performance-optimized (CSS transforms)
 */

import React from 'react'

export function BackgroundEffect() {
  return (
    <div className="fixed inset-0 -z-10 overflow-hidden" aria-hidden="true">
      <div className="absolute top-0 -left-4 w-72 h-72 bg-primary/30 rounded-full mix-blend-multiply filter blur-xl opacity-70 animate-blob" />
      <div className="absolute top-0 -right-4 w-72 h-72 bg-secondary/30 rounded-full mix-blend-multiply filter blur-xl opacity-70 animate-blob animation-delay-2000" />
      <div className="absolute -bottom-8 left-20 w-72 h-72 bg-accent/30 rounded-full mix-blend-multiply filter blur-xl opacity-70 animate-blob animation-delay-4000" />
    </div>
  )
}
