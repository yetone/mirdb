/**
 * BackgroundEffect Component
 * Owner: Scenario 11 - Background Visual Effects
 *
 * Animated background effect component using Framer Motion.
 * Provides a subtle, non-intrusive visual enhancement to pages.
 * Uses DaisyUI theme-aware colors for seamless theme switching.
 *
 * Expected exports:
 * - BackgroundEffect: React.FC - Animated background component
 *
 * Features:
 * - Framer Motion animations with continuous loop
 * - Theme-aware color adaptation using DaisyUI
 * - Fixed positioning with low z-index
 * - Pointer events disabled to not block content
 * - Accessible (aria-hidden for screen readers)
 */

import { motion } from 'framer-motion'

export function BackgroundEffect() {
  return (
    <motion.div
      data-testid="background-effect"
      aria-hidden="true"
      className="fixed inset-0 z-0 pointer-events-none overflow-hidden bg-base-100"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{
        duration: 1,
        repeat: Infinity,
        repeatType: 'reverse',
        ease: 'easeInOut',
      }}
    >
      {/* Gradient orb 1 - Primary color */}
      <motion.div
        className="absolute w-96 h-96 rounded-full bg-primary/20 blur-3xl"
        initial={{ x: -100, y: -100, scale: 0.8 }}
        animate={{ x: 100, y: 100, scale: 1.2 }}
        transition={{
          duration: 8,
          repeat: Infinity,
          repeatType: 'reverse',
          ease: 'easeInOut',
        }}
        style={{ top: '10%', left: '10%' }}
      />

      {/* Gradient orb 2 - Secondary color */}
      <motion.div
        className="absolute w-80 h-80 rounded-full bg-secondary/20 blur-3xl"
        initial={{ x: 50, y: 50, scale: 1 }}
        animate={{ x: -50, y: -50, scale: 0.9 }}
        transition={{
          duration: 10,
          repeat: Infinity,
          repeatType: 'reverse',
          ease: 'easeInOut',
        }}
        style={{ top: '50%', right: '20%' }}
      />

      {/* Gradient orb 3 - Accent color */}
      <motion.div
        className="absolute w-72 h-72 rounded-full bg-accent/15 blur-3xl"
        initial={{ x: 0, y: 0, scale: 1.1 }}
        animate={{ x: 80, y: -60, scale: 0.85 }}
        transition={{
          duration: 12,
          repeat: Infinity,
          repeatType: 'reverse',
          ease: 'easeInOut',
        }}
        style={{ bottom: '20%', left: '30%' }}
      />

      {/* Subtle grid overlay */}
      <div
        className="absolute inset-0 bg-base-content/5"
        style={{
          backgroundImage: `linear-gradient(rgba(var(--bc) / 0.03) 1px, transparent 1px),
                           linear-gradient(90deg, rgba(var(--bc) / 0.03) 1px, transparent 1px)`,
          backgroundSize: '50px 50px',
        }}
      />
    </motion.div>
  )
}

export default BackgroundEffect
