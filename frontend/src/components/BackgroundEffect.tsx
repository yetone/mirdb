/**
 * BackgroundEffect Component
 *
 * Provides animated visual background effects for the homepage.
 * Adds visual appeal with subtle animations and gradient backgrounds.
 *
 * Requirements:
 * - NFR-3: Framer Motion for smooth animations
 * - Performance optimized with CSS transforms
 * - Respects prefers-reduced-motion
 */
import { motion } from 'framer-motion'
import { useEffect, useState } from 'react'

export interface BackgroundEffectProps {
  className?: string
  variant?: 'default' | 'subtle' | 'intense'
}

export function BackgroundEffect({
  className = '',
  variant = 'default',
}: BackgroundEffectProps) {
  const [prefersReducedMotion, setPrefersReducedMotion] = useState(false)

  useEffect(() => {
    const mediaQuery = window.matchMedia('(prefers-reduced-motion: reduce)')
    setPrefersReducedMotion(mediaQuery.matches)

    const handleChange = (e: MediaQueryListEvent) => {
      setPrefersReducedMotion(e.matches)
    }

    mediaQuery.addEventListener('change', handleChange)
    return () => mediaQuery.removeEventListener('change', handleChange)
  }, [])

  const animationDuration = prefersReducedMotion ? 0 : variant === 'intense' ? 15 : 20

  const opacityMap = {
    default: 0.3,
    subtle: 0.15,
    intense: 0.5,
  }

  return (
    <div
      className={`fixed inset-0 overflow-hidden pointer-events-none z-0 ${className}`}
      data-testid="background-effect"
      aria-hidden="true"
    >
      {/* Gradient Orb 1 */}
      <motion.div
        className="absolute -top-1/4 -left-1/4 w-1/2 h-1/2 rounded-full bg-primary blur-3xl"
        style={{ opacity: opacityMap[variant] }}
        animate={
          prefersReducedMotion
            ? {}
            : {
                x: [0, 100, 50, 0],
                y: [0, 50, 100, 0],
                scale: [1, 1.1, 0.9, 1],
              }
        }
        transition={{
          duration: animationDuration,
          repeat: Infinity,
          ease: 'easeInOut',
        }}
        data-testid="background-orb-1"
      />

      {/* Gradient Orb 2 */}
      <motion.div
        className="absolute -bottom-1/4 -right-1/4 w-1/2 h-1/2 rounded-full bg-secondary blur-3xl"
        style={{ opacity: opacityMap[variant] }}
        animate={
          prefersReducedMotion
            ? {}
            : {
                x: [0, -100, -50, 0],
                y: [0, -50, -100, 0],
                scale: [1, 0.9, 1.1, 1],
              }
        }
        transition={{
          duration: animationDuration,
          repeat: Infinity,
          ease: 'easeInOut',
          delay: 2,
        }}
        data-testid="background-orb-2"
      />

      {/* Grid pattern overlay */}
      <div
        className="absolute inset-0 bg-[linear-gradient(rgba(255,255,255,.02)_1px,transparent_1px),linear-gradient(90deg,rgba(255,255,255,.02)_1px,transparent_1px)] bg-[size:50px_50px]"
        data-testid="background-grid"
      />
    </div>
  )
}

export default BackgroundEffect
