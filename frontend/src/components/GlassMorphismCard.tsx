/**
 * GlassMorphism Card Component
 *
 * A card component with translucent, blurred background (glassmorphism effect).
 * Used for feature cards and other content containers.
 */

import { motion, type HTMLMotionProps } from 'framer-motion'
import { forwardRef } from 'react'

export interface GlassMorphismCardProps extends HTMLMotionProps<'div'> {
  children: React.ReactNode
}

export const GlassMorphismCard = forwardRef<
  HTMLDivElement,
  GlassMorphismCardProps
>(({ children, className = '', ...props }, ref) => {
  return (
    <motion.div
      ref={ref}
      className={`backdrop-blur-md bg-base-100/70 border border-base-content/10 rounded-2xl p-6 shadow-xl ${className}`}
      {...props}
    >
      {children}
    </motion.div>
  )
})

GlassMorphismCard.displayName = 'GlassMorphismCard'

export default GlassMorphismCard
