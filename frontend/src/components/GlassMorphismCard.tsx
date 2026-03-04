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
      className={`card bg-base-100/80 backdrop-blur-md shadow-xl border border-base-300/50 ${className}`}
      {...props}
    >
      {children}
    </motion.div>
  )
})

GlassMorphismCard.displayName = 'GlassMorphismCard'
