import { ReactNode } from 'react'
import { motion, HTMLMotionProps } from 'framer-motion'

interface GlassMorphismCardProps extends Omit<HTMLMotionProps<'div'>, 'children'> {
  children: ReactNode
  className?: string
  'data-testid'?: string
}

/**
 * GlassMorphismCard - A card component with glassmorphism styling
 * Features backdrop-filter blur for the frosted glass effect
 * NFR-4 compliant design consistency component
 */
const GlassMorphismCard = ({
  children,
  className = '',
  'data-testid': testId,
  ...motionProps
}: GlassMorphismCardProps) => {
  return (
    <motion.div
      className={`
        card
        bg-base-100/80
        backdrop-blur-md
        border border-base-content/10
        shadow-xl
        hover:shadow-2xl
        transition-shadow
        duration-300
        ${className}
      `.replace(/\s+/g, ' ').trim()}
      data-testid={testId}
      {...motionProps}
    >
      {children}
    </motion.div>
  )
}

export default GlassMorphismCard
