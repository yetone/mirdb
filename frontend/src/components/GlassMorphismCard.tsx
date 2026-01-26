import { ReactNode } from 'react'
import { motion } from 'framer-motion'

interface GlassMorphismCardProps {
  children: ReactNode
  className?: string
}

export function GlassMorphismCard({ children, className = '' }: GlassMorphismCardProps) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      whileInView={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5 }}
      className={`card bg-base-100/70 backdrop-blur-md shadow-xl border border-base-300/50 ${className}`}
    >
      {children}
    </motion.div>
  )
}
