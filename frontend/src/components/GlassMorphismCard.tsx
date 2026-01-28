import { ReactNode } from 'react'
import { motion } from 'framer-motion'

interface GlassMorphismCardProps {
  children: ReactNode
  className?: string
}

export function GlassMorphismCard({ children, className = '' }: GlassMorphismCardProps) {
  return (
    <motion.div
      className={`card bg-base-100/80 backdrop-blur-sm shadow-xl border border-base-content/10 ${className}`}
      initial={{ opacity: 0, y: 20 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true }}
      transition={{ duration: 0.5 }}
    >
      {children}
    </motion.div>
  )
}
