import { ReactNode } from 'react'
import { motion } from 'framer-motion'

interface GlassMorphismCardProps {
  children: ReactNode
  className?: string
}

export function GlassMorphismCard({ children, className = '' }: GlassMorphismCardProps) {
  return (
    <motion.div
      className={`backdrop-blur-md bg-base-100/50 rounded-2xl p-6 shadow-xl border border-base-300/50 ${className}`}
      initial={{ opacity: 0, y: 20 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true }}
      transition={{ duration: 0.5 }}
    >
      {children}
    </motion.div>
  )
}

export default GlassMorphismCard
