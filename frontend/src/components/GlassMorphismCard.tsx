import { ReactNode } from 'react'
import { motion } from 'framer-motion'

interface GlassMorphismCardProps {
  children: ReactNode
  className?: string
}

export default function GlassMorphismCard({ children, className = '' }: GlassMorphismCardProps) {
  return (
    <motion.div
      className={`glass-card p-6 ${className}`}
      whileHover={{ scale: 1.02, y: -4 }}
      transition={{ duration: 0.2 }}
    >
      {children}
    </motion.div>
  )
}
