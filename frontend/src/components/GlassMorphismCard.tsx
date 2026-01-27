import { motion } from 'framer-motion'
import { ReactNode } from 'react'

interface GlassMorphismCardProps {
  children: ReactNode
  className?: string
}

export default function GlassMorphismCard({ children, className = '' }: GlassMorphismCardProps) {
  return (
    <motion.div
      className={`backdrop-blur-md bg-base-100/30 border border-base-content/10 rounded-2xl shadow-xl ${className}`}
      initial={{ opacity: 0, y: 20 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true }}
      transition={{ duration: 0.5 }}
    >
      {children}
    </motion.div>
  )
}
