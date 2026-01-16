import React from 'react'
import { motion } from 'framer-motion'

interface GlassMorphismCardProps {
  children: React.ReactNode
  className?: string
}

const GlassMorphismCard: React.FC<GlassMorphismCardProps> = ({
  children,
  className = '',
}) => {
  return (
    <motion.div
      className={`
        backdrop-blur-md
        bg-base-200/30
        border border-base-content/10
        rounded-2xl
        shadow-xl
        p-6
        ${className}
      `}
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5 }}
      whileHover={{ scale: 1.02 }}
      data-testid="glassmorphism-card"
    >
      {children}
    </motion.div>
  )
}

export default GlassMorphismCard
