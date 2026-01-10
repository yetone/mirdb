import type { ReactNode } from 'react'

interface GlassMorphismCardProps {
  children: ReactNode
  className?: string
}

function GlassMorphismCard({ children, className = '' }: GlassMorphismCardProps) {
  return (
    <div
      className={`backdrop-blur-md bg-base-100/30 border border-base-content/10 rounded-2xl shadow-xl hover:shadow-2xl hover:scale-[1.02] hover:border-primary/30 transition-all duration-300 ${className}`}
      data-testid="glassmorphism-card"
    >
      {children}
    </div>
  )
}

export default GlassMorphismCard
