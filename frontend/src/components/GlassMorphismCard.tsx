import { ReactNode } from 'react'

interface GlassMorphismCardProps {
  children: ReactNode
  className?: string
}

function GlassMorphismCard({ children, className = '' }: GlassMorphismCardProps) {
  return (
    <div
      className={`backdrop-blur-md bg-base-100/30 border border-base-content/10 rounded-2xl shadow-xl ${className}`}
      data-testid="glassmorphism-card"
    >
      {children}
    </div>
  )
}

export default GlassMorphismCard
