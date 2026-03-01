import { ReactNode } from 'react'

interface BackgroundEffectProps {
  children: ReactNode
}

export default function BackgroundEffect({ children }: BackgroundEffectProps) {
  return (
    <div className="min-h-screen bg-gradient-to-br from-base-300 via-base-100 to-base-300 relative overflow-hidden">
      {/* Animated gradient orbs */}
      <div className="absolute inset-0 overflow-hidden pointer-events-none">
        <div className="absolute -top-40 -right-40 w-80 h-80 bg-primary/20 rounded-full blur-3xl animate-pulse" />
        <div className="absolute -bottom-40 -left-40 w-80 h-80 bg-secondary/20 rounded-full blur-3xl animate-pulse delay-1000" />
        <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-96 h-96 bg-accent/10 rounded-full blur-3xl animate-pulse delay-500" />
      </div>
      {/* Content */}
      <div className="relative z-10">
        {children}
      </div>
    </div>
  )
}
