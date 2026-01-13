import { ReactNode } from 'react'
import { clsx } from 'clsx'
import { twMerge } from 'tailwind-merge'

interface GlassMorphismCardProps {
  children: ReactNode
  className?: string
  'data-testid'?: string
}

export function GlassMorphismCard({
  children,
  className,
  'data-testid': testId
}: GlassMorphismCardProps) {
  return (
    <div
      data-testid={testId}
      className={twMerge(
        clsx(
          'backdrop-blur-md bg-base-100/30 border border-base-content/10',
          'rounded-2xl p-6 shadow-xl',
          'transition-all duration-300 hover:shadow-2xl hover:scale-[1.02]',
          'glassmorphism-card',
          className
        )
      )}
    >
      {children}
    </div>
  )
}

export default GlassMorphismCard
