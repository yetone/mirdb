import { ReactNode } from 'react'
import { clsx } from 'clsx'

interface GlassMorphismCardProps {
  children: ReactNode
  className?: string
  title?: string
  icon?: ReactNode
  description?: string
  'data-testid'?: string
}

export default function GlassMorphismCard({
  children,
  className,
  title,
  icon,
  description,
  'data-testid': testId,
}: GlassMorphismCardProps) {
  return (
    <div
      data-testid={testId}
      className={clsx(
        'backdrop-blur-lg bg-white/10 rounded-2xl border border-white/20',
        'shadow-xl hover:shadow-2xl transition-all duration-300',
        'p-6',
        className
      )}
    >
      {icon && (
        <div className="flex items-center justify-center w-12 h-12 mb-4 rounded-lg bg-primary/20 text-primary" data-testid="card-icon">
          {icon}
        </div>
      )}
      {title && (
        <h3 className="text-xl font-bold mb-2" data-testid="card-title">
          {title}
        </h3>
      )}
      {description && (
        <p className="text-base-content/70" data-testid="card-description">
          {description}
        </p>
      )}
      {children}
    </div>
  )
}
