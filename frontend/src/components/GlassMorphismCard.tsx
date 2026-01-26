import { ReactNode } from 'react'
import { clsx } from 'clsx'

interface GlassMorphismCardProps {
  children: ReactNode
  className?: string
}

export default function GlassMorphismCard({
  children,
  className,
}: GlassMorphismCardProps) {
  return (
    <div
      className={clsx(
        'card bg-base-100/80 backdrop-blur-md shadow-xl border border-base-300/50',
        className
      )}
    >
      <div className="card-body">{children}</div>
    </div>
  )
}
