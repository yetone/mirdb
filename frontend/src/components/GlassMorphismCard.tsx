import { ReactNode, HTMLAttributes } from 'react'
import { clsx } from 'clsx'

interface GlassMorphismCardProps extends HTMLAttributes<HTMLDivElement> {
  children: ReactNode
  className?: string
}

export default function GlassMorphismCard({
  children,
  className,
  ...props
}: GlassMorphismCardProps) {
  return (
    <div
      className={clsx(
        'card bg-base-100/80 backdrop-blur-md shadow-xl border border-base-300/50 transition-all duration-300 hover:shadow-2xl hover:scale-[1.02]',
        className
      )}
      {...props}
    >
      <div className="card-body">{children}</div>
    </div>
  )
}
