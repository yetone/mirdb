import type { ReactNode } from 'react';

export interface GlassMorphismCardProps {
  children: ReactNode;
  className?: string;
  'data-testid'?: string;
}

export default function GlassMorphismCard({
  children,
  className = '',
  'data-testid': dataTestId,
}: GlassMorphismCardProps) {
  return (
    <div
      data-testid={dataTestId}
      className={[
        'glass-card',
        'relative overflow-hidden rounded-2xl',
        'bg-white/10 backdrop-blur-md',
        'border border-white/20',
        'shadow-lg shadow-black/5',
        'transition-all duration-300',
        'hover:bg-white/15 hover:shadow-xl',
        className,
      ].join(' ')}
    >
      {children}
    </div>
  );
}
