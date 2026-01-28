import { ReactNode } from 'react';
import { clsx } from 'clsx';

export interface GlassMorphismCardProps {
  children: ReactNode;
  className?: string;
  hoverEffect?: boolean;
}

/**
 * GlassMorphismCard component
 *
 * A reusable card component with glass morphism styling effect.
 * Features backdrop blur, semi-transparent background, and subtle border.
 */
export function GlassMorphismCard({
  children,
  className = '',
  hoverEffect = true,
}: GlassMorphismCardProps) {
  return (
    <div
      data-testid="glass-morphism-card"
      className={clsx(
        // Glass morphism base styles
        'relative rounded-2xl',
        'bg-white/10 dark:bg-white/5',
        'backdrop-blur-md',
        'border border-white/20 dark:border-white/10',
        'shadow-xl',
        // Hover effect
        hoverEffect && 'transition-all duration-300 hover:bg-white/15 hover:scale-[1.02] hover:shadow-2xl',
        className
      )}
    >
      {children}
    </div>
  );
}
