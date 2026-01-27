import { ReactNode } from 'react';
import { clsx } from 'clsx';

interface GlassMorphismCardProps {
  children: ReactNode;
  className?: string;
}

export function GlassMorphismCard({ children, className = '' }: GlassMorphismCardProps) {
  return (
    <div
      className={clsx(
        'backdrop-blur-md bg-base-100/30 border border-base-content/10',
        'rounded-xl shadow-xl p-6',
        'transition-all duration-300 hover:shadow-2xl hover:bg-base-100/40',
        className
      )}
    >
      {children}
    </div>
  );
}

export default GlassMorphismCard;
