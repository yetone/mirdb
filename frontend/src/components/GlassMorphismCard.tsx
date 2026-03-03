import type { ReactNode } from 'react';

interface GlassMorphismCardProps {
  children: ReactNode;
  className?: string;
  hover?: boolean;
}

export function GlassMorphismCard({
  children,
  className = '',
  hover = true,
}: GlassMorphismCardProps) {
  return (
    <div
      className={`
        card bg-base-100/80 backdrop-blur-md
        border border-base-300
        rounded-xl shadow-xl
        p-6
        ${hover ? 'transition-all duration-300 hover:scale-105 hover:shadow-2xl hover:border-primary' : ''}
        ${className}
      `}
    >
      {children}
    </div>
  );
}

export default GlassMorphismCard;
