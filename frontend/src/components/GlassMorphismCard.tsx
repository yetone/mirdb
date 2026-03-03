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
        bg-base-100/50 backdrop-blur-md
        border border-base-300
        rounded-xl shadow-lg
        p-6
        ${hover ? 'transition-all duration-300 hover:scale-[1.02] hover:shadow-xl hover:border-primary/30' : ''}
        ${className}
      `}
    >
      {children}
    </div>
  );
}
