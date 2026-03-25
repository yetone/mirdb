import React, { ReactNode } from 'react';

interface GlassMorphismCardProps {
  children: ReactNode;
  className?: string;
  'data-testid'?: string;
}

export function GlassMorphismCard({ children, className = '', 'data-testid': dataTestId }: GlassMorphismCardProps) {
  return (
    <div
      className={`backdrop-blur-md bg-base-100/30 border border-base-content/10 rounded-2xl p-6 shadow-xl ${className}`}
      data-testid={dataTestId}
    >
      {children}
    </div>
  );
}
