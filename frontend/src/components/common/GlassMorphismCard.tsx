import React, { ReactNode } from 'react';

interface GlassMorphismCardProps {
  children: ReactNode;
  className?: string;
  testId?: string;
}

export function GlassMorphismCard({ children, className = '', testId }: GlassMorphismCardProps) {
  return (
    <div
      className={`bg-base-100/50 backdrop-blur-md border border-base-content/10 rounded-xl p-6 shadow-lg hover:shadow-xl transition-shadow duration-300 ${className}`}
      data-testid={testId}
    >
      {children}
    </div>
  );
}
