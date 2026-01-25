import React from 'react';

interface GlassMorphismCardProps {
  children: React.ReactNode;
  className?: string;
}

export function GlassMorphismCard({ children, className = '' }: GlassMorphismCardProps) {
  return (
    <div
      className={`bg-base-100/60 backdrop-blur-md rounded-xl border border-base-200 p-6 shadow-lg ${className}`}
    >
      {children}
    </div>
  );
}
