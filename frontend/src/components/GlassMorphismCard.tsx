import React from 'react';

interface GlassMorphismCardProps {
  children: React.ReactNode;
  className?: string;
}

export function GlassMorphismCard({ children, className = '' }: GlassMorphismCardProps) {
  return (
    <div className={`card bg-base-200/70 backdrop-blur-md shadow-xl border border-base-300/50 ${className}`}>
      {children}
    </div>
  );
}
