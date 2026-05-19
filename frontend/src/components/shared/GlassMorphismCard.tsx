/**
 * Reused glass card primitive.
 * Stub created by first builder; owned by consumer scenarios.
 */
import React from 'react';

interface GlassMorphismCardProps {
  children: React.ReactNode;
  className?: string;
}

export function GlassMorphismCard({ children, className = '' }: GlassMorphismCardProps) {
  return (
    <div className={`glass-card ${className}`}>
      {children}
    </div>
  );
}
