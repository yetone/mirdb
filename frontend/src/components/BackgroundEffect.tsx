import React from 'react';

interface BackgroundEffectProps {
  variant?: 'hero' | 'subtle';
}

export function BackgroundEffect({ variant = 'hero' }: BackgroundEffectProps) {
  return (
    <div
      className={`absolute inset-0 overflow-hidden pointer-events-none ${
        variant === 'hero' ? 'opacity-30' : 'opacity-10'
      }`}
      aria-hidden="true"
      data-testid="background-effect"
    >
      <div className="absolute top-1/4 left-1/4 w-96 h-96 bg-primary/20 rounded-full blur-3xl animate-pulse" />
      <div className="absolute bottom-1/4 right-1/4 w-80 h-80 bg-secondary/20 rounded-full blur-3xl animate-pulse delay-1000" />
      <div className="absolute top-1/2 left-1/2 w-64 h-64 bg-accent/20 rounded-full blur-3xl animate-pulse delay-500" />
    </div>
  );
}
