/**
 * Reused button primitive.
 * Stub created by first builder; owned by consumer scenarios.
 */
import React from 'react';

interface FuturisticButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'primary' | 'secondary';
}

export function FuturisticButton({ children, variant = 'primary', ...props }: FuturisticButtonProps) {
  return (
    <button className={`futuristic-btn ${variant}`} {...props}>
      {children}
    </button>
  );
}
