/**
 * Layout container wrapper for consistent max-width and padding.
 * Owner: First Builder (Shared)
 */
import React from 'react';
import { ContainerProps } from '@/types';

export function Container({ children, className = '' }: ContainerProps) {
  return (
    <div className={`container ${className}`}>
      {children}
    </div>
  );
}
