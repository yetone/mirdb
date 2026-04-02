/**
 * Layout container wrapper for consistent max-width and padding.
 * Owner: First Builder (Shared)
 */
import React from 'react';
import { ContainerProps } from '@/types';
import { cn } from '@/utils/helpers';

export function Container({ children, className }: ContainerProps) {
  return (
    <div className={cn('max-w-7xl mx-auto px-4 sm:px-6 lg:px-8', className)}>
      {children}
    </div>
  );
}
