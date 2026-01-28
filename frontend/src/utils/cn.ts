/**
 * Utility function for merging class names.
 * Uses clsx for conditional class names and tailwind-merge to handle Tailwind conflicts.
 */

import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}
