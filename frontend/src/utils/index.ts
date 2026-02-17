/**
 * Common utility functions.
 * Owner: First builder (shared)
 */

/** Format a date string to a readable format */
export function formatDate(dateString: string): string {
  const date = new Date(dateString)
  return date.toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
  })
}

/** Truncate a string to a maximum length */
export function truncate(str: string, maxLength: number): string {
  if (str.length <= maxLength) return str
  return str.slice(0, maxLength - 3) + '...'
}

/** Generate a unique ID */
export function generateId(): string {
  return Math.random().toString(36).substring(2, 11)
}

/** Class name utility for conditional classes */
export function cn(...classes: (string | undefined | null | false)[]): string {
  return classes.filter(Boolean).join(' ')
}
