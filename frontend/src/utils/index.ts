/**
 * Common utility functions.
 *
 * Shared utilities used across the application.
 */

/** Format a date string to a readable format */
export function formatDate(dateString: string): string {
  return new Date(dateString).toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
  })
}

/** Format a number with commas for readability */
export function formatNumber(num: number): string {
  return num.toLocaleString('en-US')
}

/** Truncate a string to a maximum length with ellipsis */
export function truncateString(str: string, maxLength: number): string {
  if (str.length <= maxLength) return str
  return str.slice(0, maxLength - 3) + '...'
}

/** Generate a full short URL from a short code */
export function getShortUrl(shortCode: string): string {
  const baseUrl = window.location.origin
  return `${baseUrl}/${shortCode}`
}

/** Delay execution for a specified time (useful for debouncing) */
export function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
