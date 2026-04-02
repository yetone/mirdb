/**
 * Common utility functions for MirDB Homepage.
 */

/**
 * Combines class names, filtering out falsy values.
 */
export function cn(...classes: (string | undefined | null | false)[]): string {
  return classes.filter(Boolean).join(' ');
}

/**
 * Determines if a URL is external (absolute URL).
 */
export function isExternalUrl(url: string): boolean {
  return url.startsWith('http://') || url.startsWith('https://');
}
