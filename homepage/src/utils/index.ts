/**
 * Utility functions for the MirDB homepage.
 */

/**
 * Combines class names, filtering out falsy values.
 */
export function classNames(...classes: (string | boolean | undefined | null)[]): string {
  return classes.filter(Boolean).join(' ')
}
