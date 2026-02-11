/**
 * Utility functions for the MirDB homepage.
 */

export function classNames(...classes: (string | undefined | false)[]): string {
  return classes.filter(Boolean).join(' ')
}
