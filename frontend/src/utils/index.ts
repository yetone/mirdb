/**
 * Utility Functions
 * Owner: First scenario builder
 *
 * Shared utility functions for the homepage
 */

export function classNames(...classes: (string | undefined | false)[]): string {
  return classes.filter(Boolean).join(' ');
}
