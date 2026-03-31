/**
 * Common utility functions for the MirDB Homepage.
 */

export function cn(...classes: (string | undefined | false)[]): string {
  return classes.filter(Boolean).join(' ');
}
