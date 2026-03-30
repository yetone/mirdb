/**
 * Barrel export for hooks.
 *
 * Re-exports all hooks for cleaner imports:
 * import { useUrlShortener } from '@/hooks'
 */

export { useUrlShortener, getPendingUrl, clearPendingUrl, PENDING_URL_KEY } from './useUrlShortener'
