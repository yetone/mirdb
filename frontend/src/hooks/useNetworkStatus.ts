import { useState, useEffect, useCallback } from 'react'

/**
 * Hook to monitor network connectivity status
 * Provides real-time updates when the browser goes online/offline
 */
export interface NetworkStatusState {
  /** Whether the browser is currently online */
  isOnline: boolean
}

export function useNetworkStatus(): NetworkStatusState {
  const [isOnline, setIsOnline] = useState<boolean>(() => {
    // Check if navigator is available (for SSR compatibility)
    if (typeof navigator !== 'undefined') {
      return navigator.onLine
    }
    return true // Assume online in SSR
  })

  const handleOnline = useCallback(() => {
    setIsOnline(true)
  }, [])

  const handleOffline = useCallback(() => {
    setIsOnline(false)
  }, [])

  useEffect(() => {
    // Add event listeners for online/offline events
    window.addEventListener('online', handleOnline)
    window.addEventListener('offline', handleOffline)

    // Sync with current state
    setIsOnline(navigator.onLine)

    // Cleanup event listeners on unmount
    return () => {
      window.removeEventListener('online', handleOnline)
      window.removeEventListener('offline', handleOffline)
    }
  }, [handleOnline, handleOffline])

  return { isOnline }
}
