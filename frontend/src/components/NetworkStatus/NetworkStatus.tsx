import { type ReactNode } from 'react'
import { useNetworkStatus } from '../../hooks'
import './NetworkStatus.css'

/**
 * NetworkStatus component displays an offline indicator when the user
 * loses network connectivity. Gracefully handles network failures
 * by informing users while still rendering children (cached content).
 */
export interface NetworkStatusProps {
  /** Child components to render */
  children?: ReactNode
}

export const NetworkStatus: React.FC<NetworkStatusProps> = ({ children }) => {
  const { isOnline } = useNetworkStatus()

  return (
    <>
      {!isOnline && (
        <div
          className="offline-indicator"
          data-testid="offline-indicator"
          role="alert"
          aria-live="polite"
        >
          <svg
            className="offline-icon"
            xmlns="http://www.w3.org/2000/svg"
            width="20"
            height="20"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
            aria-hidden="true"
          >
            <line x1="1" y1="1" x2="23" y2="23" />
            <path d="M16.72 11.06A10.94 10.94 0 0 1 19 12.55" />
            <path d="M5 12.55a10.94 10.94 0 0 1 5.17-2.39" />
            <path d="M10.71 5.05A16 16 0 0 1 22.58 9" />
            <path d="M1.42 9a15.91 15.91 0 0 1 4.7-2.88" />
            <path d="M8.53 16.11a6 6 0 0 1 6.95 0" />
            <line x1="12" y1="20" x2="12.01" y2="20" />
          </svg>
          <span className="offline-message">
            You are currently offline. Some content may be unavailable.
          </span>
        </div>
      )}
      {children}
    </>
  )
}
