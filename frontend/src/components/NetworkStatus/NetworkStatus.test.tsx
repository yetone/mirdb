import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, act } from '@testing-library/react'
import { NetworkStatus } from './NetworkStatus'

/**
 * Integration tests for NetworkStatus component
 * Test Case 3: Test content API failure - Page displays fallback content or error state gracefully
 */
describe('NetworkStatus Component', () => {
  beforeEach(() => {
    // Reset online status mock
    vi.stubGlobal('navigator', {
      ...window.navigator,
      onLine: true,
    })
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  describe('Online state', () => {
    it('does not show offline indicator when online', () => {
      render(<NetworkStatus />)

      const offlineIndicator = screen.queryByTestId('offline-indicator')
      expect(offlineIndicator).not.toBeInTheDocument()
    })

    it('renders children when online', () => {
      render(
        <NetworkStatus>
          <div data-testid="child-content">Online Content</div>
        </NetworkStatus>
      )

      expect(screen.getByTestId('child-content')).toBeInTheDocument()
    })
  })

  describe('Offline state', () => {
    beforeEach(() => {
      vi.stubGlobal('navigator', {
        ...window.navigator,
        onLine: false,
      })
    })

    it('shows offline indicator when offline', () => {
      render(<NetworkStatus />)

      const offlineIndicator = screen.getByTestId('offline-indicator')
      expect(offlineIndicator).toBeInTheDocument()
    })

    it('displays appropriate offline message', () => {
      render(<NetworkStatus />)

      expect(screen.getByText(/you are currently offline/i)).toBeInTheDocument()
    })

    it('still renders children when offline', () => {
      render(
        <NetworkStatus>
          <div data-testid="child-content">Content</div>
        </NetworkStatus>
      )

      // Children should still render (cached content)
      expect(screen.getByTestId('child-content')).toBeInTheDocument()
      // But offline indicator should also be shown
      expect(screen.getByTestId('offline-indicator')).toBeInTheDocument()
    })
  })

  describe('Network state changes', () => {
    it('shows offline indicator when going offline', async () => {
      vi.stubGlobal('navigator', {
        ...window.navigator,
        onLine: true,
      })

      render(<NetworkStatus />)

      // Initially online - no indicator
      expect(screen.queryByTestId('offline-indicator')).not.toBeInTheDocument()

      // Simulate going offline
      await act(async () => {
        vi.stubGlobal('navigator', {
          ...window.navigator,
          onLine: false,
        })
        window.dispatchEvent(new Event('offline'))
      })

      expect(screen.getByTestId('offline-indicator')).toBeInTheDocument()
    })

    it('hides offline indicator when coming back online', async () => {
      vi.stubGlobal('navigator', {
        ...window.navigator,
        onLine: false,
      })

      render(<NetworkStatus />)

      // Initially offline
      expect(screen.getByTestId('offline-indicator')).toBeInTheDocument()

      // Simulate coming online
      await act(async () => {
        vi.stubGlobal('navigator', {
          ...window.navigator,
          onLine: true,
        })
        window.dispatchEvent(new Event('online'))
      })

      expect(screen.queryByTestId('offline-indicator')).not.toBeInTheDocument()
    })
  })

  describe('Accessibility', () => {
    beforeEach(() => {
      vi.stubGlobal('navigator', {
        ...window.navigator,
        onLine: false,
      })
    })

    it('offline indicator has proper aria attributes', () => {
      render(<NetworkStatus />)

      const indicator = screen.getByTestId('offline-indicator')
      expect(indicator).toHaveAttribute('role', 'alert')
      expect(indicator).toHaveAttribute('aria-live', 'polite')
    })

    it('offline message is accessible to screen readers', () => {
      render(<NetworkStatus />)

      const message = screen.getByText(/you are currently offline/i)
      expect(message).toBeInTheDocument()
    })
  })
})
