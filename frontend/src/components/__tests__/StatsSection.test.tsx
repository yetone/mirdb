import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import StatsSection from '../StatsSection'

/**
 * Tests for StatsSection component
 * Scenario: Error Handling - Graceful Degradation
 * Test Cases 1 & 2: API failure and slow network handling
 */

// Helper to render with Router
const renderStatsSection = (
  fetchStats?: () => Promise<{ totalUrls: number; totalClicks: number; activeUsers: number }>
) => {
  return render(
    <MemoryRouter>
      <StatsSection fetchStats={fetchStats} />
    </MemoryRouter>
  )
}

describe('StatsSection', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('Test Case 1: Render homepage with failed stats API call', () => {
    it('renders fallback stats when API call fails', async () => {
      const failingFetchStats = vi.fn().mockRejectedValue(new Error('API Error'))

      renderStatsSection(failingFetchStats)

      // Wait for loading to complete
      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
      })

      // Stats section should still be rendered
      expect(screen.getByTestId('stats-section')).toBeInTheDocument()

      // Fallback notice should be displayed
      expect(screen.getByTestId('stats-fallback-notice')).toBeInTheDocument()
      expect(screen.getByText('Showing estimated statistics')).toBeInTheDocument()

      // Stats cards should still be displayed with fallback values
      const statsCards = screen.getAllByTestId('stats-card')
      expect(statsCards.length).toBe(3)
    })

    it('page renders without crashing when stats API fails', async () => {
      const failingFetchStats = vi.fn().mockRejectedValue(new Error('Network Error'))

      // This should not throw
      const { container } = renderStatsSection(failingFetchStats)

      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
      })

      // The component should have rendered content
      expect(container.querySelector('[data-testid="stats-section"]')).toBeInTheDocument()
    })

    it('displays fallback values when API returns error', async () => {
      const failingFetchStats = vi.fn().mockRejectedValue(new Error('500 Internal Server Error'))

      renderStatsSection(failingFetchStats)

      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
      })

      // Should display formatted numbers (10K+, 500K+, 5K+)
      expect(screen.getByText('10K+')).toBeInTheDocument()
      expect(screen.getByText('500K+')).toBeInTheDocument()
      expect(screen.getByText('5K+')).toBeInTheDocument()

      // Labels should be visible
      expect(screen.getByText('URLs Shortened')).toBeInTheDocument()
      expect(screen.getByText('Total Clicks')).toBeInTheDocument()
      expect(screen.getByText('Active Users')).toBeInTheDocument()
    })

    it('calls the fetch function on mount', async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        totalUrls: 1000,
        totalClicks: 50000,
        activeUsers: 500,
      })

      renderStatsSection(mockFetch)

      await waitFor(() => {
        expect(mockFetch).toHaveBeenCalledTimes(1)
      })
    })
  })

  describe('Test Case 2: Render homepage with slow network', () => {
    it('shows loading state while fetching data', async () => {
      // Create a promise that doesn't resolve immediately
      let resolvePromise: (value: unknown) => void
      const slowFetchStats = vi.fn().mockImplementation(
        () =>
          new Promise((resolve) => {
            resolvePromise = resolve
          })
      )

      renderStatsSection(slowFetchStats)

      // Loading state should be visible initially
      expect(screen.getByTestId('stats-loading')).toBeInTheDocument()

      // Skeleton loaders should be visible
      const skeletons = screen.getAllByTestId('stats-skeleton')
      expect(skeletons.length).toBe(3)

      // Resolve the promise
      resolvePromise!({
        totalUrls: 1000,
        totalClicks: 50000,
        activeUsers: 500,
      })

      // Wait for loading to finish
      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
      })

      // Stats should now be visible
      expect(screen.getAllByTestId('stats-card').length).toBe(3)
    })

    it('displays skeleton loaders with proper structure', () => {
      const slowFetchStats = vi.fn().mockImplementation(
        () => new Promise(() => {}) // Never resolves
      )

      renderStatsSection(slowFetchStats)

      // Check that loading skeleton is displayed
      const loadingContainer = screen.getByTestId('stats-loading')
      expect(loadingContainer).toBeInTheDocument()

      // Check skeleton structure
      const skeletons = screen.getAllByTestId('stats-skeleton')
      expect(skeletons.length).toBe(3)

      // Each skeleton should have the animate-pulse class
      skeletons.forEach((skeleton) => {
        expect(skeleton).toHaveClass('animate-pulse')
      })
    })

    it('transitions from loading to content smoothly', async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        totalUrls: 25000,
        totalClicks: 1500000,
        activeUsers: 8000,
      })

      renderStatsSection(mockFetch)

      // Initial loading state
      expect(screen.getByTestId('stats-loading')).toBeInTheDocument()

      // Wait for content to load
      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
      })

      // Content should now be visible
      const statsCards = screen.getAllByTestId('stats-card')
      expect(statsCards.length).toBe(3)

      // Verify data is displayed
      expect(screen.getByText('25K+')).toBeInTheDocument()
      expect(screen.getByText('1.5M+')).toBeInTheDocument()
      expect(screen.getByText('8K+')).toBeInTheDocument()
    })

    it('handles delayed success response correctly', async () => {
      const delayedFetch = vi.fn().mockImplementation(
        () =>
          new Promise((resolve) => {
            setTimeout(() => {
              resolve({
                totalUrls: 5000,
                totalClicks: 100000,
                activeUsers: 2000,
              })
            }, 100)
          })
      )

      renderStatsSection(delayedFetch)

      // Should show loading initially
      expect(screen.getByTestId('stats-loading')).toBeInTheDocument()

      // Wait for data to load
      await waitFor(
        () => {
          expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
        },
        { timeout: 500 }
      )

      // No fallback notice should be shown (successful request)
      expect(screen.queryByTestId('stats-fallback-notice')).not.toBeInTheDocument()

      // Data should be displayed
      expect(screen.getByText('5K+')).toBeInTheDocument()
      expect(screen.getByText('100K+')).toBeInTheDocument()
      expect(screen.getByText('2K+')).toBeInTheDocument()
    })
  })

  describe('Accessibility', () => {
    it('has proper section labeling for screen readers', async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        totalUrls: 1000,
        totalClicks: 50000,
        activeUsers: 500,
      })

      renderStatsSection(mockFetch)

      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
      })

      // Section should have aria-labelledby
      const section = screen.getByTestId('stats-section')
      expect(section).toHaveAttribute('aria-labelledby', 'stats-heading')

      // Heading should exist (sr-only)
      const heading = section.querySelector('#stats-heading')
      expect(heading).toBeInTheDocument()
      expect(heading).toHaveTextContent('Platform Statistics')
    })

    it('icons are hidden from screen readers', async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        totalUrls: 1000,
        totalClicks: 50000,
        activeUsers: 500,
      })

      renderStatsSection(mockFetch)

      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
      })

      // Check that icons have aria-hidden
      const icons = screen.getByTestId('stats-section').querySelectorAll('svg')
      icons.forEach((icon) => {
        expect(icon).toHaveAttribute('aria-hidden', 'true')
      })
    })
  })

  describe('Number formatting', () => {
    it('formats millions correctly', async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        totalUrls: 2500000,
        totalClicks: 10000000,
        activeUsers: 1000000,
      })

      renderStatsSection(mockFetch)

      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
      })

      expect(screen.getByText('2.5M+')).toBeInTheDocument()
      expect(screen.getByText('10.0M+')).toBeInTheDocument()
      expect(screen.getByText('1.0M+')).toBeInTheDocument()
    })

    it('formats thousands correctly', async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        totalUrls: 5000,
        totalClicks: 50000,
        activeUsers: 500,
      })

      renderStatsSection(mockFetch)

      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
      })

      expect(screen.getByText('5K+')).toBeInTheDocument()
      expect(screen.getByText('50K+')).toBeInTheDocument()
      expect(screen.getByText('500')).toBeInTheDocument() // Under 1000, shows exact number
    })
  })
})
