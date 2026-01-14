import { describe, it, expect, beforeEach, vi } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Home from '../pages/Home'
import StatsSection from '../components/StatsSection'
import { ThemeProvider } from '../contexts/ThemeContext'

/**
 * Social Proof Section Tests
 * Scenario: Verify social proof or usage statistics display (REQ-5)
 *
 * These tests verify that:
 * 1. The homepage displays a social proof/stats section
 * 2. Statistics are displayed (URLs shortened, clicks tracked, etc.)
 */

// Helper function to render Home with required providers
const renderHome = () => {
  return render(
    <MemoryRouter>
      <ThemeProvider defaultTheme="light">
        <Home />
      </ThemeProvider>
    </MemoryRouter>
  )
}

// Helper function to render StatsSection directly
const renderStatsSection = (
  fetchStats?: () => Promise<{ totalUrls: number; totalClicks: number; activeUsers: number }>
) => {
  return render(
    <MemoryRouter>
      <ThemeProvider defaultTheme="light">
        <StatsSection fetchStats={fetchStats} />
      </ThemeProvider>
    </MemoryRouter>
  )
}

describe('Social Proof Section - REQ-5', () => {
  beforeEach(() => {
    // Mock localStorage for ThemeProvider
    const localStorageMock = {
      getItem: vi.fn(() => null),
      setItem: vi.fn(),
      removeItem: vi.fn(),
      clear: vi.fn(),
    }
    Object.defineProperty(window, 'localStorage', { value: localStorageMock })

    // Mock matchMedia for system theme detection
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      value: vi.fn().mockImplementation((query: string) => ({
        matches: query === '(prefers-color-scheme: dark)' ? false : false,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      })),
    })
  })

  describe('Test Case 1: Render homepage and check for stats section', () => {
    it('navigating to homepage displays social proof section', () => {
      renderHome()

      // Step 1: Navigate to homepage (done via render with MemoryRouter)
      // Step 2: Locate social proof section
      const statsSection = screen.getByTestId('stats-section')
      expect(statsSection).toBeInTheDocument()
    })

    it('social proof section is found in the correct position on homepage', () => {
      renderHome()

      // Verify stats section is present
      const statsSection = screen.getByTestId('stats-section')
      expect(statsSection).toBeInTheDocument()

      // Verify it appears after features section and before demo section
      const featuresSection = screen.getByTestId('features-section')
      const demoSection = screen.getByTestId('demo-section')

      const featuresBeforeStats = featuresSection.compareDocumentPosition(statsSection) & Node.DOCUMENT_POSITION_FOLLOWING
      const statsBeforeDemo = statsSection.compareDocumentPosition(demoSection) & Node.DOCUMENT_POSITION_FOLLOWING

      expect(featuresBeforeStats).toBeTruthy()
      expect(statsBeforeDemo).toBeTruthy()
    })

    it('usage statistics section has proper accessibility labeling', () => {
      renderHome()

      const statsSection = screen.getByTestId('stats-section')
      expect(statsSection).toHaveAttribute('aria-labelledby', 'stats-heading')

      // Screen reader heading should exist
      const heading = statsSection.querySelector('#stats-heading')
      expect(heading).toBeInTheDocument()
      expect(heading).toHaveTextContent('Platform Statistics')
    })
  })

  describe('Test Case 2: Render social proof section with statistics displayed', () => {
    it('displays URLs shortened statistic', async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        totalUrls: 15000,
        totalClicks: 250000,
        activeUsers: 3000,
      })

      renderStatsSection(mockFetch)

      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
      })

      // Verify URLs shortened is displayed
      expect(screen.getByText('URLs Shortened')).toBeInTheDocument()
      expect(screen.getByText('15K+')).toBeInTheDocument()
    })

    it('displays total clicks statistic', async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        totalUrls: 15000,
        totalClicks: 250000,
        activeUsers: 3000,
      })

      renderStatsSection(mockFetch)

      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
      })

      // Verify Total Clicks is displayed
      expect(screen.getByText('Total Clicks')).toBeInTheDocument()
      expect(screen.getByText('250K+')).toBeInTheDocument()
    })

    it('displays active users statistic', async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        totalUrls: 15000,
        totalClicks: 250000,
        activeUsers: 3000,
      })

      renderStatsSection(mockFetch)

      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
      })

      // Verify Active Users is displayed
      expect(screen.getByText('Active Users')).toBeInTheDocument()
      expect(screen.getByText('3K+')).toBeInTheDocument()
    })

    it('displays three stats cards as trust indicators', async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        totalUrls: 10000,
        totalClicks: 500000,
        activeUsers: 5000,
      })

      renderStatsSection(mockFetch)

      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
      })

      // Verify all three stats cards are displayed
      const statsCards = screen.getAllByTestId('stats-card')
      expect(statsCards.length).toBe(3)
    })

    it('statistics are visually distinct with icons', async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        totalUrls: 10000,
        totalClicks: 500000,
        activeUsers: 5000,
      })

      renderStatsSection(mockFetch)

      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
      })

      // Each stat should have an icon (SVG element)
      const statsSection = screen.getByTestId('stats-section')
      const icons = statsSection.querySelectorAll('svg')
      expect(icons.length).toBe(3)

      // Icons should be aria-hidden for accessibility
      icons.forEach((icon) => {
        expect(icon).toHaveAttribute('aria-hidden', 'true')
      })
    })

    it('uses fallback statistics when API fails (graceful degradation)', async () => {
      const failingFetch = vi.fn().mockRejectedValue(new Error('API Error'))

      renderStatsSection(failingFetch)

      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
      })

      // Should show fallback notice
      expect(screen.getByText('Showing estimated statistics')).toBeInTheDocument()

      // Fallback values should still be displayed
      expect(screen.getByText('10K+')).toBeInTheDocument() // URLs
      expect(screen.getByText('500K+')).toBeInTheDocument() // Clicks
      expect(screen.getByText('5K+')).toBeInTheDocument() // Users
    })
  })

  describe('Social Proof Section - Visual and UX', () => {
    it('shows loading skeleton while fetching statistics', () => {
      const slowFetch = vi.fn().mockImplementation(
        () => new Promise(() => {}) // Never resolves
      )

      renderStatsSection(slowFetch)

      // Loading state should be visible
      expect(screen.getByTestId('stats-loading')).toBeInTheDocument()

      // Skeleton loaders should be visible
      const skeletons = screen.getAllByTestId('stats-skeleton')
      expect(skeletons.length).toBe(3)
    })

    it('stats section has appropriate background styling', () => {
      renderHome()

      const statsSection = screen.getByTestId('stats-section')
      expect(statsSection).toHaveClass('bg-base-300')
    })

    it('stats section is responsive with grid layout', async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        totalUrls: 10000,
        totalClicks: 500000,
        activeUsers: 5000,
      })

      renderStatsSection(mockFetch)

      await waitFor(() => {
        expect(screen.queryByTestId('stats-loading')).not.toBeInTheDocument()
      })

      // Check that the grid container exists with responsive classes
      const statsSection = screen.getByTestId('stats-section')
      const gridContainer = statsSection.querySelector('.grid')
      expect(gridContainer).toBeInTheDocument()
      expect(gridContainer).toHaveClass('md:grid-cols-3')
    })
  })
})
