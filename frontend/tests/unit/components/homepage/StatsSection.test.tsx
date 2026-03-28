/**
 * Unit and integration tests for StatsSection component.
 * Owner: Scenario 4 - Social Proof Statistics Section
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor, act } from '../../../setup'
import { StatsSection } from '@/components/homepage/StatsSection'
import { PublicStats, DEFAULT_STATS } from '@/api'

describe('StatsSection', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  // Helper to create mock fetch functions
  const createMockFetch = (data: PublicStats, delay = 0): (() => Promise<PublicStats>) => {
    return () => new Promise((resolve) => {
      if (delay > 0) {
        setTimeout(() => resolve(data), delay)
      } else {
        resolve(data)
      }
    })
  }

  const createFailingMockFetch = (): (() => Promise<PublicStats>) => {
    return () => Promise.reject(new Error('API Error'))
  }

  const testStats: PublicStats = {
    linksShortened: 2000000,
    clicksTracked: 10000000,
    activeUsers: 50000,
  }

  // Test Case 1: Statistics section is present in the DOM
  describe('Test Case 1: Statistics section presence', () => {
    it('renders the statistics section in the DOM', async () => {
      const mockFetch = createMockFetch(testStats)

      await act(async () => {
        render(<StatsSection fetchStats={mockFetch} />)
      })

      const statsSection = screen.getByTestId('stats-section')
      expect(statsSection).toBeInTheDocument()
    })

    it('has correct aria-label for accessibility', async () => {
      const mockFetch = createMockFetch(testStats)

      await act(async () => {
        render(<StatsSection fetchStats={mockFetch} />)
      })

      const statsSection = screen.getByTestId('stats-section')
      expect(statsSection).toHaveAttribute('aria-label', 'Platform Statistics')
    })

    it('renders section heading', async () => {
      const mockFetch = createMockFetch(testStats)

      await act(async () => {
        render(<StatsSection fetchStats={mockFetch} />)
      })

      const heading = screen.getByRole('heading', { level: 2 })
      expect(heading).toBeInTheDocument()
      expect(heading.textContent).toContain('Trusted')
    })
  })

  // Test Case 2: Links Shortened statistic is displayed
  describe('Test Case 2: Links Shortened statistic', () => {
    it('displays Links Shortened statistic card', async () => {
      const mockFetch = createMockFetch(testStats)

      await act(async () => {
        render(<StatsSection fetchStats={mockFetch} />)
      })

      await waitFor(() => {
        const linksCard = screen.getByTestId('stat-links-shortened')
        expect(linksCard).toBeInTheDocument()
      })
    })

    it('displays Links Shortened label', async () => {
      const mockFetch = createMockFetch(testStats)

      await act(async () => {
        render(<StatsSection fetchStats={mockFetch} />)
      })

      await waitFor(() => {
        const label = screen.getByTestId('stat-links-shortened-label')
        expect(label).toBeInTheDocument()
        expect(label.textContent).toBe('Links Shortened')
      })
    })

    it('displays Links Shortened value', async () => {
      const mockFetch = createMockFetch(testStats)

      await act(async () => {
        render(<StatsSection fetchStats={mockFetch} />)
      })

      await waitFor(() => {
        const value = screen.getByTestId('stat-links-shortened-value')
        expect(value).toBeInTheDocument()
        expect(value.textContent).toBe('2,000,000')
      })
    })
  })

  // Test Case 3: Clicks Tracked statistic is displayed
  describe('Test Case 3: Clicks Tracked statistic', () => {
    it('displays Clicks Tracked statistic card', async () => {
      const mockFetch = createMockFetch(testStats)

      await act(async () => {
        render(<StatsSection fetchStats={mockFetch} />)
      })

      await waitFor(() => {
        const clicksCard = screen.getByTestId('stat-clicks-tracked')
        expect(clicksCard).toBeInTheDocument()
      })
    })

    it('displays Clicks Tracked label', async () => {
      const mockFetch = createMockFetch(testStats)

      await act(async () => {
        render(<StatsSection fetchStats={mockFetch} />)
      })

      await waitFor(() => {
        const label = screen.getByTestId('stat-clicks-tracked-label')
        expect(label).toBeInTheDocument()
        expect(label.textContent).toBe('Clicks Tracked')
      })
    })

    it('displays Clicks Tracked value', async () => {
      const mockFetch = createMockFetch(testStats)

      await act(async () => {
        render(<StatsSection fetchStats={mockFetch} />)
      })

      await waitFor(() => {
        const value = screen.getByTestId('stat-clicks-tracked-value')
        expect(value).toBeInTheDocument()
        expect(value.textContent).toBe('10,000,000')
      })
    })
  })

  // Test Case 4: Active Users statistic is displayed
  describe('Test Case 4: Active Users statistic', () => {
    it('displays Active Users statistic card', async () => {
      const mockFetch = createMockFetch(testStats)

      await act(async () => {
        render(<StatsSection fetchStats={mockFetch} />)
      })

      await waitFor(() => {
        const usersCard = screen.getByTestId('stat-active-users')
        expect(usersCard).toBeInTheDocument()
      })
    })

    it('displays Active Users label', async () => {
      const mockFetch = createMockFetch(testStats)

      await act(async () => {
        render(<StatsSection fetchStats={mockFetch} />)
      })

      await waitFor(() => {
        const label = screen.getByTestId('stat-active-users-label')
        expect(label).toBeInTheDocument()
        expect(label.textContent).toBe('Active Users')
      })
    })

    it('displays Active Users value', async () => {
      const mockFetch = createMockFetch(testStats)

      await act(async () => {
        render(<StatsSection fetchStats={mockFetch} />)
      })

      await waitFor(() => {
        const value = screen.getByTestId('stat-active-users-value')
        expect(value).toBeInTheDocument()
        expect(value.textContent).toBe('50,000')
      })
    })
  })

  // Test Case 5: Statistics load from API within 2 seconds or show fallback values
  describe('Test Case 5: API loading within 2 seconds', () => {
    it('shows loading state initially', () => {
      const mockFetch = createMockFetch(testStats, 100)
      render(<StatsSection fetchStats={mockFetch} />)

      const loadingState = screen.getByTestId('stats-loading')
      expect(loadingState).toBeInTheDocument()
    })

    it('loads statistics from API within 2 seconds', async () => {
      const mockFetch = createMockFetch(testStats, 100)

      render(<StatsSection fetchStats={mockFetch} />)

      // Initially shows loading
      expect(screen.getByTestId('stats-loading')).toBeInTheDocument()

      // Wait for content to appear
      await waitFor(() => {
        const content = screen.getByTestId('stats-content')
        expect(content).toBeInTheDocument()
      }, { timeout: 2000 })
    })

    it('displays API data after loading', async () => {
      const mockFetch = createMockFetch(testStats)

      await act(async () => {
        render(<StatsSection fetchStats={mockFetch} />)
      })

      await waitFor(() => {
        const value = screen.getByTestId('stat-links-shortened-value')
        expect(value.textContent).toBe('2,000,000')
      })
    })
  })

  // Test Case 6: Static placeholder values are displayed gracefully when API fails
  describe('Test Case 6: API failure handling', () => {
    it('displays fallback values when API fails', async () => {
      const mockFetch = createFailingMockFetch()

      await act(async () => {
        render(<StatsSection fetchStats={mockFetch} />)
      })

      await waitFor(() => {
        const linksValue = screen.getByTestId('stat-links-shortened-value')
        expect(linksValue.textContent).toBe(DEFAULT_STATS.linksShortened.toLocaleString())
      })
    })

    it('displays all three stats with fallback values on error', async () => {
      const mockFetch = createFailingMockFetch()

      await act(async () => {
        render(<StatsSection fetchStats={mockFetch} />)
      })

      await waitFor(() => {
        const linksValue = screen.getByTestId('stat-links-shortened-value')
        expect(linksValue.textContent).toBe(DEFAULT_STATS.linksShortened.toLocaleString())

        const clicksValue = screen.getByTestId('stat-clicks-tracked-value')
        expect(clicksValue.textContent).toBe(DEFAULT_STATS.clicksTracked.toLocaleString())

        const usersValue = screen.getByTestId('stat-active-users-value')
        expect(usersValue.textContent).toBe(DEFAULT_STATS.activeUsers.toLocaleString())
      })
    })

    it('shows error note when API fails', async () => {
      const mockFetch = createFailingMockFetch()

      await act(async () => {
        render(<StatsSection fetchStats={mockFetch} />)
      })

      await waitFor(() => {
        const errorNote = screen.getByTestId('stats-error-note')
        expect(errorNote).toBeInTheDocument()
        expect(errorNote.textContent).toContain('estimated')
      })
    })

    it('does not show error note when API succeeds', async () => {
      const mockFetch = createMockFetch(testStats)

      await act(async () => {
        render(<StatsSection fetchStats={mockFetch} />)
      })

      await waitFor(() => {
        const content = screen.getByTestId('stats-content')
        expect(content).toBeInTheDocument()
      })

      expect(screen.queryByTestId('stats-error-note')).not.toBeInTheDocument()
    })
  })

  // Additional tests for styling and layout
  describe('Styling and layout', () => {
    it('renders three statistic cards', async () => {
      const mockFetch = createMockFetch(testStats)

      await act(async () => {
        render(<StatsSection fetchStats={mockFetch} />)
      })

      await waitFor(() => {
        const linksCard = screen.getByTestId('stat-links-shortened')
        const clicksCard = screen.getByTestId('stat-clicks-tracked')
        const usersCard = screen.getByTestId('stat-active-users')

        expect(linksCard).toBeInTheDocument()
        expect(clicksCard).toBeInTheDocument()
        expect(usersCard).toBeInTheDocument()
      })
    })

    it('uses responsive grid layout', async () => {
      const mockFetch = createMockFetch(testStats)

      await act(async () => {
        render(<StatsSection fetchStats={mockFetch} />)
      })

      await waitFor(() => {
        const grid = screen.getByTestId('stats-content')
        expect(grid).toHaveClass('grid')
        expect(grid).toHaveClass('grid-cols-1')
        expect(grid).toHaveClass('md:grid-cols-3')
      })
    })

    it('statistic cards have proper styling classes', async () => {
      const mockFetch = createMockFetch(testStats)

      await act(async () => {
        render(<StatsSection fetchStats={mockFetch} />)
      })

      await waitFor(() => {
        const card = screen.getByTestId('stat-links-shortened')
        expect(card).toHaveClass('card')
        expect(card).toHaveClass('bg-base-200')
        expect(card).toHaveClass('shadow-xl')
      })
    })
  })

  // Test default export and named export
  describe('Component exports', () => {
    it('can be imported as named export', () => {
      expect(StatsSection).toBeDefined()
      expect(typeof StatsSection).toBe('function')
    })
  })
})
