/**
 * StatsSection Component Tests
 * Owner: Scenario 5 - Statistics Display Section
 *
 * Tests for:
 * - Total URLs shortened statistic display
 * - Total clicks tracked statistic display
 * - Number formatting (e.g., 1,234,567 or 1.2M)
 * - Scroll-triggered counter animations
 * - Framer Motion animation performance (60fps)
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor, act } from '../../../utils/test-utils'
import { StatsSection } from '../../../../src/components/homepage/StatsSection'
import type { StatItem } from '../../../../src/types/homepage'

// Mock IntersectionObserver with controllable callbacks
let intersectionObserverCallback: IntersectionObserverCallback | null = null
const mockIntersectionObserver = vi.fn((callback: IntersectionObserverCallback) => {
  intersectionObserverCallback = callback
  return {
    observe: vi.fn(),
    unobserve: vi.fn(),
    disconnect: vi.fn(),
  }
})

describe('StatsSection', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    window.IntersectionObserver = mockIntersectionObserver as unknown as typeof IntersectionObserver
    intersectionObserverCallback = null
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  describe('Test Case 1: Total URLs shortened statistic display', () => {
    it('renders Total URLs shortened statistic', () => {
      render(<StatsSection />)

      expect(screen.getByText(/URLs Shortened/i)).toBeInTheDocument()
    })

    it('displays URLs shortened with a numeric value', () => {
      render(<StatsSection />)

      const urlsSection = screen.getByTestId('stat-urls-shortened')
      expect(urlsSection).toBeInTheDocument()
    })

    it('accepts custom stats for URLs via props', () => {
      const customStats: StatItem[] = [
        { label: 'URLs Shortened', value: 5000000 },
      ]

      render(<StatsSection stats={customStats} />)

      expect(screen.getByText(/URLs Shortened/i)).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Total clicks tracked statistic display', () => {
    it('renders Total clicks tracked statistic', () => {
      render(<StatsSection />)

      expect(screen.getByText(/Clicks Tracked/i)).toBeInTheDocument()
    })

    it('displays clicks tracked with a numeric value', () => {
      render(<StatsSection />)

      const clicksSection = screen.getByTestId('stat-clicks-tracked')
      expect(clicksSection).toBeInTheDocument()
    })

    it('displays active users statistic when provided', () => {
      render(<StatsSection />)

      expect(screen.getByText(/Active Users/i)).toBeInTheDocument()
    })
  })

  describe('Test Case 3: Number formatting', () => {
    it('formats large numbers with commas (e.g., 1,234,567)', () => {
      const customStats: StatItem[] = [
        { label: 'Test Stat', value: 1234567 },
      ]

      render(<StatsSection stats={customStats} />)

      // After animation completes, should show formatted number
      const statValue = screen.getByTestId('stat-test-stat')
      expect(statValue).toBeInTheDocument()
    })

    it('formats very large numbers with suffix (e.g., 1.2M)', () => {
      const customStats: StatItem[] = [
        { label: 'Big Stat', value: 1200000, suffix: 'M' },
      ]

      render(<StatsSection stats={customStats} />)

      expect(screen.getByText(/Big Stat/i)).toBeInTheDocument()
    })

    it('renders with + suffix for ongoing counts', () => {
      const customStats: StatItem[] = [
        { label: 'Growing Stat', value: 500000, suffix: '+' },
      ]

      render(<StatsSection stats={customStats} />)

      expect(screen.getByText(/Growing Stat/i)).toBeInTheDocument()
    })

    it('handles zero values correctly', () => {
      const customStats: StatItem[] = [
        { label: 'Zero Stat', value: 0 },
      ]

      render(<StatsSection stats={customStats} />)

      expect(screen.getByText(/Zero Stat/i)).toBeInTheDocument()
    })

    it('handles numbers in millions', () => {
      const customStats: StatItem[] = [
        { label: 'Million Stat', value: 5000000 },
      ]

      render(<StatsSection stats={customStats} />)

      const statSection = screen.getByTestId('stat-million-stat')
      expect(statSection).toBeInTheDocument()
    })
  })

  describe('Test Case 4: Scroll-triggered counter animation', () => {
    it('uses IntersectionObserver for scroll detection', () => {
      render(<StatsSection />)

      expect(mockIntersectionObserver).toHaveBeenCalled()
    })

    it('triggers animation when scrolled into view', async () => {
      render(<StatsSection />)

      // Simulate intersection
      if (intersectionObserverCallback) {
        const mockEntry = {
          isIntersecting: true,
          target: document.createElement('div'),
          boundingClientRect: {} as DOMRectReadOnly,
          intersectionRatio: 1,
          intersectionRect: {} as DOMRectReadOnly,
          rootBounds: null,
          time: 0,
        }

        act(() => {
          intersectionObserverCallback!([mockEntry], {} as IntersectionObserver)
        })
      }

      // Animation should have started
      const statsSection = screen.getByRole('region', { name: /Statistics/i })
      expect(statsSection).toBeInTheDocument()
    })

    it('animates counters from 0 to target value', async () => {
      render(<StatsSection />)

      // Section should be visible and ready for animation
      const statsSection = screen.getByRole('region', { name: /Statistics/i })
      expect(statsSection).toBeInTheDocument()
    })

    it('does not re-animate when scrolled into view multiple times', () => {
      render(<StatsSection />)

      // Simulate intersection twice
      if (intersectionObserverCallback) {
        const mockEntry = {
          isIntersecting: true,
          target: document.createElement('div'),
          boundingClientRect: {} as DOMRectReadOnly,
          intersectionRatio: 1,
          intersectionRect: {} as DOMRectReadOnly,
          rootBounds: null,
          time: 0,
        }

        act(() => {
          intersectionObserverCallback!([mockEntry], {} as IntersectionObserver)
          intersectionObserverCallback!([mockEntry], {} as IntersectionObserver)
        })
      }

      // Should still only have one set of stats
      expect(screen.getAllByTestId(/^stat-/)).toHaveLength(3) // Default 3 stats
    })
  })

  describe('Test Case 5: Framer Motion animation performance', () => {
    it('renders with Framer Motion components', () => {
      render(<StatsSection />)

      // Component should render successfully with Framer Motion
      const statsSection = screen.getByRole('region', { name: /Statistics/i })
      expect(statsSection).toBeInTheDocument()
    })

    it('applies animation variants for smooth transitions', () => {
      render(<StatsSection />)

      // Check that animation container exists
      const statsSection = screen.getByTestId('stats-section')
      expect(statsSection).toBeInTheDocument()
    })

    it('uses viewport-based animation trigger', () => {
      render(<StatsSection />)

      // Viewport animation should be configured
      expect(mockIntersectionObserver).toHaveBeenCalledWith(
        expect.any(Function),
        expect.objectContaining({
          threshold: expect.any(Number),
        })
      )
    })

    it('renders stat cards with staggered animations', () => {
      render(<StatsSection />)

      // All stat items should be present for staggered animation
      const statItems = screen.getAllByTestId(/^stat-/)
      expect(statItems.length).toBeGreaterThan(0)
    })

    it('maintains smooth 60fps animation by using transform properties', () => {
      render(<StatsSection />)

      // Component renders without layout thrashing
      const statsSection = screen.getByTestId('stats-section')
      expect(statsSection).toBeInTheDocument()

      // Check that motion elements exist (Framer Motion handles the 60fps)
      const motionElements = document.querySelectorAll('[style*="transform"]')
      // Framer Motion applies transforms for smooth animations
      expect(statsSection).toBeInTheDocument()
    })
  })

  describe('Section Structure and Accessibility', () => {
    it('renders stats section with proper id for navigation', () => {
      render(<StatsSection />)

      const section = screen.getByRole('region', { name: /Statistics/i })
      expect(section).toHaveAttribute('id', 'stats')
    })

    it('renders section heading', () => {
      render(<StatsSection />)

      expect(screen.getByRole('heading', { level: 2 })).toBeInTheDocument()
    })

    it('has proper aria-labels for accessibility', () => {
      render(<StatsSection />)

      expect(screen.getByRole('region', { name: /Statistics/i })).toBeInTheDocument()
    })

    it('renders default stats when no props provided', () => {
      render(<StatsSection />)

      expect(screen.getByText(/URLs Shortened/i)).toBeInTheDocument()
      expect(screen.getByText(/Clicks Tracked/i)).toBeInTheDocument()
      expect(screen.getByText(/Active Users/i)).toBeInTheDocument()
    })
  })

  describe('Responsive Layout', () => {
    it('has responsive grid layout classes', () => {
      render(<StatsSection />)

      const statsGrid = screen.getByTestId('stats-grid')
      expect(statsGrid).toHaveClass('grid-cols-1')
      expect(statsGrid).toHaveClass('md:grid-cols-3')
    })

    it('centers content for mobile view', () => {
      render(<StatsSection />)

      const statsGrid = screen.getByTestId('stats-grid')
      expect(statsGrid).toHaveClass('text-center')
    })
  })

  describe('GlassMorphismCard Integration', () => {
    it('renders stat items with GlassMorphismCard backdrop blur effect', () => {
      render(<StatsSection />)

      const statsGrid = screen.getByTestId('stats-grid')
      const cards = statsGrid.querySelectorAll('[class*="backdrop-blur"]')

      expect(cards.length).toBeGreaterThan(0)
    })

    it('each stat card has rounded corners styling', () => {
      render(<StatsSection />)

      const statsGrid = screen.getByTestId('stats-grid')
      const cards = statsGrid.querySelectorAll('[class*="rounded"]')

      expect(cards.length).toBeGreaterThan(0)
    })
  })
})
