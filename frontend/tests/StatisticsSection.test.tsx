import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import StatisticsSection, {
  defaultStatistics,
  StatisticsSectionProps,
  Statistic,
} from '../src/components/StatisticsSection'
import { HiLink, HiCursorClick, HiUsers } from 'react-icons/hi'

describe('StatisticsSection', () => {
  // Test Case 1: Social proof/statistics section is visible (or gracefully absent if not implemented)
  describe('Test Case 1: Statistics section visibility', () => {
    it('renders the statistics section when visible prop is true (default)', () => {
      render(<StatisticsSection />)

      const section = screen.getByTestId('statistics-section')
      expect(section).toBeInTheDocument()
    })

    it('renders the statistics section with default data', () => {
      render(<StatisticsSection />)

      // The section is labeled by the heading which defaults to "Trusted by Thousands"
      const section = screen.getByRole('region', { name: /trusted by thousands/i })
      expect(section).toBeInTheDocument()

      // Should have statistics grid
      const grid = screen.getByTestId('statistics-grid')
      expect(grid).toBeInTheDocument()
    })

    it('gracefully hides the section when visible prop is false', () => {
      render(<StatisticsSection visible={false} />)

      const section = screen.queryByTestId('statistics-section')
      expect(section).not.toBeInTheDocument()
    })

    it('gracefully hides the section when statistics array is empty', () => {
      render(<StatisticsSection statistics={[]} />)

      const section = screen.queryByTestId('statistics-section')
      expect(section).not.toBeInTheDocument()
    })

    it('renders section title and subtitle', () => {
      render(<StatisticsSection />)

      const title = screen.getByTestId('statistics-title')
      expect(title).toHaveTextContent('Trusted by Thousands')

      const subtitle = screen.getByTestId('statistics-subtitle')
      expect(subtitle).toHaveTextContent('Join our growing community')
    })
  })

  // Test Case 2: Verify 'URLs shortened' statistic display
  describe('Test Case 2: URLs shortened statistic display', () => {
    it('displays URLs shortened statistic with formatted number', () => {
      render(<StatisticsSection />)

      const card = screen.getByTestId('statistic-card-urls-shortened')
      expect(card).toBeInTheDocument()

      const label = screen.getByTestId('statistic-label-urls-shortened')
      expect(label).toHaveTextContent('URLs Shortened')

      const value = screen.getByTestId('statistic-value-urls-shortened')
      expect(value).toHaveTextContent('1.3M') // 1250000 formatted (rounds to 1.3M with 1 decimal)
    })

    it('displays URLs shortened statistic with icon', () => {
      render(<StatisticsSection />)

      const icon = screen.getByTestId('statistic-icon-urls-shortened')
      expect(icon).toBeInTheDocument()
      expect(icon.querySelector('svg')).toBeInTheDocument()
    })

    it('URLs shortened value has accessible aria-label', () => {
      render(<StatisticsSection />)

      const value = screen.getByTestId('statistic-value-urls-shortened')
      expect(value).toHaveAttribute('aria-label', expect.stringContaining('URLs Shortened'))
    })
  })

  // Test Case 3: Verify 'clicks tracked' statistic display
  describe('Test Case 3: Clicks tracked statistic display', () => {
    it('displays clicks tracked statistic with formatted number', () => {
      render(<StatisticsSection />)

      const card = screen.getByTestId('statistic-card-clicks-tracked')
      expect(card).toBeInTheDocument()

      const label = screen.getByTestId('statistic-label-clicks-tracked')
      expect(label).toHaveTextContent('Clicks Tracked')

      const value = screen.getByTestId('statistic-value-clicks-tracked')
      expect(value).toHaveTextContent('45.7M') // 45700000 formatted
    })

    it('displays clicks tracked statistic with icon', () => {
      render(<StatisticsSection />)

      const icon = screen.getByTestId('statistic-icon-clicks-tracked')
      expect(icon).toBeInTheDocument()
      expect(icon.querySelector('svg')).toBeInTheDocument()
    })

    it('clicks tracked value has accessible aria-label', () => {
      render(<StatisticsSection />)

      const value = screen.getByTestId('statistic-value-clicks-tracked')
      expect(value).toHaveAttribute('aria-label', expect.stringContaining('Clicks Tracked'))
    })
  })

  // Test Case 4: Large numbers are formatted for readability
  describe('Test Case 4: Large number formatting', () => {
    it('formats millions with M suffix (e.g., 1.2M)', () => {
      const customStats: Statistic[] = [
        {
          id: 'test-millions',
          label: 'Test Millions',
          value: 1200000,
          icon: <HiLink className="w-8 h-8" aria-hidden="true" />,
        },
      ]

      render(<StatisticsSection statistics={customStats} />)

      const value = screen.getByTestId('statistic-value-test-millions')
      expect(value).toHaveTextContent('1.2M')
    })

    it('formats thousands with K suffix (e.g., 500K)', () => {
      const customStats: Statistic[] = [
        {
          id: 'test-thousands',
          label: 'Test Thousands',
          value: 500000,
          icon: <HiUsers className="w-8 h-8" aria-hidden="true" />,
        },
      ]

      render(<StatisticsSection statistics={customStats} />)

      const value = screen.getByTestId('statistic-value-test-thousands')
      expect(value).toHaveTextContent('500K')
    })

    it('formats billions with B suffix', () => {
      const customStats: Statistic[] = [
        {
          id: 'test-billions',
          label: 'Test Billions',
          value: 5500000000,
          icon: <HiCursorClick className="w-8 h-8" aria-hidden="true" />,
        },
      ]

      render(<StatisticsSection statistics={customStats} />)

      const value = screen.getByTestId('statistic-value-test-billions')
      expect(value).toHaveTextContent('5.5B')
    })

    it('displays small numbers without suffix', () => {
      const customStats: Statistic[] = [
        {
          id: 'test-small',
          label: 'Test Small',
          value: 999,
          icon: <HiLink className="w-8 h-8" aria-hidden="true" />,
        },
      ]

      render(<StatisticsSection statistics={customStats} />)

      const value = screen.getByTestId('statistic-value-test-small')
      expect(value).toHaveTextContent('999')
    })

    it('displays zero correctly', () => {
      const customStats: Statistic[] = [
        {
          id: 'test-zero',
          label: 'Test Zero',
          value: 0,
          icon: <HiLink className="w-8 h-8" aria-hidden="true" />,
        },
      ]

      render(<StatisticsSection statistics={customStats} />)

      const value = screen.getByTestId('statistic-value-test-zero')
      expect(value).toHaveTextContent('0')
    })
  })

  // Additional tests for customization and accessibility
  describe('Customization and Accessibility', () => {
    it('accepts custom title and subtitle', () => {
      render(
        <StatisticsSection
          title="Custom Title"
          subtitle="Custom subtitle text"
        />
      )

      const title = screen.getByTestId('statistics-title')
      expect(title).toHaveTextContent('Custom Title')

      const subtitle = screen.getByTestId('statistics-subtitle')
      expect(subtitle).toHaveTextContent('Custom subtitle text')
    })

    it('accepts custom statistics data', () => {
      const customStats: Statistic[] = [
        {
          id: 'custom-stat',
          label: 'Custom Metric',
          value: 42,
          icon: <HiLink className="w-8 h-8" aria-hidden="true" />,
        },
      ]

      render(<StatisticsSection statistics={customStats} />)

      const card = screen.getByTestId('statistic-card-custom-stat')
      expect(card).toBeInTheDocument()

      const label = screen.getByTestId('statistic-label-custom-stat')
      expect(label).toHaveTextContent('Custom Metric')
    })

    it('has proper heading structure for accessibility', () => {
      render(<StatisticsSection />)

      const heading = screen.getByRole('heading', { name: /trusted by thousands/i })
      expect(heading).toBeInTheDocument()
      expect(heading.tagName).toBe('H2')
    })

    it('section has proper aria-labelledby', () => {
      render(<StatisticsSection />)

      const section = screen.getByRole('region')
      expect(section).toHaveAttribute('aria-labelledby', 'statistics-heading')
    })

    it('statistics grid has proper list role and aria-label', () => {
      render(<StatisticsSection />)

      const grid = screen.getByRole('list', { name: /platform statistics/i })
      expect(grid).toBeInTheDocument()
    })

    it('each statistic card has listitem role', () => {
      render(<StatisticsSection />)

      const listItems = screen.getAllByRole('listitem')
      expect(listItems).toHaveLength(defaultStatistics.length)
    })

    it('icons have aria-hidden attribute', () => {
      render(<StatisticsSection />)

      defaultStatistics.forEach((stat) => {
        const icon = screen.getByTestId(`statistic-icon-${stat.id}`)
        const svg = icon.querySelector('svg')
        expect(svg).toHaveAttribute('aria-hidden', 'true')
      })
    })

    it('has correct section id for anchor navigation', () => {
      render(<StatisticsSection />)

      const section = screen.getByTestId('statistics-section')
      expect(section).toHaveAttribute('id', 'statistics')
    })
  })

  // Test rendering all default statistics
  describe('Default Statistics Rendering', () => {
    it('renders all 3 default statistics', () => {
      render(<StatisticsSection />)

      const cards = screen.getAllByTestId(/^statistic-card-/)
      expect(cards).toHaveLength(3)
    })

    it('renders URLs shortened, Clicks tracked, and Active users', () => {
      render(<StatisticsSection />)

      expect(screen.getByTestId('statistic-card-urls-shortened')).toBeInTheDocument()
      expect(screen.getByTestId('statistic-card-clicks-tracked')).toBeInTheDocument()
      expect(screen.getByTestId('statistic-card-active-users')).toBeInTheDocument()
    })

    it('displays Active Users statistic correctly', () => {
      render(<StatisticsSection />)

      const label = screen.getByTestId('statistic-label-active-users')
      expect(label).toHaveTextContent('Active Users')

      const value = screen.getByTestId('statistic-value-active-users')
      expect(value).toHaveTextContent('52K') // 52000 formatted
    })
  })
})
