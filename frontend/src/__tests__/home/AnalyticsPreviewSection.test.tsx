/**
 * Analytics Preview Section Tests
 * Owner: Scenario 9 - Analytics Preview Section
 *
 * Test coverage:
 * - Section renders with visualization
 * - Analytics benefits text present
 * - Sample data display
 */
import { describe, it, expect, vi } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithProviders } from './test-utils'
import { AnalyticsPreviewSection } from '../../components/home/AnalyticsPreviewSection'

// Mock ResizeObserver for Recharts
;(globalThis as unknown as { ResizeObserver: unknown }).ResizeObserver = vi
  .fn()
  .mockImplementation(() => ({
    observe: vi.fn(),
    unobserve: vi.fn(),
    disconnect: vi.fn(),
  }))

describe('AnalyticsPreviewSection', () => {
  describe('Test Case 1: Section renders with analytics visualization elements', () => {
    it('should render the section with analytics visualization elements', () => {
      renderWithProviders(<AnalyticsPreviewSection />)

      // Check that the section exists
      const section = screen.getByTestId('analytics-preview-section')
      expect(section).toBeInTheDocument()

      // Check for the main heading
      expect(
        screen.getByRole('heading', { name: /powerful analytics at your fingertips/i })
      ).toBeInTheDocument()

      // Check that analytics chart container exists
      const chart = screen.getByTestId('analytics-chart')
      expect(chart).toBeInTheDocument()

      // Check that statistics values are displayed
      expect(screen.getByTestId('stat-value-0')).toBeInTheDocument()
      expect(screen.getByTestId('stat-value-1')).toBeInTheDocument()
      expect(screen.getByTestId('stat-value-2')).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Visual representation of analytics data is displayed', () => {
    it('should display sample chart or statistics visualization', () => {
      renderWithProviders(<AnalyticsPreviewSection />)

      // Check for the chart element with proper aria label
      const chart = screen.getByTestId('analytics-chart')
      expect(chart).toBeInTheDocument()
      expect(chart).toHaveAttribute('role', 'img')
      expect(chart).toHaveAttribute(
        'aria-label',
        'Sample chart showing weekly click trends'
      )

      // Check for the chart heading
      expect(
        screen.getByRole('heading', { name: /weekly click trends/i })
      ).toBeInTheDocument()

      // Check for statistic values displaying numbers/data
      const totalClicks = screen.getByTestId('stat-value-0')
      expect(totalClicks).toHaveTextContent('1,690')

      const countries = screen.getByTestId('stat-value-1')
      expect(countries).toHaveTextContent('24')

      const devices = screen.getByTestId('stat-value-2')
      expect(devices).toHaveTextContent('3 types')

      // Check for stat icons
      expect(screen.getByTestId('stat-icon-0')).toBeInTheDocument()
      expect(screen.getByTestId('stat-icon-1')).toBeInTheDocument()
      expect(screen.getByTestId('stat-icon-2')).toBeInTheDocument()
    })
  })

  describe('Test Case 3: Section includes text explaining analytics benefits', () => {
    it('should display text explaining analytics benefits', () => {
      renderWithProviders(<AnalyticsPreviewSection />)

      // Check for the main description explaining analytics
      const description = screen.getByTestId('analytics-description')
      expect(description).toBeInTheDocument()
      expect(description).toHaveTextContent(/gain valuable insights/i)
      expect(description).toHaveTextContent(/track clicks/i)

      // Check for the benefits section
      const benefits = screen.getByTestId('analytics-benefits')
      expect(benefits).toBeInTheDocument()

      // Check for "Why Analytics Matter" heading
      expect(
        screen.getByRole('heading', { name: /why analytics matter/i })
      ).toBeInTheDocument()

      // Check benefits text includes key messages
      expect(benefits).toHaveTextContent(/optimize your marketing/i)
      expect(benefits).toHaveTextContent(/data-driven decisions/i)

      // Check for individual stat descriptions
      expect(screen.getByText(/track every click on your links/i)).toBeInTheDocument()
      expect(screen.getByText(/geoip location tracking/i)).toBeInTheDocument()
      expect(screen.getByText(/browser and device analytics/i)).toBeInTheDocument()
    })
  })

  describe('Additional coverage: Accessibility', () => {
    it('should have proper heading hierarchy and section labeling', () => {
      renderWithProviders(<AnalyticsPreviewSection />)

      // Check section has aria-labelledby
      const section = screen.getByRole('region', { name: /powerful analytics/i })
      expect(section).toBeInTheDocument()

      // Check for h2 level heading
      const mainHeading = screen.getByRole('heading', {
        level: 2,
        name: /powerful analytics at your fingertips/i,
      })
      expect(mainHeading).toBeInTheDocument()

      // Check for h3 level subheadings
      const subHeadings = screen.getAllByRole('heading', { level: 3 })
      expect(subHeadings.length).toBeGreaterThanOrEqual(2)
    })
  })

  describe('Additional coverage: Content structure', () => {
    it('should display all expected labels for statistics', () => {
      renderWithProviders(<AnalyticsPreviewSection />)

      // Check for labels
      expect(screen.getByText('Total Clicks')).toBeInTheDocument()
      expect(screen.getByText('Countries')).toBeInTheDocument()
      expect(screen.getByText('Devices')).toBeInTheDocument()
    })

    it('should have sample data note under the chart', () => {
      renderWithProviders(<AnalyticsPreviewSection />)

      expect(
        screen.getByText(/sample data showing typical link performance/i)
      ).toBeInTheDocument()
    })
  })
})
