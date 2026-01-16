import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import SocialProofSection from './SocialProofSection'

vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: { children: React.ReactNode; [key: string]: unknown }) => (
      <div {...props}>{children}</div>
    ),
    section: ({ children, ...props }: { children: React.ReactNode; [key: string]: unknown }) => (
      <section {...props}>{children}</section>
    ),
  },
}))

describe('SocialProofSection', () => {
  describe('Test Case 1: Component renders with statistics and trust indicators', () => {
    it('renders the SocialProofSection component', () => {
      render(<SocialProofSection />)

      const section = screen.getByTestId('social-proof-section')
      expect(section).toBeInTheDocument()
    })

    it('renders with proper section element', () => {
      render(<SocialProofSection />)

      const section = screen.getByRole('region', { name: /social proof|trusted by|our impact/i })
      expect(section).toBeInTheDocument()
    })

    it('has a section heading', () => {
      render(<SocialProofSection />)

      const heading = screen.getByRole('heading', { level: 2 })
      expect(heading).toBeInTheDocument()
    })

    it('renders statistics cards', () => {
      render(<SocialProofSection />)

      const statsCards = screen.getAllByTestId(/stat-card-/)
      expect(statsCards.length).toBeGreaterThanOrEqual(2)
    })

    it('renders trust indicators', () => {
      render(<SocialProofSection />)

      const trustIndicators = screen.getAllByTestId(/trust-indicator-/)
      expect(trustIndicators.length).toBeGreaterThanOrEqual(1)
    })
  })

  describe('Test Case 2: Section displays metrics like X URLs shortened, Y clicks tracked', () => {
    it('displays URLs shortened statistic', () => {
      render(<SocialProofSection />)

      const urlsStat = screen.getByTestId('stat-urls-shortened')
      expect(urlsStat).toBeInTheDocument()
      expect(urlsStat).toHaveTextContent(/URLs shortened/i)
    })

    it('displays clicks tracked statistic', () => {
      render(<SocialProofSection />)

      const clicksStat = screen.getByTestId('stat-clicks-tracked')
      expect(clicksStat).toBeInTheDocument()
      expect(clicksStat).toHaveTextContent(/clicks tracked/i)
    })

    it('displays statistics with numeric values', () => {
      render(<SocialProofSection />)

      const urlsStat = screen.getByTestId('stat-urls-shortened')
      const clicksStat = screen.getByTestId('stat-clicks-tracked')

      // Check that stats contain numeric values (formatted with + or K/M suffix)
      expect(urlsStat).toHaveTextContent(/\d+/)
      expect(clicksStat).toHaveTextContent(/\d+/)
    })

    it('displays statistics values prominently', () => {
      render(<SocialProofSection />)

      const statValues = screen.getAllByTestId(/stat-value-/)
      expect(statValues.length).toBeGreaterThanOrEqual(2)

      statValues.forEach((value) => {
        expect(value).toBeVisible()
      })
    })

    it('displays statistics labels', () => {
      render(<SocialProofSection />)

      const statLabels = screen.getAllByTestId(/stat-label-/)
      expect(statLabels.length).toBeGreaterThanOrEqual(2)
    })
  })

  describe('Section structure and accessibility', () => {
    it('has proper aria-labelledby attribute', () => {
      render(<SocialProofSection />)

      const section = screen.getByTestId('social-proof-section')
      expect(section).toHaveAttribute('aria-labelledby', 'social-proof-heading')
    })

    it('has proper id for scroll navigation', () => {
      render(<SocialProofSection />)

      const section = screen.getByTestId('social-proof-section')
      expect(section).toHaveAttribute('id', 'social-proof')
    })

    it('icons/indicators are properly marked for accessibility', () => {
      render(<SocialProofSection />)

      const trustIndicators = screen.getAllByTestId(/trust-indicator-/)
      trustIndicators.forEach((indicator) => {
        const icon = indicator.querySelector('[aria-hidden="true"]')
        expect(icon).toBeInTheDocument()
      })
    })
  })

  describe('Visual styling and layout', () => {
    it('section has proper padding for visual separation', () => {
      render(<SocialProofSection />)

      const section = screen.getByTestId('social-proof-section')
      expect(section).toHaveClass('py-16')
    })

    it('section has background styling', () => {
      render(<SocialProofSection />)

      const section = screen.getByTestId('social-proof-section')
      expect(section.className).toMatch(/bg-/)
    })

    it('has max-width container for content', () => {
      render(<SocialProofSection />)

      const container = screen.getByTestId('social-proof-section').querySelector('.max-w-6xl, .max-w-7xl')
      expect(container).toBeInTheDocument()
    })

    it('statistics are displayed in a grid or flex layout', () => {
      render(<SocialProofSection />)

      const statsContainer = screen.getByTestId('stats-container')
      expect(statsContainer).toBeInTheDocument()
      const hasLayoutClass = statsContainer.className.includes('grid') || statsContainer.className.includes('flex')
      expect(hasLayoutClass).toBe(true)
    })

    it('trust indicators are displayed in a grid or flex layout', () => {
      render(<SocialProofSection />)

      const trustContainer = screen.getByTestId('trust-indicators-container')
      expect(trustContainer).toBeInTheDocument()
      const hasLayoutClass = trustContainer.className.includes('grid') || trustContainer.className.includes('flex')
      expect(hasLayoutClass).toBe(true)
    })
  })

  describe('Trust indicators content', () => {
    it('displays security-related trust indicator', () => {
      render(<SocialProofSection />)

      const section = screen.getByTestId('social-proof-section')
      expect(section).toHaveTextContent(/secure|security|encrypted|ssl/i)
    })

    it('displays uptime or reliability indicator', () => {
      render(<SocialProofSection />)

      const section = screen.getByTestId('social-proof-section')
      expect(section).toHaveTextContent(/uptime|reliable|availability|\d+%/i)
    })

    it('displays performance indicator', () => {
      render(<SocialProofSection />)

      const section = screen.getByTestId('social-proof-section')
      expect(section).toHaveTextContent(/fast|speed|instant|performance|<\d+/i)
    })
  })
})
