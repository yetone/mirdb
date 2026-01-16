import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import SocialProofSection from './SocialProofSection'

vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: { children: React.ReactNode; [key: string]: unknown }) => (
      <div {...props}>{children}</div>
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

    it('renders the section heading', () => {
      render(<SocialProofSection />)

      expect(screen.getByRole('heading', { level: 2 })).toHaveTextContent(/trusted by|numbers/i)
    })

    it('renders statistics and trust indicator cards', () => {
      render(<SocialProofSection />)

      const cards = screen.getAllByTestId('glassmorphism-card')
      expect(cards.length).toBeGreaterThanOrEqual(3)
    })
  })

  describe('Test Case 2: Section displays metrics like URLs shortened and clicks tracked', () => {
    it('displays URLs shortened statistic', () => {
      render(<SocialProofSection />)

      const urlsStat = screen.getByTestId('stat-urls-shortened')
      expect(urlsStat).toBeInTheDocument()
      expect(urlsStat).toHaveTextContent(/urls shortened/i)
    })

    it('displays clicks tracked statistic', () => {
      render(<SocialProofSection />)

      const clicksStat = screen.getByTestId('stat-clicks-tracked')
      expect(clicksStat).toBeInTheDocument()
      expect(clicksStat).toHaveTextContent(/clicks tracked/i)
    })

    it('displays numeric values for statistics', () => {
      render(<SocialProofSection />)

      // Check that stat values contain numbers
      const statValue1 = screen.getByTestId('stat-value-1')
      const statValue2 = screen.getByTestId('stat-value-2')

      expect(statValue1.textContent).toMatch(/\d/)
      expect(statValue2.textContent).toMatch(/\d/)
    })

    it('displays statistic labels', () => {
      render(<SocialProofSection />)

      const statLabel1 = screen.getByTestId('stat-label-1')
      const statLabel2 = screen.getByTestId('stat-label-2')

      expect(statLabel1).toBeInTheDocument()
      expect(statLabel2).toBeInTheDocument()
    })
  })

  describe('Test Case 3: Trust indicators are displayed', () => {
    it('displays security trust indicator', () => {
      render(<SocialProofSection />)

      const securityIndicator = screen.getByTestId('trust-indicator-security')
      expect(securityIndicator).toBeInTheDocument()
    })

    it('displays uptime trust indicator', () => {
      render(<SocialProofSection />)

      const uptimeIndicator = screen.getByTestId('trust-indicator-uptime')
      expect(uptimeIndicator).toBeInTheDocument()
    })

    it('displays trust indicator icons', () => {
      render(<SocialProofSection />)

      const trustIcon1 = screen.getByTestId('trust-icon-1')
      const trustIcon2 = screen.getByTestId('trust-icon-2')

      expect(trustIcon1).toBeInTheDocument()
      expect(trustIcon2).toBeInTheDocument()
    })
  })

  describe('Grid layout structure', () => {
    it('renders statistics in a grid container', () => {
      render(<SocialProofSection />)

      const grid = screen.getByTestId('social-proof-grid')
      expect(grid).toBeInTheDocument()
      expect(grid).toHaveClass('grid')
    })

    it('has responsive grid classes for different viewport sizes', () => {
      render(<SocialProofSection />)

      const grid = screen.getByTestId('social-proof-grid')
      expect(grid).toHaveClass('grid-cols-1')
      expect(grid).toHaveClass('sm:grid-cols-2')
      expect(grid).toHaveClass('lg:grid-cols-4')
    })

    it('has proper gap between grid items', () => {
      render(<SocialProofSection />)

      const grid = screen.getByTestId('social-proof-grid')
      expect(grid).toHaveClass('gap-6')
    })
  })

  describe('Section structure and accessibility', () => {
    it('renders with proper section element', () => {
      render(<SocialProofSection />)

      const section = screen.getByRole('region', { name: /trusted|numbers/i })
      expect(section).toBeInTheDocument()
    })

    it('has section heading at h2 level', () => {
      render(<SocialProofSection />)

      const heading = screen.getByRole('heading', { level: 2 })
      expect(heading).toBeInTheDocument()
    })

    it('has proper aria-labelledby attribute', () => {
      render(<SocialProofSection />)

      const section = screen.getByTestId('social-proof-section')
      expect(section).toHaveAttribute('aria-labelledby', 'social-proof-heading')
    })

    it('icons are marked as decorative with aria-hidden', () => {
      render(<SocialProofSection />)

      const iconContainers = [
        screen.getByTestId('trust-icon-1'),
        screen.getByTestId('trust-icon-2'),
      ]

      iconContainers.forEach((container) => {
        expect(container).toHaveAttribute('aria-hidden', 'true')
      })
    })
  })

  describe('Visual styling', () => {
    it('section has proper padding for visual separation', () => {
      render(<SocialProofSection />)

      const section = screen.getByTestId('social-proof-section')
      expect(section).toHaveClass('py-20')
      expect(section).toHaveClass('px-4')
    })

    it('section has background color class', () => {
      render(<SocialProofSection />)

      const section = screen.getByTestId('social-proof-section')
      expect(section).toHaveClass('bg-base-100')
    })

    it('has max-width container for content', () => {
      render(<SocialProofSection />)

      const container = screen.getByTestId('social-proof-section').querySelector('.max-w-7xl')
      expect(container).toBeInTheDocument()
    })
  })
})
