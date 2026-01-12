import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import SocialProofSection, { formatNumber } from './SocialProofSection'

const renderWithRouter = (component: React.ReactElement) => {
  return render(
    <MemoryRouter initialEntries={['/']}>
      {component}
    </MemoryRouter>
  )
}

describe('SocialProofSection - Unit Tests', () => {
  // Test Case 1: Section exists with usage statistics
  it('renders social proof section with usage statistics', () => {
    renderWithRouter(<SocialProofSection />)

    // Verify section exists
    const section = screen.getByTestId('social-proof-section')
    expect(section).toBeInTheDocument()

    // Verify statistics container exists
    const statsContainer = screen.getByTestId('statistics-container')
    expect(statsContainer).toBeInTheDocument()

    // Verify URLs shortened statistic exists
    const urlsStat = screen.getByTestId('statistic-urls-shortened')
    expect(urlsStat).toBeInTheDocument()

    // Verify clicks tracked statistic exists
    const clicksStat = screen.getByTestId('statistic-clicks-tracked')
    expect(clicksStat).toBeInTheDocument()
  })

  it('displays section heading about trust/social proof', () => {
    renderWithRouter(<SocialProofSection />)

    // Verify section has an appropriate heading
    const heading = screen.getByRole('heading', { level: 2 })
    expect(heading).toBeInTheDocument()
    expect(heading.textContent?.toLowerCase()).toMatch(/trust|community|users|proof|join/)
  })

  it('displays descriptive text about the service', () => {
    renderWithRouter(<SocialProofSection />)

    const description = screen.getByTestId('social-proof-description')
    expect(description).toBeInTheDocument()
    expect(description.textContent?.toLowerCase()).toMatch(/join|community|trust|url|shortening/)
  })

  // Test Case 2: Trust indicators are displayed
  it('renders trust indicators section', () => {
    renderWithRouter(<SocialProofSection />)

    // Verify trust indicators container exists
    const trustIndicators = screen.getByTestId('trust-indicators')
    expect(trustIndicators).toBeInTheDocument()

    // Verify security indicator exists
    const securityIndicator = screen.getByTestId('trust-indicator-security')
    expect(securityIndicator).toBeInTheDocument()
    expect(securityIndicator.textContent).toMatch(/security/i)

    // Verify privacy indicator exists
    const privacyIndicator = screen.getByTestId('trust-indicator-privacy')
    expect(privacyIndicator).toBeInTheDocument()
    expect(privacyIndicator.textContent).toMatch(/gdpr|privacy|compliant/i)
  })

  it('displays multiple trust badges', () => {
    renderWithRouter(<SocialProofSection />)

    const trustIndicators = screen.getByTestId('trust-indicators')
    const badges = trustIndicators.querySelectorAll('[data-testid^="trust-indicator-"]')

    // Should have at least 2 trust indicators
    expect(badges.length).toBeGreaterThanOrEqual(2)
  })

  // Test Case 3: Statistics are properly formatted
  it('displays statistics with properly formatted numbers', () => {
    renderWithRouter(<SocialProofSection />)

    // URLs shortened should show formatted value (1.3M+)
    const urlsValue = screen.getByTestId('statistic-value-urls-shortened')
    expect(urlsValue).toBeInTheDocument()
    // Should be formatted like "1.3M+" not "1250000"
    expect(urlsValue.textContent).toMatch(/^\d+\.?\d*[MK]\+$/)

    // Clicks tracked should show formatted value (8.5M+)
    const clicksValue = screen.getByTestId('statistic-value-clicks-tracked')
    expect(clicksValue).toBeInTheDocument()
    expect(clicksValue.textContent).toMatch(/^\d+\.?\d*[MK]\+$/)

    // Active users should show formatted value (25K+)
    const usersValue = screen.getByTestId('statistic-value-active-users')
    expect(usersValue).toBeInTheDocument()
    expect(usersValue.textContent).toMatch(/^\d+\.?\d*[MK]\+$/)
  })

  it('formats uptime percentage correctly', () => {
    renderWithRouter(<SocialProofSection />)

    const uptimeValue = screen.getByTestId('statistic-value-uptime')
    expect(uptimeValue).toBeInTheDocument()
    expect(uptimeValue.textContent).toMatch(/99\.9%/)
  })
})

describe('SocialProofSection - formatNumber utility', () => {
  it('formats millions correctly', () => {
    expect(formatNumber(1000000)).toBe('1M+')
    expect(formatNumber(1250000)).toBe('1.3M+')
    expect(formatNumber(8500000)).toBe('8.5M+')
    expect(formatNumber(10000000)).toBe('10M+')
  })

  it('formats thousands correctly', () => {
    expect(formatNumber(1000)).toBe('1K+')
    expect(formatNumber(25000)).toBe('25K+')
    expect(formatNumber(999000)).toBe('999K+')
  })

  it('returns small numbers as-is', () => {
    expect(formatNumber(0)).toBe('0')
    expect(formatNumber(100)).toBe('100')
    expect(formatNumber(999)).toBe('999')
  })

  it('removes unnecessary decimal places', () => {
    // 1000000 should be "1M+" not "1.0M+"
    expect(formatNumber(1000000)).toBe('1M+')
    expect(formatNumber(2000000)).toBe('2M+')
    expect(formatNumber(5000)).toBe('5K+')
  })
})

describe('SocialProofSection - Visual Elements', () => {
  it('renders all statistic cards with icons', () => {
    renderWithRouter(<SocialProofSection />)

    const statsContainer = screen.getByTestId('statistics-container')
    const cards = statsContainer.querySelectorAll('.card')

    // Should have 4 statistic cards
    expect(cards.length).toBe(4)

    // Each card should have an SVG icon
    cards.forEach((card) => {
      const icon = card.querySelector('svg')
      expect(icon).toBeInTheDocument()
    })
  })

  it('has proper section styling with base theme classes', () => {
    renderWithRouter(<SocialProofSection />)

    const section = screen.getByTestId('social-proof-section')

    // Verify section has styling classes
    expect(section.className).toMatch(/bg-|py-|px-/)
  })

  it('statistics grid is responsive', () => {
    renderWithRouter(<SocialProofSection />)

    const statsContainer = screen.getByTestId('statistics-container')

    // Should have grid layout classes
    expect(statsContainer.className).toMatch(/grid/)
    expect(statsContainer.className).toMatch(/grid-cols-2/)
    expect(statsContainer.className).toMatch(/md:grid-cols-4/)
  })
})

describe('SocialProofSection - Accessibility', () => {
  it('has accessible heading structure', () => {
    renderWithRouter(<SocialProofSection />)

    const heading = screen.getByRole('heading', { level: 2 })
    expect(heading).toBeInTheDocument()
  })

  it('statistics have proper semantic structure', () => {
    renderWithRouter(<SocialProofSection />)

    // Each stat should have both a value and a label
    const urlsStat = screen.getByTestId('statistic-urls-shortened')
    expect(urlsStat.textContent).toMatch(/URLs Shortened/i)

    const clicksStat = screen.getByTestId('statistic-clicks-tracked')
    expect(clicksStat.textContent).toMatch(/Clicks Tracked/i)
  })
})
