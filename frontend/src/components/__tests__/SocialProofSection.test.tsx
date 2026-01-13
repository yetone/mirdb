import { describe, it, expect } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { ThemeProvider } from '../../contexts/ThemeContext'
import { SocialProofSection } from '../SocialProofSection'
import Home from '../../pages/Home'
import { formatStatNumber } from '../../utils/formatNumber'

// Helper to render Home with ThemeProvider
function renderHome() {
  return render(
    <ThemeProvider>
      <BrowserRouter>
        <Home />
      </BrowserRouter>
    </ThemeProvider>
  )
}

// Test Case 1: Statistics section container is present in the DOM
describe('Test Case 1: Social proof/statistics section presence', () => {
  it('should render the social proof section container on homepage', async () => {
    renderHome()

    await waitFor(() => {
      const socialProofSection = screen.getByTestId('social-proof-section')
      expect(socialProofSection).toBeInTheDocument()
    })
  })

  it('should render the statistics container with stat cards', async () => {
    render(
      <BrowserRouter>
        <SocialProofSection />
      </BrowserRouter>
    )

    await waitFor(() => {
      const statsContainer = screen.getByTestId('statistics-container')
      expect(statsContainer).toBeInTheDocument()
    })
  })

  it('should have proper section id for anchor navigation', async () => {
    render(
      <BrowserRouter>
        <SocialProofSection />
      </BrowserRouter>
    )

    await waitFor(() => {
      const section = screen.getByTestId('social-proof-section')
      expect(section).toHaveAttribute('id', 'social-proof')
    })
  })
})

// Test Case 2: URLs shortened statistic is displayed with a formatted number
describe('Test Case 2: URLs shortened statistic display', () => {
  it('should display URLs shortened stat card', async () => {
    render(
      <BrowserRouter>
        <SocialProofSection />
      </BrowserRouter>
    )

    await waitFor(() => {
      const urlsStatCard = screen.getByTestId('stat-card-urls-shortened')
      expect(urlsStatCard).toBeInTheDocument()
    })
  })

  it('should display URLs shortened label', async () => {
    render(
      <BrowserRouter>
        <SocialProofSection />
      </BrowserRouter>
    )

    await waitFor(() => {
      const label = screen.getByTestId('stat-label-urls-shortened')
      expect(label).toHaveTextContent('URLs Shortened')
    })
  })

  it('should display URLs shortened value with formatted number', async () => {
    render(
      <BrowserRouter>
        <SocialProofSection urlsShortened={1250000} />
      </BrowserRouter>
    )

    await waitFor(() => {
      const value = screen.getByTestId('stat-value-urls-shortened')
      expect(value).toHaveTextContent('1.3M')
    })
  })

  it('should display URLs shortened icon', async () => {
    render(
      <BrowserRouter>
        <SocialProofSection />
      </BrowserRouter>
    )

    await waitFor(() => {
      const icon = screen.getByTestId('stat-icon-urls-shortened')
      expect(icon).toBeInTheDocument()
    })
  })
})

// Test Case 3: Clicks tracked statistic is displayed with a formatted number
describe('Test Case 3: Clicks tracked statistic display', () => {
  it('should display clicks tracked stat card', async () => {
    render(
      <BrowserRouter>
        <SocialProofSection />
      </BrowserRouter>
    )

    await waitFor(() => {
      const clicksStatCard = screen.getByTestId('stat-card-clicks-tracked')
      expect(clicksStatCard).toBeInTheDocument()
    })
  })

  it('should display clicks tracked label', async () => {
    render(
      <BrowserRouter>
        <SocialProofSection />
      </BrowserRouter>
    )

    await waitFor(() => {
      const label = screen.getByTestId('stat-label-clicks-tracked')
      expect(label).toHaveTextContent('Clicks Tracked')
    })
  })

  it('should display clicks tracked value with formatted number', async () => {
    render(
      <BrowserRouter>
        <SocialProofSection clicksTracked={45600000} />
      </BrowserRouter>
    )

    await waitFor(() => {
      const value = screen.getByTestId('stat-value-clicks-tracked')
      expect(value).toHaveTextContent('45.6M')
    })
  })

  it('should display clicks tracked icon', async () => {
    render(
      <BrowserRouter>
        <SocialProofSection />
      </BrowserRouter>
    )

    await waitFor(() => {
      const icon = screen.getByTestId('stat-icon-clicks-tracked')
      expect(icon).toBeInTheDocument()
    })
  })
})

// Test Case 4: Large numbers are formatted with commas or abbreviated
describe('Test Case 4: Number formatting for large numbers', () => {
  it('should format numbers less than 1000 without abbreviation', () => {
    expect(formatStatNumber(999)).toBe('999')
    expect(formatStatNumber(150)).toBe('150')
  })

  it('should format thousands with commas', () => {
    expect(formatStatNumber(1234)).toBe('1,234')
    expect(formatStatNumber(12500)).toBe('12,500')
    expect(formatStatNumber(999999)).toBe('999,999')
  })

  it('should abbreviate millions with M suffix', () => {
    expect(formatStatNumber(1000000)).toBe('1M')
    expect(formatStatNumber(1250000)).toBe('1.3M')
    expect(formatStatNumber(45600000)).toBe('45.6M')
    expect(formatStatNumber(100000000)).toBe('100M')
  })

  it('should abbreviate billions with B suffix', () => {
    expect(formatStatNumber(1000000000)).toBe('1B')
    expect(formatStatNumber(1500000000)).toBe('1.5B')
    expect(formatStatNumber(2300000000)).toBe('2.3B')
  })

  it('should render correctly formatted numbers in component', async () => {
    render(
      <BrowserRouter>
        <SocialProofSection
          urlsShortened={1500000}
          clicksTracked={2500000000}
          activeUsers={50000}
          countriesReached={200}
        />
      </BrowserRouter>
    )

    await waitFor(() => {
      // 1.5M for URLs
      expect(screen.getByTestId('stat-value-urls-shortened')).toHaveTextContent('1.5M')
      // 2.5B for clicks
      expect(screen.getByTestId('stat-value-clicks-tracked')).toHaveTextContent('2.5B')
      // 50,000 with commas for users
      expect(screen.getByTestId('stat-value-active-users')).toHaveTextContent('50,000')
      // 200 without abbreviation for countries
      expect(screen.getByTestId('stat-value-countries-reached')).toHaveTextContent('200')
    })
  })
})

// Additional tests for section structure
describe('SocialProofSection structure and accessibility', () => {
  it('should render section title', async () => {
    render(
      <BrowserRouter>
        <SocialProofSection />
      </BrowserRouter>
    )

    await waitFor(() => {
      expect(screen.getByText('Trusted by Thousands')).toBeInTheDocument()
    })
  })

  it('should render section description', async () => {
    render(
      <BrowserRouter>
        <SocialProofSection />
      </BrowserRouter>
    )

    await waitFor(() => {
      expect(screen.getByText(/Join our growing community/)).toBeInTheDocument()
    })
  })

  it('should render all four statistics', async () => {
    render(
      <BrowserRouter>
        <SocialProofSection />
      </BrowserRouter>
    )

    await waitFor(() => {
      expect(screen.getByTestId('stat-card-urls-shortened')).toBeInTheDocument()
      expect(screen.getByTestId('stat-card-clicks-tracked')).toBeInTheDocument()
      expect(screen.getByTestId('stat-card-active-users')).toBeInTheDocument()
      expect(screen.getByTestId('stat-card-countries-reached')).toBeInTheDocument()
    })
  })

  it('should have icons with aria-hidden for accessibility', async () => {
    render(
      <BrowserRouter>
        <SocialProofSection />
      </BrowserRouter>
    )

    await waitFor(() => {
      const iconContainers = [
        screen.getByTestId('stat-icon-urls-shortened'),
        screen.getByTestId('stat-icon-clicks-tracked'),
        screen.getByTestId('stat-icon-active-users'),
        screen.getByTestId('stat-icon-countries-reached'),
      ]

      iconContainers.forEach(container => {
        const svg = container.querySelector('svg')
        expect(svg).toHaveAttribute('aria-hidden', 'true')
      })
    })
  })
})
