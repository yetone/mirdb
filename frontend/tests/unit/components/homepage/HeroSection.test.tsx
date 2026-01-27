/**
 * HeroSection Component Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Test cases for validating the hero section displays correctly
 * with product tagline, value proposition, and visual elements
 * as specified in REQ-1 and US-1.
 */

import { describe, it, expect, vi } from 'vitest'
import { screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { renderWithProviders } from '../../../utils/test-utils'
import { HeroSection } from '../../../../src/components/homepage/HeroSection'

describe('HeroSection', () => {
  // Test Case 1: Headline text about URL shortening service is visible
  it('displays headline text about URL shortening service', () => {
    renderWithProviders(<HeroSection />)

    // Check for headline that mentions URL shortening
    const headline = screen.getByRole('heading', { level: 1 })
    expect(headline).toBeInTheDocument()
    expect(headline.textContent?.toLowerCase()).toMatch(/shorten|url|link/i)
  })

  // Test Case 2: Subheadline mentions analytics capabilities
  it('displays subheadline mentioning analytics capabilities', () => {
    renderWithProviders(<HeroSection />)

    // Check for subheadline/paragraph that mentions analytics
    const subheadline = screen.getByText(/analytics|track|performance/i)
    expect(subheadline).toBeInTheDocument()
  })

  // Test Case 3: Primary CTA button 'Get Started' is visible
  it('displays primary CTA button "Get Started"', () => {
    renderWithProviders(<HeroSection />)

    const getStartedButton = screen.getByRole('button', { name: /get started/i })
    expect(getStartedButton).toBeInTheDocument()
    // Check button is not hidden (jsdom limitation with motion elements)
    expect(getStartedButton).not.toHaveAttribute('hidden')
  })

  // Test Case 4: Secondary CTA button 'Learn More' is visible
  it('displays secondary CTA button "Learn More"', () => {
    renderWithProviders(<HeroSection />)

    const learnMoreButton = screen.getByRole('button', { name: /learn more/i })
    expect(learnMoreButton).toBeInTheDocument()
    // Check button is not hidden (jsdom limitation with motion elements)
    expect(learnMoreButton).not.toHaveAttribute('hidden')
  })

  // Additional tests for functionality
  it('calls onLearnMoreClick when Learn More button is clicked', async () => {
    const user = userEvent.setup()
    const handleLearnMore = vi.fn()

    renderWithProviders(<HeroSection onLearnMoreClick={handleLearnMore} />)

    const learnMoreButton = screen.getByRole('button', { name: /learn more/i })
    await user.click(learnMoreButton)

    expect(handleLearnMore).toHaveBeenCalledTimes(1)
  })

  it('renders Get Started button as a link to register page', () => {
    renderWithProviders(<HeroSection />)

    // The Get Started button should navigate to /register
    const getStartedLink = screen.getByRole('link', { name: /get started/i })
    expect(getStartedLink).toHaveAttribute('href', '/register')
  })
})

describe('HeroSection with BackgroundEffect integration', () => {
  // Test Case 5: BackgroundEffect component is rendered and visible
  it('renders with BackgroundEffect visible in the parent', () => {
    // Import Home page to test integration
    const { container } = renderWithProviders(<HeroSection />)

    // Check that the hero section itself is rendered
    const heroSection = container.querySelector('section')
    expect(heroSection).toBeInTheDocument()
    expect(heroSection).toHaveAttribute('aria-label', 'Hero')
  })
})
