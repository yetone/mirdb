/**
 * SocialProofSection Unit Tests
 * Owner: Scenario 7 - Social Proof Section
 *
 * Tests for the SocialProofSection component covering all test cases:
 * - TC1: Section renders without errors
 * - TC2: Section displays usage statistics placeholder
 * - TC3: Section has placeholder for customer testimonials or logos
 */
import { render, screen } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { SocialProofSection } from '@/components/homepage/SocialProofSection'

describe('SocialProofSection', () => {
  it('renders the social proof section without errors (TC1)', () => {
    render(<SocialProofSection />)
    expect(screen.getByTestId('social-proof-section')).toBeInTheDocument()
  })

  it('displays usage statistics placeholder (TC2)', () => {
    render(<SocialProofSection />)

    // Check for usage statistics like "10,000+ links shortened"
    const statisticsSection = screen.getByTestId('statistics-section')
    expect(statisticsSection).toBeInTheDocument()

    // Check for specific statistic content
    const linksShortened = screen.getByText(/10,000\+/i)
    expect(linksShortened).toBeInTheDocument()

    const linksLabel = screen.getByText(/links shortened/i)
    expect(linksLabel).toBeInTheDocument()
  })

  it('has placeholder for customer testimonials or logos (TC3)', () => {
    render(<SocialProofSection />)

    // Check for testimonials or partner logos placeholder
    const testimonialsSection = screen.getByTestId('testimonials-section')
    expect(testimonialsSection).toBeInTheDocument()

    // Should have at least one testimonial placeholder
    const testimonialCards = screen.getAllByTestId('testimonial-card')
    expect(testimonialCards.length).toBeGreaterThan(0)
  })

  it('renders section heading', () => {
    render(<SocialProofSection />)

    // Should have a heading indicating social proof
    const heading = screen.getByRole('heading', { level: 2 })
    expect(heading).toBeInTheDocument()
  })

  it('displays multiple statistics', () => {
    render(<SocialProofSection />)

    const statisticItems = screen.getAllByTestId('statistic-item')
    expect(statisticItems.length).toBeGreaterThanOrEqual(2)
  })

  it('has section id for anchor navigation', () => {
    render(<SocialProofSection />)
    const section = screen.getByTestId('social-proof-section')
    expect(section).toHaveAttribute('id', 'social-proof')
  })

  it('displays testimonial quotes and authors', () => {
    render(<SocialProofSection />)

    // Check for testimonial content
    const testimonialQuotes = screen.getAllByTestId('testimonial-quote')
    expect(testimonialQuotes.length).toBeGreaterThan(0)

    const testimonialAuthors = screen.getAllByTestId('testimonial-author')
    expect(testimonialAuthors.length).toBeGreaterThan(0)
  })
})
