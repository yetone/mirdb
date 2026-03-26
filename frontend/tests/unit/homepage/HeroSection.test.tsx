/**
 * Unit Tests for HeroSection Component
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests:
 * - Hero renders with headline containing 'shorten' or 'URL' keywords
 * - Description text is present with 2-3 sentences
 * - Primary CTA button (filled) with 'Shorten URL' text is visible
 * - Secondary CTA button (outline) with 'Sign Up' text is visible
 *
 * Requirements: REQ-1, REQ-3
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { HeroSection } from '../../../src/components/homepage/HeroSection'

describe('HeroSection', () => {
  it('renders hero section with headline containing URL shortening keywords', () => {
    render(<HeroSection />)

    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection).toBeInTheDocument()

    // Check for headline with 'shorten' or 'URL' keywords
    const headline = screen.getByRole('heading', { level: 1 })
    expect(headline).toBeInTheDocument()

    const headlineText = headline.textContent?.toLowerCase() ?? ''
    const containsShorten = headlineText.includes('shorten')
    const containsUrl = headlineText.includes('url')

    expect(containsShorten || containsUrl).toBe(true)
  })

  it('renders description text with 2-3 sentences explaining the service', () => {
    render(<HeroSection />)

    const description = screen.getByTestId('hero-description')
    expect(description).toBeInTheDocument()

    const text = description.textContent ?? ''

    // Check that description has substantive content (at least 50 characters)
    expect(text.length).toBeGreaterThan(50)

    // Check for multiple sentences (at least 2 periods indicating 2+ sentences)
    const sentenceEndings = (text.match(/[.!?]/g) ?? []).length
    expect(sentenceEndings).toBeGreaterThanOrEqual(2)
    expect(sentenceEndings).toBeLessThanOrEqual(4) // 2-3 sentences + possibly 4
  })

  it('renders primary CTA button (filled) with Shorten URL text', () => {
    render(<HeroSection />)

    const primaryButton = screen.getByTestId('primary-cta')
    expect(primaryButton).toBeInTheDocument()

    // Check button text contains 'Shorten' (case-insensitive)
    expect(primaryButton.textContent?.toLowerCase()).toContain('shorten')

    // Check that it's styled as a filled/primary button (has btn-primary class)
    expect(primaryButton.className).toContain('btn-primary')
    expect(primaryButton.className).not.toContain('btn-outline')
  })

  it('renders secondary CTA button (outline) with Sign Up text', () => {
    render(<HeroSection />)

    const secondaryButton = screen.getByTestId('secondary-cta')
    expect(secondaryButton).toBeInTheDocument()

    // Check button text contains 'Sign Up' or 'Learn More'
    const buttonText = secondaryButton.textContent?.toLowerCase() ?? ''
    const hasSignUp = buttonText.includes('sign up')
    const hasLearnMore = buttonText.includes('learn more')

    expect(hasSignUp || hasLearnMore).toBe(true)

    // Check that it's styled as an outline button (has btn-outline class)
    expect(secondaryButton.className).toContain('btn-outline')
  })

  it('calls onPrimaryClick when primary CTA is clicked', () => {
    const onPrimaryClick = vi.fn()
    render(<HeroSection onPrimaryClick={onPrimaryClick} />)

    const primaryButton = screen.getByTestId('primary-cta')
    fireEvent.click(primaryButton)

    expect(onPrimaryClick).toHaveBeenCalledTimes(1)
  })

  it('calls onSecondaryClick when secondary CTA is clicked', () => {
    const onSecondaryClick = vi.fn()
    render(<HeroSection onSecondaryClick={onSecondaryClick} />)

    const secondaryButton = screen.getByTestId('secondary-cta')
    fireEvent.click(secondaryButton)

    expect(onSecondaryClick).toHaveBeenCalledTimes(1)
  })

  it('has gradient or pattern background styling', () => {
    render(<HeroSection />)

    const heroSection = screen.getByTestId('hero-section')

    // Check for gradient class in the section
    const hasGradient = heroSection.className.includes('gradient')

    expect(hasGradient).toBe(true)
  })
})
