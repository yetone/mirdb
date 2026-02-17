/**
 * Unit tests for HeroSection component.
 * Owner: Scenario 1 - Hero Section Display
 *
 * Test cases:
 * 1. Homepage renders with hero section containing clear headline describing URL shortening service
 * 2. Headline is visible within 1 second of page load, sub-headline describes service benefits
 * 3. Primary CTA button (Get Started/Sign Up) is prominently displayed and identifiable
 * 4. Secondary login CTA is visible for existing users
 */

import React from 'react'
import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { HeroSection } from '@/components/home/HeroSection'

// Wrapper component for Router context
const TestWrapper = ({ children }: { children: React.ReactNode }) => (
  <BrowserRouter>{children}</BrowserRouter>
)

describe('HeroSection', () => {
  /**
   * Test Case 1: Hero section renders with headline describing URL shortening service
   */
  it('renders hero section with headline about URL shortening', () => {
    render(
      <TestWrapper>
        <HeroSection />
      </TestWrapper>
    )

    // Hero section should be present
    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection).toBeInTheDocument()

    // Headline should be present and contain URL shortening related content
    const headline = screen.getByTestId('hero-headline')
    expect(headline).toBeInTheDocument()
    expect(headline.tagName.toLowerCase()).toBe('h1')

    // Headline text should reference link shortening/URL service
    const headlineText = headline.textContent?.toLowerCase() || ''
    expect(
      headlineText.includes('shorten') ||
        headlineText.includes('url') ||
        headlineText.includes('link')
    ).toBe(true)
  })

  /**
   * Test Case 2: Headline visible immediately, sub-headline describes benefits
   */
  it('displays headline and sub-headline with service benefits', () => {
    render(
      <TestWrapper>
        <HeroSection />
      </TestWrapper>
    )

    // Headline should be visible immediately (no loading state)
    const headline = screen.getByTestId('hero-headline')
    expect(headline).toBeVisible()

    // Sub-headline should be present and describe benefits
    const subheadline = screen.getByTestId('hero-subheadline')
    expect(subheadline).toBeInTheDocument()
    expect(subheadline).toBeVisible()

    // Sub-headline should mention benefits like track, analytics, or share
    const subText = subheadline.textContent?.toLowerCase() || ''
    expect(
      subText.includes('track') ||
        subText.includes('analytic') ||
        subText.includes('share') ||
        subText.includes('free') ||
        subText.includes('short')
    ).toBe(true)
  })

  /**
   * Test Case 3: Primary CTA button is prominently displayed and identifiable
   */
  it('displays primary CTA button prominently', () => {
    const onRegisterClick = vi.fn()

    render(
      <TestWrapper>
        <HeroSection onRegisterClick={onRegisterClick} />
      </TestWrapper>
    )

    // Primary CTA should be present
    const primaryCta = screen.getByTestId('hero-primary-cta')
    expect(primaryCta).toBeInTheDocument()
    expect(primaryCta).toBeVisible()

    // Should be identifiable as a button
    expect(primaryCta.tagName.toLowerCase()).toBe('button')

    // Should have "Get Started" or similar text
    const ctaText = primaryCta.textContent?.toLowerCase() || ''
    expect(
      ctaText.includes('get started') ||
        ctaText.includes('sign up') ||
        ctaText.includes('start')
    ).toBe(true)

    // Should be clickable and call the register handler
    fireEvent.click(primaryCta)
    expect(onRegisterClick).toHaveBeenCalledTimes(1)
  })

  /**
   * Test Case 4: Secondary login CTA is visible for existing users
   */
  it('displays secondary login CTA for existing users', () => {
    const onLoginClick = vi.fn()

    render(
      <TestWrapper>
        <HeroSection onLoginClick={onLoginClick} />
      </TestWrapper>
    )

    // Secondary CTA should be present
    const secondaryCta = screen.getByTestId('hero-secondary-cta')
    expect(secondaryCta).toBeInTheDocument()
    expect(secondaryCta).toBeVisible()

    // Should mention login for existing users
    const ctaText = secondaryCta.textContent?.toLowerCase() || ''
    expect(
      ctaText.includes('log in') ||
        ctaText.includes('login') ||
        ctaText.includes('sign in') ||
        ctaText.includes('account')
    ).toBe(true)

    // Should be clickable and call the login handler
    fireEvent.click(secondaryCta)
    expect(onLoginClick).toHaveBeenCalledTimes(1)
  })

  /**
   * Additional test: CTAs are distinguishable (primary vs secondary styling)
   */
  it('distinguishes primary and secondary CTAs visually', () => {
    render(
      <TestWrapper>
        <HeroSection />
      </TestWrapper>
    )

    const primaryCta = screen.getByTestId('hero-primary-cta')
    const secondaryCta = screen.getByTestId('hero-secondary-cta')

    // Primary CTA should have primary button styling
    expect(primaryCta.className).toContain('btn-primary')

    // Secondary CTA should have ghost/outline styling (not primary)
    expect(secondaryCta.className).toContain('btn-ghost')
    expect(secondaryCta.className).not.toContain('btn-primary')
  })

  /**
   * Additional test: Accessibility - proper heading structure
   */
  it('has proper accessibility attributes', () => {
    render(
      <TestWrapper>
        <HeroSection />
      </TestWrapper>
    )

    // Hero section should have proper labeling
    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-headline')

    // Headline should be an h1
    const headline = screen.getByTestId('hero-headline')
    expect(headline.tagName.toLowerCase()).toBe('h1')
    expect(headline).toHaveAttribute('id', 'hero-headline')

    // Primary CTA should have aria-label
    const primaryCta = screen.getByTestId('hero-primary-cta')
    expect(primaryCta).toHaveAttribute('aria-label')
  })
})
