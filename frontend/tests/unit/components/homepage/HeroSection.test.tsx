/**
 * Unit tests for HeroSection component.
 * Owner: Scenario 1 - Hero Section Rendering and CTA
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '../../../setup'
import userEvent from '@testing-library/user-event'
import { HeroSection } from '@/components/homepage/HeroSection'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import { ThemeProvider } from '@/contexts/ThemeContext'
import React from 'react'

describe('HeroSection', () => {
  // Test Case 1: Hero section renders with H1 headline containing value proposition text
  it('renders H1 headline with value proposition text', () => {
    render(<HeroSection />)

    const headline = screen.getByRole('heading', { level: 1 })
    expect(headline).toBeInTheDocument()
    expect(headline.textContent).toContain('Shorten URLs')
  })

  // Test Case 2: Hero section contains H2 subheading elaborating on benefits
  it('renders H2 subheading elaborating on benefits', () => {
    render(<HeroSection />)

    const subheading = screen.getByRole('heading', { level: 2 })
    expect(subheading).toBeInTheDocument()
    expect(subheading.textContent).toBeTruthy()
    expect(subheading.textContent?.length).toBeGreaterThan(20)
  })

  // Test Case 3: Primary CTA button is visible with text 'Get Started' or 'Start Shortening'
  it('renders primary CTA button with correct text', () => {
    render(<HeroSection />)

    const primaryCta = screen.getByTestId('primary-cta')
    expect(primaryCta).toBeInTheDocument()
    expect(primaryCta).toBeVisible()

    const ctaText = primaryCta.textContent
    expect(
      ctaText?.includes('Get Started') || ctaText?.includes('Start Shortening')
    ).toBe(true)
  })

  // Test Case 4: Primary CTA button uses brand accent color from DaisyUI theme
  it('primary CTA button uses btn-primary class for accent color', () => {
    render(<HeroSection />)

    const primaryCta = screen.getByTestId('primary-cta')
    expect(primaryCta).toHaveClass('btn-primary')
  })

  // Test Case 5: Click primary CTA button - User is navigated to /register or /dashboard route
  it('navigates to /register when primary CTA is clicked', async () => {
    const user = userEvent.setup()

    const TestComponent = () => {
      return React.createElement(
        MemoryRouter,
        { initialEntries: ['/'] },
        React.createElement(ThemeProvider, null,
          React.createElement(Routes, null,
            React.createElement(Route, { path: '/', element: React.createElement(HeroSection) }),
            React.createElement(Route, {
              path: '/register',
              element: React.createElement('div', { 'data-testid': 'register-page' }, 'Register Page')
            })
          )
        )
      )
    }

    render(React.createElement(TestComponent), { wrapper: ({ children }) => children as React.ReactElement })

    const primaryCta = screen.getByTestId('primary-cta')
    expect(primaryCta).toHaveAttribute('href', '/register')

    await user.click(primaryCta)

    expect(screen.getByTestId('register-page')).toBeInTheDocument()
  })

  // Test Case 6: Secondary CTA link is visible for alternate action
  it('renders secondary CTA link for alternate action', () => {
    render(<HeroSection />)

    const secondaryCta = screen.getByTestId('secondary-cta')
    expect(secondaryCta).toBeInTheDocument()
    expect(secondaryCta).toBeVisible()

    const ctaText = secondaryCta.textContent
    expect(
      ctaText?.includes('Login') || ctaText?.includes('Learn More')
    ).toBe(true)
  })

  // Additional tests for props customization
  it('accepts custom headline via props', () => {
    render(<HeroSection headline="Custom Headline" />)

    const headline = screen.getByRole('heading', { level: 1 })
    expect(headline.textContent).toBe('Custom Headline')
  })

  it('accepts custom CTA text via props', () => {
    render(<HeroSection primaryCtaText="Start Shortening" />)

    const primaryCta = screen.getByTestId('primary-cta')
    expect(primaryCta.textContent).toBe('Start Shortening')
  })

  it('renders hero section container', () => {
    render(<HeroSection />)

    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection).toBeInTheDocument()
    expect(heroSection).toHaveClass('hero')
  })
})
