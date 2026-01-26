/**
 * HeroSection Unit Tests
 * Owner: Scenario 1 - Hero Section Display and Value Proposition
 *
 * Tests for the HeroSection component to verify:
 * - Headline and subheadline render correctly
 * - CTA buttons are present and functional
 * - Accessibility requirements are met
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from './setup'
import { HeroSection } from '@/components/homepage/HeroSection'

describe('HeroSection', () => {
  it('renders the hero section with headline', () => {
    render(<HeroSection />)

    const headline = screen.getByTestId('hero-headline')
    expect(headline).toBeInTheDocument()
    expect(headline).toHaveTextContent('Shorten. Share. Track.')
  })

  it('renders the subheadline with value proposition', () => {
    render(<HeroSection />)

    const subheadline = screen.getByTestId('hero-subheadline')
    expect(subheadline).toBeInTheDocument()
    expect(subheadline).toHaveTextContent('Transform your long URLs into memorable short links')
  })

  it('renders the Get Started button', () => {
    render(<HeroSection />)

    const getStartedButton = screen.getByTestId('get-started-button')
    expect(getStartedButton).toBeInTheDocument()
    expect(getStartedButton).toHaveTextContent('Get Started Free')
  })

  it('renders the Sign In button', () => {
    render(<HeroSection />)

    const signInButton = screen.getByTestId('sign-in-button')
    expect(signInButton).toBeInTheDocument()
    expect(signInButton).toHaveTextContent('Sign In')
  })

  it('calls onGetStarted when Get Started button is clicked', () => {
    const onGetStarted = vi.fn()
    render(<HeroSection onGetStarted={onGetStarted} />)

    const getStartedButton = screen.getByTestId('get-started-button')
    fireEvent.click(getStartedButton)

    expect(onGetStarted).toHaveBeenCalledTimes(1)
  })

  it('calls onSignIn when Sign In button is clicked', () => {
    const onSignIn = vi.fn()
    render(<HeroSection onSignIn={onSignIn} />)

    const signInButton = screen.getByTestId('sign-in-button')
    fireEvent.click(signInButton)

    expect(onSignIn).toHaveBeenCalledTimes(1)
  })

  it('has accessible heading structure', () => {
    render(<HeroSection />)

    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toBeInTheDocument()
    expect(heading).toHaveAttribute('id', 'hero-heading')
  })

  it('hero section has proper aria-labelledby attribute', () => {
    render(<HeroSection />)

    const section = screen.getByTestId('hero-section')
    expect(section).toHaveAttribute('aria-labelledby', 'hero-heading')
  })

  it('CTA buttons have accessible labels', () => {
    render(<HeroSection />)

    const getStartedButton = screen.getByRole('button', { name: /get started with url shortening for free/i })
    const signInButton = screen.getByRole('button', { name: /sign in to your account/i })

    expect(getStartedButton).toBeInTheDocument()
    expect(signInButton).toBeInTheDocument()
  })

  it('renders the CTA container with both buttons', () => {
    render(<HeroSection />)

    const ctaContainer = screen.getByTestId('hero-cta-container')
    expect(ctaContainer).toBeInTheDocument()

    const getStartedButton = screen.getByTestId('get-started-button')
    const signInButton = screen.getByTestId('sign-in-button')

    expect(ctaContainer).toContainElement(getStartedButton)
    expect(ctaContainer).toContainElement(signInButton)
  })
})
