/**
 * HeroSection Unit Tests
 * Owner: Scenario 1 (Hero Section Display)
 *
 * Tests for the hero section component:
 * - Product name and tagline display
 * - URL input field presence
 * - Primary and secondary CTA buttons
 */

import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { describe, it, expect } from 'vitest'
import HeroSection from '../../../../src/components/homepage/HeroSection'

const renderHeroSection = () => {
  return render(
    <BrowserRouter>
      <HeroSection />
    </BrowserRouter>
  )
}

describe('HeroSection', () => {
  it('displays product name and tagline', () => {
    renderHeroSection()

    expect(screen.getByText('URL Shortening Service')).toBeInTheDocument()
    expect(screen.getByText('Shorten Links. Track Insights. Share Smarter.')).toBeInTheDocument()
  })

  it('displays URL input field with placeholder text', () => {
    renderHeroSection()

    const urlInput = screen.getByTestId('url-input')
    expect(urlInput).toBeInTheDocument()
    expect(urlInput).toHaveAttribute('placeholder', 'Enter your long URL here...')
  })

  it('displays primary CTA Shorten URL button', () => {
    renderHeroSection()

    const shortenButton = screen.getByTestId('shorten-url-button')
    expect(shortenButton).toBeInTheDocument()
    expect(shortenButton).toHaveTextContent('Shorten URL')
  })

  it('displays secondary CTAs Sign Up Free and Log In buttons', () => {
    renderHeroSection()

    const signUpButton = screen.getByTestId('signup-button')
    const loginButton = screen.getByTestId('login-button')

    expect(signUpButton).toBeInTheDocument()
    expect(signUpButton).toHaveTextContent('Sign Up Free')
    expect(loginButton).toBeInTheDocument()
    expect(loginButton).toHaveTextContent('Log In')
  })

  it('renders hero section with proper structure', () => {
    renderHeroSection()

    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection).toBeInTheDocument()
    expect(heroSection).toHaveClass('hero')
  })
})
