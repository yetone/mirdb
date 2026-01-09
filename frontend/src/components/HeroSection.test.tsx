import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import HeroSection from './HeroSection'

const renderWithRouter = (component: React.ReactNode) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

describe('HeroSection', () => {
  it('renders without errors and displays all required elements', () => {
    renderWithRouter(<HeroSection />)

    // Check hero section is rendered
    expect(screen.getByTestId('hero-section')).toBeInTheDocument()

    // Check headline is rendered
    expect(screen.getByTestId('hero-headline')).toBeInTheDocument()

    // Check subheadline is rendered
    expect(screen.getByTestId('hero-subheadline')).toBeInTheDocument()

    // Check CTA buttons are rendered
    expect(screen.getByTestId('hero-cta-primary')).toBeInTheDocument()
    expect(screen.getByTestId('hero-cta-secondary')).toBeInTheDocument()
  })

  it('displays headline with value proposition text related to URL shortening', () => {
    renderWithRouter(<HeroSection />)

    const headline = screen.getByTestId('hero-headline')
    const headlineText = headline.textContent?.toLowerCase() || ''

    // Headline should contain at least one of: Shorten, Share, Track
    const hasShorten = headlineText.includes('shorten')
    const hasShare = headlineText.includes('share')
    const hasTrack = headlineText.includes('track')

    expect(hasShorten || hasShare || hasTrack).toBe(true)
  })

  it('displays default headline text "Shorten. Share. Track."', () => {
    renderWithRouter(<HeroSection />)

    const headline = screen.getByTestId('hero-headline')
    expect(headline).toHaveTextContent('Shorten. Share. Track.')
  })

  it('displays subheadline with service description', () => {
    renderWithRouter(<HeroSection />)

    const subheadline = screen.getByTestId('hero-subheadline')
    expect(subheadline).toHaveTextContent(/url|link|analytics|click/i)
  })

  it('renders "Get Started Free" button with correct link', () => {
    renderWithRouter(<HeroSection />)

    const primaryCta = screen.getByTestId('hero-cta-primary')
    expect(primaryCta).toHaveTextContent('Get Started Free')
    expect(primaryCta).toHaveAttribute('href', '/register')
  })

  it('renders "Log In" button with correct link', () => {
    renderWithRouter(<HeroSection />)

    const secondaryCta = screen.getByTestId('hero-cta-secondary')
    expect(secondaryCta).toHaveTextContent('Log In')
    expect(secondaryCta).toHaveAttribute('href', '/login')
  })

  it('accepts custom props for headline and subheadline', () => {
    const customProps = {
      headline: 'Custom Headline',
      subheadline: 'Custom subheadline text',
    }

    renderWithRouter(<HeroSection {...customProps} />)

    expect(screen.getByTestId('hero-headline')).toHaveTextContent('Custom Headline')
    expect(screen.getByTestId('hero-subheadline')).toHaveTextContent('Custom subheadline text')
  })

  it('accepts custom CTA text and links', () => {
    const customProps = {
      primaryCtaText: 'Sign Up Now',
      primaryCtaLink: '/signup',
      secondaryCtaText: 'Learn More',
      secondaryCtaLink: '/about',
    }

    renderWithRouter(<HeroSection {...customProps} />)

    const primaryCta = screen.getByTestId('hero-cta-primary')
    const secondaryCta = screen.getByTestId('hero-cta-secondary')

    expect(primaryCta).toHaveTextContent('Sign Up Now')
    expect(primaryCta).toHaveAttribute('href', '/signup')
    expect(secondaryCta).toHaveTextContent('Learn More')
    expect(secondaryCta).toHaveAttribute('href', '/about')
  })
})
