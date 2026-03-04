/**
 * Home Page Unit Tests
 *
 * Tests for the homepage component rendering and integration.
 */
import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { Home } from './Home'

// Helper to wrap component with router
const renderWithRouter = (ui: React.ReactElement) => {
  return render(<BrowserRouter>{ui}</BrowserRouter>)
}

describe('Home Page', () => {
  it('should render the home page container', () => {
    renderWithRouter(<Home />)

    const homePage = screen.getByTestId('home-page')
    expect(homePage).toBeInTheDocument()
  })

  it('should have proper accessibility role', () => {
    renderWithRouter(<Home />)

    const main = screen.getByRole('main')
    expect(main).toBeInTheDocument()
    expect(main).toHaveAttribute('aria-label', 'Homepage')
  })

  it('should include the HeroSection component', () => {
    renderWithRouter(<Home />)

    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection).toBeInTheDocument()
  })

  it('should render hero headline within the home page', () => {
    renderWithRouter(<Home />)

    const headline = screen.getByTestId('hero-headline')
    expect(headline).toBeInTheDocument()
  })

  it('should render hero subheading within the home page', () => {
    renderWithRouter(<Home />)

    const subheading = screen.getByTestId('hero-subheading')
    expect(subheading).toBeInTheDocument()
  })

  it('should render hero CTA button within the home page', () => {
    renderWithRouter(<Home />)

    const ctaButton = screen.getByTestId('hero-cta')
    expect(ctaButton).toBeInTheDocument()
  })
})
