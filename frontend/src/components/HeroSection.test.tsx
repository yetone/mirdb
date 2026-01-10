import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import HeroSection from './HeroSection'

const renderHeroSection = () => {
  return render(
    <BrowserRouter>
      <HeroSection />
    </BrowserRouter>
  )
}

describe('HeroSection', () => {
  // Test Case 1: Component renders without errors and displays headline text
  it('renders without errors and displays headline text', () => {
    renderHeroSection()

    // Check component renders
    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection).toBeInTheDocument()

    // Check headline is present
    const headline = screen.getByRole('heading', { level: 1 })
    expect(headline).toBeInTheDocument()
  })

  // Test Case 3: Headline contains value proposition text
  it('displays headline with value proposition text like "Shorten, Share, Track"', () => {
    renderHeroSection()

    const headline = screen.getByRole('heading', { level: 1 })
    expect(headline).toHaveTextContent(/shorten/i)
    expect(headline).toHaveTextContent(/share/i)
    expect(headline).toHaveTextContent(/track/i)
  })

  // Test Case 4: Subheadline explaining the service is present
  it('displays subheadline explaining the service in one sentence', () => {
    renderHeroSection()

    const subheadline = screen.getByTestId('hero-subheadline')
    expect(subheadline).toBeInTheDocument()
    // Subheadline should have meaningful content about the service
    expect(subheadline.textContent?.length).toBeGreaterThan(20)
  })

  it('displays primary CTA button for getting started', () => {
    renderHeroSection()

    const ctaButton = screen.getByRole('link', { name: /get started/i })
    expect(ctaButton).toBeInTheDocument()
    expect(ctaButton).toHaveAttribute('href', '/register')
  })

  it('displays secondary CTA link for login', () => {
    renderHeroSection()

    const loginLink = screen.getByRole('link', { name: /log in/i })
    expect(loginLink).toBeInTheDocument()
    expect(loginLink).toHaveAttribute('href', '/login')
  })

  it('has gradient background styling', () => {
    renderHeroSection()

    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection).toHaveClass('bg-gradient-to-br')
  })
})
