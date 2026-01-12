import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import HeroSection from './HeroSection'

// Mock scrollIntoView
Element.prototype.scrollIntoView = vi.fn()

const renderWithRouter = (component: React.ReactElement, { initialEntries = ['/'] } = {}) => {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      {component}
    </MemoryRouter>
  )
}

describe('HeroSection', () => {
  // Test Case 1: Hero section contains headline with tagline about URL shortening
  it('renders hero section with headline containing URL shortening tagline', () => {
    renderWithRouter(<HeroSection />)

    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection).toBeInTheDocument()

    const headline = screen.getByTestId('hero-headline')
    expect(headline).toBeInTheDocument()
    expect(headline.textContent).toContain('Shorten')
    expect(headline.textContent).toMatch(/shorten|short|url/i)
  })

  // Test Case 2: Subheadline describes analytics capabilities
  it('renders subheadline describing analytics capabilities', () => {
    renderWithRouter(<HeroSection />)

    const subheadline = screen.getByTestId('hero-subheadline')
    expect(subheadline).toBeInTheDocument()
    expect(subheadline.textContent?.toLowerCase()).toMatch(/analytics|track|insight|statistics/)
  })

  // Test Case 3: Get Started Free button navigates to /register
  it('renders Get Started Free button that links to /register', () => {
    renderWithRouter(<HeroSection />)

    const primaryCTA = screen.getByTestId('hero-cta-primary')
    expect(primaryCTA).toBeInTheDocument()
    expect(primaryCTA.textContent).toContain('Get Started Free')
    expect(primaryCTA).toHaveAttribute('href', '/register')
  })

  // Test Case 4: Learn More button scrolls to Features section
  it('renders Learn More button that scrolls to features section', () => {
    // Create a mock features section
    const featuresDiv = document.createElement('div')
    featuresDiv.id = 'features'
    document.body.appendChild(featuresDiv)

    renderWithRouter(<HeroSection />)

    const secondaryCTA = screen.getByTestId('hero-cta-secondary')
    expect(secondaryCTA).toBeInTheDocument()
    expect(secondaryCTA.textContent).toContain('Learn More')

    fireEvent.click(secondaryCTA)
    expect(featuresDiv.scrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' })

    // Cleanup
    document.body.removeChild(featuresDiv)
  })

  // Test Case 5: Hero section visual element is rendered
  it('renders illustration or animated graphic in hero section', () => {
    renderWithRouter(<HeroSection />)

    const visual = screen.getByTestId('hero-visual')
    expect(visual).toBeInTheDocument()

    // Check that the visual contains an SVG element (our animated illustration)
    const svg = visual.querySelector('svg')
    expect(svg).toBeInTheDocument()
  })
})
