import { describe, it, expect } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Home from '../pages/Home'

describe('Home Page Component Composition', () => {
  // Test Case 1: Render Home page component - Component renders without errors
  it('renders Home page component without errors', () => {
    expect(() => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )
    }).not.toThrow()

    // Verify main element is present
    const mainElement = screen.getByRole('main')
    expect(mainElement).toBeInTheDocument()
  })

  // Test Case 2: Check for HeroSection component - HeroSection is rendered as child of Home
  it('renders HeroSection as child of Home component', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <Home />
      </MemoryRouter>
    )

    const mainElement = screen.getByRole('main')
    const heroSection = within(mainElement).getByTestId('hero-section')
    expect(heroSection).toBeInTheDocument()

    // Verify HeroSection contains expected content
    const headline = within(heroSection).getByRole('heading', { level: 1 })
    expect(headline).toHaveTextContent('Shorten, Share, Track')
  })

  // Test Case 3: Check for FeaturesSection component - FeaturesSection is rendered as child of Home
  it('renders FeaturesSection as child of Home component', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <Home />
      </MemoryRouter>
    )

    const mainElement = screen.getByRole('main')
    const featuresSection = within(mainElement).getByTestId('features-section')
    expect(featuresSection).toBeInTheDocument()

    // Verify FeaturesSection contains expected content
    const heading = within(featuresSection).getByRole('heading', { level: 2 })
    expect(heading).toHaveTextContent('Powerful Features')
  })

  // Test Case 4: Check for HowItWorksSection component - HowItWorksSection is rendered as child of Home
  it('renders HowItWorksSection as child of Home component', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <Home />
      </MemoryRouter>
    )

    const mainElement = screen.getByRole('main')
    const howItWorksSection = within(mainElement).getByTestId('how-it-works-section')
    expect(howItWorksSection).toBeInTheDocument()

    // Verify HowItWorksSection contains expected content
    const heading = within(howItWorksSection).getByRole('heading', { level: 2 })
    expect(heading).toHaveTextContent('How It Works')
  })

  // Test Case 5: Check for FooterSection component - FooterSection is rendered as child of Home
  it('renders FooterSection as child of Home component', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <Home />
      </MemoryRouter>
    )

    const mainElement = screen.getByRole('main')
    const footerSection = within(mainElement).getByTestId('footer-section')
    expect(footerSection).toBeInTheDocument()

    // Verify FooterSection contains expected content (brand name)
    expect(within(footerSection).getByText('URL Shortener')).toBeInTheDocument()
  })

  // Additional test: Verify component order (Hero → Features → HowItWorks → Footer)
  it('renders section components in correct visual order', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <Home />
      </MemoryRouter>
    )

    const mainElement = screen.getByRole('main')
    const children = Array.from(mainElement.children)

    // Verify we have exactly 4 section components
    expect(children).toHaveLength(4)

    // Verify order by checking data-testid attributes
    expect(children[0]).toHaveAttribute('data-testid', 'hero-section')
    expect(children[1]).toHaveAttribute('data-testid', 'features-section')
    expect(children[2]).toHaveAttribute('data-testid', 'how-it-works-section')
    expect(children[3]).toHaveAttribute('data-testid', 'footer-section')
  })

  // Verify all four sections are present together
  it('renders all four section components simultaneously', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <Home />
      </MemoryRouter>
    )

    const heroSection = screen.getByTestId('hero-section')
    const featuresSection = screen.getByTestId('features-section')
    const howItWorksSection = screen.getByTestId('how-it-works-section')
    const footerSection = screen.getByTestId('footer-section')

    expect(heroSection).toBeInTheDocument()
    expect(featuresSection).toBeInTheDocument()
    expect(howItWorksSection).toBeInTheDocument()
    expect(footerSection).toBeInTheDocument()
  })
})
