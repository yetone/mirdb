/**
 * Home Page Unit Tests
 * Owner: Shared - Scenario 1 (hero tests), other scenarios for other components
 *
 * Tests for the Home page component to verify:
 * - Hero section integration
 * - CTA button navigation
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { render, screen, fireEvent, mockNavigate } from './setup'
import Home from '@/pages/Home'

describe('Home Page', () => {
  beforeEach(() => {
    mockNavigate.mockClear()
  })

  it('renders the Home component with hero section', () => {
    render(<Home />)

    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection).toBeInTheDocument()
  })

  it('contains heading element with product tagline text', () => {
    render(<Home />)

    const headline = screen.getByRole('heading', { level: 1 })
    expect(headline).toBeInTheDocument()
    expect(headline).toHaveTextContent('Shorten. Share. Track.')
  })

  it('displays both Get Started and Sign In buttons', () => {
    render(<Home />)

    const getStartedButton = screen.getByTestId('get-started-button')
    const signInButton = screen.getByTestId('sign-in-button')

    expect(getStartedButton).toBeInTheDocument()
    expect(signInButton).toBeInTheDocument()
  })

  it('navigates to /register when Get Started button is clicked', () => {
    render(<Home />)

    const getStartedButton = screen.getByTestId('get-started-button')
    fireEvent.click(getStartedButton)

    expect(mockNavigate).toHaveBeenCalledWith('/register')
  })

  it('navigates to /login when Sign In button is clicked', () => {
    render(<Home />)

    const signInButton = screen.getByTestId('sign-in-button')
    fireEvent.click(signInButton)

    expect(mockNavigate).toHaveBeenCalledWith('/login')
  })

  it('renders the navbar component', () => {
    render(<Home />)

    // Use getAllByRole since there are multiple navigation elements (navbar and footer)
    const navElements = screen.getAllByRole('navigation')
    expect(navElements.length).toBeGreaterThanOrEqual(1)
    // The first navigation element should be the navbar
    expect(navElements[0]).toHaveClass('navbar')
  })

  it('has the main content area', () => {
    render(<Home />)

    const main = screen.getByRole('main')
    expect(main).toBeInTheDocument()
  })
})

/**
 * Navigation Header Tests
 * Owner: Scenario 6 - Navigation Header
 *
 * Tests for the Navigation Header component to verify:
 * - Navigation header is present in the DOM
 * - Logo/brand element exists
 * - Navigation links exist (Features, How It Works)
 * - Auth buttons (Sign In, Get Started) are present
 */
describe('Navigation Header', () => {
  beforeEach(() => {
    mockNavigate.mockClear()
  })

  it('renders navigation header in the DOM', () => {
    render(<Home />)

    // Use getAllByRole since there are multiple navigation elements (navbar and footer)
    const navElements = screen.getAllByRole('navigation')
    expect(navElements.length).toBeGreaterThanOrEqual(1)
    // The first navigation element should be the navbar
    expect(navElements[0]).toHaveClass('navbar')
  })

  it('displays logo/brand element', () => {
    render(<Home />)

    const brandLink = screen.getByRole('link', { name: /urlshortener/i })
    expect(brandLink).toBeInTheDocument()
  })

  it('has Features anchor link with correct href', () => {
    render(<Home />)

    const featuresLink = screen.getByRole('link', { name: /features/i })
    expect(featuresLink).toBeInTheDocument()
    expect(featuresLink).toHaveAttribute('href', '#features')
  })

  it('has How It Works anchor link with correct href', () => {
    render(<Home />)

    const howItWorksLink = screen.getByRole('link', { name: /how it works/i })
    expect(howItWorksLink).toBeInTheDocument()
    expect(howItWorksLink).toHaveAttribute('href', '#how-it-works')
  })

  it('displays Sign In button in the header', () => {
    render(<Home />)

    const signInButton = screen.getByTestId('sign-in-nav')
    expect(signInButton).toBeInTheDocument()
    expect(signInButton).toHaveTextContent('Sign In')
  })

  it('displays Get Started button in the header', () => {
    render(<Home />)

    const getStartedButton = screen.getByTestId('get-started-nav')
    expect(getStartedButton).toBeInTheDocument()
    expect(getStartedButton).toHaveTextContent('Get Started')
  })

  it('Sign In nav button navigates to /login when clicked', () => {
    render(<Home />)

    const signInButton = screen.getByTestId('sign-in-nav')
    fireEvent.click(signInButton)

    expect(mockNavigate).toHaveBeenCalledWith('/login')
  })

  it('Get Started nav button navigates to /register when clicked', () => {
    render(<Home />)

    const getStartedButton = screen.getByTestId('get-started-nav')
    fireEvent.click(getStartedButton)

    expect(mockNavigate).toHaveBeenCalledWith('/register')
  })
})
