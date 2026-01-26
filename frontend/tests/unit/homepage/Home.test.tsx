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

    const navbar = screen.getByRole('navigation')
    expect(navbar).toBeInTheDocument()
  })

  it('has the main content area', () => {
    render(<Home />)

    const main = screen.getByRole('main')
    expect(main).toBeInTheDocument()
  })
})
