/**
 * Navbar Unit Tests
 * Owner: Scenario 4 - Navigation Component
 *
 * Unit tests for the Navbar component verifying:
 * - Rendering of all navigation elements
 * - Click handlers work correctly
 * - Component structure matches requirements
 *
 * Requirements: REQ-5
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { Navbar } from '../../../src/components/Navbar'

const renderWithRouter = (component: React.ReactNode) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

describe('Navbar Component', () => {
  it('renders logo, Features link, Login and Sign Up buttons', () => {
    renderWithRouter(<Navbar />)

    // Verify logo
    const logo = screen.getByTestId('navbar-logo')
    expect(logo).toBeInTheDocument()
    expect(logo).toHaveTextContent('LinkShort')

    // Verify Features link
    const featuresLink = screen.getByTestId('navbar-features-link')
    expect(featuresLink).toBeInTheDocument()
    expect(featuresLink).toHaveTextContent('Features')

    // Verify Login button
    const loginBtn = screen.getByTestId('navbar-login-btn')
    expect(loginBtn).toBeInTheDocument()
    expect(loginBtn).toHaveTextContent('Login')

    // Verify Sign Up button
    const signupBtn = screen.getByTestId('navbar-signup-btn')
    expect(signupBtn).toBeInTheDocument()
    expect(signupBtn).toHaveTextContent('Sign Up')
  })

  it('logo links to homepage', () => {
    renderWithRouter(<Navbar />)

    const logo = screen.getByTestId('navbar-logo')
    expect(logo).toHaveAttribute('href', '/')
  })

  it('Features link has correct anchor href', () => {
    renderWithRouter(<Navbar />)

    const featuresLink = screen.getByTestId('navbar-features-link')
    expect(featuresLink).toHaveAttribute('href', '#features-section')
  })

  it('calls onFeaturesClick when Features link is clicked', () => {
    const mockFeaturesClick = vi.fn()
    renderWithRouter(<Navbar onFeaturesClick={mockFeaturesClick} />)

    const featuresLink = screen.getByTestId('navbar-features-link')
    fireEvent.click(featuresLink)

    expect(mockFeaturesClick).toHaveBeenCalledTimes(1)
  })

  it('has fixed positioning for sticky header', () => {
    renderWithRouter(<Navbar />)

    const navbar = screen.getByTestId('navbar')
    expect(navbar).toHaveClass('fixed')
    expect(navbar).toHaveClass('top-0')
  })

  it('navbar is accessible with proper structure', () => {
    renderWithRouter(<Navbar />)

    const navbar = screen.getByTestId('navbar')
    expect(navbar.tagName.toLowerCase()).toBe('nav')
  })
})
