/**
 * Navbar Component Tests
 * Owner: Scenario 2 - Navigation and CTA Buttons
 *
 * Test cases:
 * - Test case 3: Navigation contains links for Features, FAQ, Login, and Register/Get Started
 * - Test case 2: Click Login button navigates to /login
 * - Test case 1: Click Get Started navigates to /register
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import { Navbar } from './Navbar'

describe('Navbar', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  /**
   * Test Case 3: Query for navigation links
   * Expected: Navigation contains links for Features, FAQ, Login, and Register/Get Started
   */
  it('should contain navigation links for Features, FAQ, Login, and Register/Get Started', () => {
    render(
      <MemoryRouter>
        <Navbar />
      </MemoryRouter>
    )

    // Check navbar is rendered
    expect(screen.getByTestId('navbar')).toBeInTheDocument()

    // Check Features link exists (desktop)
    expect(screen.getByTestId('nav-features')).toBeInTheDocument()
    expect(screen.getByTestId('nav-features')).toHaveTextContent('Features')

    // Check FAQ link exists (desktop)
    expect(screen.getByTestId('nav-faq')).toBeInTheDocument()
    expect(screen.getByTestId('nav-faq')).toHaveTextContent('FAQ')

    // Check Login link exists (desktop)
    expect(screen.getByTestId('nav-login')).toBeInTheDocument()
    expect(screen.getByTestId('nav-login')).toHaveTextContent('Login')

    // Check Get Started/Register button exists (desktop)
    expect(screen.getByTestId('nav-register')).toBeInTheDocument()
    expect(screen.getByTestId('nav-register')).toHaveTextContent('Get Started')
  })

  /**
   * Test Case 2: Click 'Login' button in navigation
   * Expected: User is navigated to /login route
   */
  it('should navigate to /login when Login link is clicked', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route path="/" element={<Navbar />} />
          <Route path="/login" element={<div data-testid="login-page">Login Page</div>} />
        </Routes>
      </MemoryRouter>
    )

    const loginLink = screen.getByTestId('nav-login')
    expect(loginLink).toHaveAttribute('href', '/login')

    fireEvent.click(loginLink)
    expect(screen.getByTestId('login-page')).toBeInTheDocument()
  })

  /**
   * Test Case 1: Click 'Get Started' CTA button
   * Expected: User is navigated to /register route
   */
  it('should navigate to /register when Get Started button is clicked', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route path="/" element={<Navbar />} />
          <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
        </Routes>
      </MemoryRouter>
    )

    const registerButton = screen.getByTestId('nav-register')
    fireEvent.click(registerButton)

    expect(screen.getByTestId('register-page')).toBeInTheDocument()
  })

  /**
   * Test Case 4: Click Features link in navigation
   * Expected: scrollToSection is called with 'features-section'
   */
  it('should scroll to features section when Features link is clicked', () => {
    // Mock scrollIntoView
    const scrollIntoViewMock = vi.fn()
    const mockElement = { scrollIntoView: scrollIntoViewMock }
    vi.spyOn(document, 'getElementById').mockReturnValue(mockElement as unknown as HTMLElement)

    render(
      <MemoryRouter>
        <Navbar />
      </MemoryRouter>
    )

    const featuresLink = screen.getByTestId('nav-features')
    fireEvent.click(featuresLink)

    expect(document.getElementById).toHaveBeenCalledWith('features-section')
    expect(scrollIntoViewMock).toHaveBeenCalledWith({ behavior: 'smooth' })
  })

  /**
   * Test Case 5: Click FAQ link in navigation
   * Expected: scrollToSection is called with 'faq-section'
   */
  it('should scroll to FAQ section when FAQ link is clicked', () => {
    // Mock scrollIntoView
    const scrollIntoViewMock = vi.fn()
    const mockElement = { scrollIntoView: scrollIntoViewMock }
    vi.spyOn(document, 'getElementById').mockReturnValue(mockElement as unknown as HTMLElement)

    render(
      <MemoryRouter>
        <Navbar />
      </MemoryRouter>
    )

    const faqLink = screen.getByTestId('nav-faq')
    fireEvent.click(faqLink)

    expect(document.getElementById).toHaveBeenCalledWith('faq-section')
    expect(scrollIntoViewMock).toHaveBeenCalledWith({ behavior: 'smooth' })
  })

  it('should render the logo with link to homepage', () => {
    render(
      <MemoryRouter>
        <Navbar />
      </MemoryRouter>
    )

    const logo = screen.getByTestId('navbar-logo')
    expect(logo).toBeInTheDocument()
    expect(logo).toHaveAttribute('href', '/')
    expect(logo).toHaveTextContent('URL Shortener')
  })

  it('should have proper accessibility attributes', () => {
    render(
      <MemoryRouter>
        <Navbar />
      </MemoryRouter>
    )

    const navbar = screen.getByTestId('navbar')
    expect(navbar).toHaveAttribute('role', 'navigation')
    expect(navbar).toHaveAttribute('aria-label', 'Main navigation')
  })

  it('should hide auth buttons when showAuthButtons is false', () => {
    render(
      <MemoryRouter>
        <Navbar showAuthButtons={false} />
      </MemoryRouter>
    )

    expect(screen.queryByTestId('nav-login')).not.toBeInTheDocument()
    expect(screen.queryByTestId('nav-register')).not.toBeInTheDocument()
  })

  it('should render mobile menu toggle', () => {
    render(
      <MemoryRouter>
        <Navbar />
      </MemoryRouter>
    )

    expect(screen.getByTestId('mobile-menu-toggle')).toBeInTheDocument()
  })
})
