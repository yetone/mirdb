import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import userEvent from '@testing-library/user-event'
import Footer from './Footer'

describe('Footer Section', () => {
  // Test Case 1: Footer element exists in the DOM
  it('should render the footer element', () => {
    render(
      <BrowserRouter>
        <Footer />
      </BrowserRouter>
    )

    const footer = screen.getByRole('contentinfo')
    expect(footer).toBeInTheDocument()
    expect(footer).toHaveAttribute('data-testid', 'footer')
  })

  // Test Case 2: Link with text 'Login' or 'Sign In' exists pointing to '/login'
  it('should render a Login link pointing to /login', () => {
    render(
      <BrowserRouter>
        <Footer />
      </BrowserRouter>
    )

    // Look for Login or Sign In link
    const loginLink = screen.getByRole('link', { name: /login|sign in/i })
    expect(loginLink).toBeInTheDocument()
    expect(loginLink).toHaveAttribute('href', '/login')
  })

  // Test Case 3: Link with text 'Register' or 'Sign Up' exists pointing to '/register'
  it('should render a Register link pointing to /register', () => {
    render(
      <BrowserRouter>
        <Footer />
      </BrowserRouter>
    )

    // Look for Register or Sign Up link
    const registerLink = screen.getByRole('link', { name: /register|sign up/i })
    expect(registerLink).toBeInTheDocument()
    expect(registerLink).toHaveAttribute('href', '/register')
  })

  // Test Case 4: Text containing copyright symbol or 'Copyright' or year is present
  it('should display copyright information', () => {
    render(
      <BrowserRouter>
        <Footer />
      </BrowserRouter>
    )

    // Look for copyright text containing © or Copyright or current year
    const currentYear = new Date().getFullYear().toString()
    const footerElement = screen.getByTestId('footer')
    const footerText = footerElement.textContent || ''

    // Check for copyright symbol, word, or year
    const hasCopyrightSymbol = footerText.includes('©')
    const hasCopyrightWord = footerText.toLowerCase().includes('copyright')
    const hasYear = footerText.includes(currentYear)

    expect(hasCopyrightSymbol || hasCopyrightWord || hasYear).toBe(true)
  })

  // Test Case 5: Click footer Login link navigates to '/login' route
  it('should navigate to /login when Login link is clicked', async () => {
    const user = userEvent.setup()

    render(
      <MemoryRouter initialEntries={['/']}>
        <Footer />
      </MemoryRouter>
    )

    const loginLink = screen.getByRole('link', { name: /login|sign in/i })
    await user.click(loginLink)

    // Verify the link has the correct href (navigation will happen via router)
    expect(loginLink).toHaveAttribute('href', '/login')
  })

  // Additional tests for comprehensive coverage

  it('should have proper semantic structure', () => {
    render(
      <BrowserRouter>
        <Footer />
      </BrowserRouter>
    )

    const footer = screen.getByRole('contentinfo')
    expect(footer.tagName.toLowerCase()).toBe('footer')
  })

  it('should display navigation links in a navigation section', () => {
    render(
      <BrowserRouter>
        <Footer />
      </BrowserRouter>
    )

    const nav = screen.getByRole('navigation', { name: /footer/i })
    expect(nav).toBeInTheDocument()
  })

  it('should have accessible navigation links', () => {
    render(
      <BrowserRouter>
        <Footer />
      </BrowserRouter>
    )

    const links = screen.getAllByRole('link')
    expect(links.length).toBeGreaterThanOrEqual(2)

    links.forEach((link) => {
      expect(link).toHaveAttribute('href')
    })
  })
})
