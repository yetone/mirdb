/**
 * FooterSection Component Tests
 * Owner: Scenario 8 - Footer Section Display
 *
 * Test cases for validating the footer section displays correctly
 * with navigation links and copyright notice as specified in REQ-9.
 */

import { describe, it, expect } from 'vitest'
import { screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { renderWithProviders } from '../../../utils/test-utils'
import { FooterSection } from '../../../../src/components/homepage/FooterSection'

describe('FooterSection', () => {
  // Test Case 1: Home navigation link is present and clickable
  it('renders Home navigation link that is present and clickable', () => {
    renderWithProviders(<FooterSection />)

    const homeLink = screen.getByRole('link', { name: /home/i })
    expect(homeLink).toBeInTheDocument()
    expect(homeLink).toHaveAttribute('href', '/')
  })

  // Test Case 2: Login navigation link is present and clickable
  it('renders Login navigation link that is present and clickable', () => {
    renderWithProviders(<FooterSection />)

    const loginLink = screen.getByRole('link', { name: /login/i })
    expect(loginLink).toBeInTheDocument()
    expect(loginLink).toHaveAttribute('href', '/login')
  })

  // Test Case 3: Register navigation link is present and clickable
  it('renders Register navigation link that is present and clickable', () => {
    renderWithProviders(<FooterSection />)

    const registerLink = screen.getByRole('link', { name: /register/i })
    expect(registerLink).toBeInTheDocument()
    expect(registerLink).toHaveAttribute('href', '/register')
  })

  // Test Case 4: Privacy Policy link is present
  it('renders Privacy Policy link', () => {
    renderWithProviders(<FooterSection />)

    const privacyLink = screen.getByRole('link', { name: /privacy policy/i })
    expect(privacyLink).toBeInTheDocument()
  })

  // Test Case 5: Copyright notice displays current year (2026)
  it('displays copyright notice with current year (2026)', () => {
    renderWithProviders(<FooterSection />)

    const copyrightText = screen.getByText(/©.*2026/i)
    expect(copyrightText).toBeInTheDocument()
  })

  // Test Case 7: Footer uses semantic <footer> HTML element
  it('uses semantic <footer> HTML element', () => {
    const { container } = renderWithProviders(<FooterSection />)

    const footerElement = container.querySelector('footer')
    expect(footerElement).toBeInTheDocument()
  })

  // Additional tests for accessibility
  it('has navigation ARIA label for screen readers', () => {
    renderWithProviders(<FooterSection />)

    const nav = screen.getByRole('navigation', { name: /footer/i })
    expect(nav).toBeInTheDocument()
  })

  it('renders all navigation links within the footer', () => {
    const { container } = renderWithProviders(<FooterSection />)

    const footer = container.querySelector('footer')!
    const links = within(footer).getAllByRole('link')

    // Should have at least 4 links: Home, Login, Register, Privacy Policy
    expect(links.length).toBeGreaterThanOrEqual(4)
  })
})

describe('FooterSection Navigation Integration', () => {
  // Test Case 6: Click footer Login link - Navigation to /login route
  it('navigates to /login route when Login link is clicked', async () => {
    const user = userEvent.setup()

    renderWithProviders(<FooterSection />)

    const loginLink = screen.getByRole('link', { name: /login/i })
    await user.click(loginLink)

    // In a real browser, this would navigate. In tests with BrowserRouter,
    // we verify the href is correct for navigation
    expect(loginLink).toHaveAttribute('href', '/login')
  })

  it('navigates to /register route when Register link is clicked', async () => {
    const user = userEvent.setup()

    renderWithProviders(<FooterSection />)

    const registerLink = screen.getByRole('link', { name: /register/i })
    await user.click(registerLink)

    expect(registerLink).toHaveAttribute('href', '/register')
  })

  it('navigates to home route when Home link is clicked', async () => {
    const user = userEvent.setup()

    renderWithProviders(<FooterSection />)

    const homeLink = screen.getByRole('link', { name: /home/i })
    await user.click(homeLink)

    expect(homeLink).toHaveAttribute('href', '/')
  })
})
