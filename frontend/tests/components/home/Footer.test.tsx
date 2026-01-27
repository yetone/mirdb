import { describe, it, expect } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { BrowserRouter, MemoryRouter, useLocation } from 'react-router-dom'
import { Footer } from '../../../src/components/home/Footer'

// Helper component to track navigation
function LocationDisplay() {
  const location = useLocation()
  return <div data-testid="location-display">{location.pathname}</div>
}

// Test wrapper with necessary providers
function TestWrapper({ children }: { children: React.ReactNode }) {
  return <BrowserRouter>{children}</BrowserRouter>
}

function TestWrapperWithMemoryRouter({
  children,
  initialEntries = ['/'],
}: {
  children: React.ReactNode
  initialEntries?: string[]
}) {
  return (
    <MemoryRouter initialEntries={initialEntries}>
      {children}
      <LocationDisplay />
    </MemoryRouter>
  )
}

describe('Footer', () => {
  // Test Case 1: Component renders without errors
  it('renders footer component without errors', () => {
    render(
      <TestWrapper>
        <Footer />
      </TestWrapper>
    )

    const footer = screen.getByTestId('footer')
    expect(footer).toBeInTheDocument()
    expect(footer).toBeVisible()
  })

  // Test Case 2: Home navigation link is present
  it('displays Home navigation link', () => {
    render(
      <TestWrapper>
        <Footer />
      </TestWrapper>
    )

    const homeLink = screen.getByTestId('footer-nav-home')
    expect(homeLink).toBeInTheDocument()
    expect(homeLink).toHaveTextContent('Home')
    expect(homeLink).toHaveAttribute('href', '/')
  })

  // Test Case 3: Dashboard navigation link is present
  it('displays Dashboard navigation link', () => {
    render(
      <TestWrapper>
        <Footer />
      </TestWrapper>
    )

    const dashboardLink = screen.getByTestId('footer-nav-dashboard')
    expect(dashboardLink).toBeInTheDocument()
    expect(dashboardLink).toHaveTextContent('Dashboard')
    expect(dashboardLink).toHaveAttribute('href', '/dashboard')
  })

  // Test Case 4: Login navigation link is present
  it('displays Login navigation link', () => {
    render(
      <TestWrapper>
        <Footer />
      </TestWrapper>
    )

    const loginLink = screen.getByTestId('footer-nav-login')
    expect(loginLink).toBeInTheDocument()
    expect(loginLink).toHaveTextContent('Login')
    expect(loginLink).toHaveAttribute('href', '/login')
  })

  // Test Case 5: Register navigation link is present
  it('displays Register navigation link', () => {
    render(
      <TestWrapper>
        <Footer />
      </TestWrapper>
    )

    const registerLink = screen.getByTestId('footer-nav-register')
    expect(registerLink).toBeInTheDocument()
    expect(registerLink).toHaveTextContent('Register')
    expect(registerLink).toHaveAttribute('href', '/register')
  })

  // Test Case 6: Privacy Policy link is present
  it('displays Privacy Policy link', () => {
    render(
      <TestWrapper>
        <Footer />
      </TestWrapper>
    )

    const privacyLink = screen.getByTestId('footer-legal-privacy-policy')
    expect(privacyLink).toBeInTheDocument()
    expect(privacyLink).toHaveTextContent('Privacy Policy')
    expect(privacyLink).toHaveAttribute('href', '/privacy')
  })

  // Test Case 7: Terms of Service link is present
  it('displays Terms of Service link', () => {
    render(
      <TestWrapper>
        <Footer />
      </TestWrapper>
    )

    const termsLink = screen.getByTestId('footer-legal-terms-of-service')
    expect(termsLink).toBeInTheDocument()
    expect(termsLink).toHaveTextContent('Terms of Service')
    expect(termsLink).toHaveAttribute('href', '/terms')
  })

  // Test Case 8: Copyright notice is present with year
  it('displays copyright notice with current year', () => {
    render(
      <TestWrapper>
        <Footer />
      </TestWrapper>
    )

    const copyright = screen.getByTestId('footer-copyright')
    expect(copyright).toBeInTheDocument()

    const currentYear = new Date().getFullYear()
    expect(copyright).toHaveTextContent(currentYear.toString())
    expect(copyright).toHaveTextContent(/©/)
    expect(copyright).toHaveTextContent(/all rights reserved/i)
  })

  // Test Case 9: Clicking Privacy Policy link navigates to privacy policy page
  it('navigates to /privacy when clicking Privacy Policy link', () => {
    render(
      <TestWrapperWithMemoryRouter initialEntries={['/']}>
        <Footer />
      </TestWrapperWithMemoryRouter>
    )

    const privacyLink = screen.getByTestId('footer-legal-privacy-policy')
    fireEvent.click(privacyLink)

    const locationDisplay = screen.getByTestId('location-display')
    expect(locationDisplay).toHaveTextContent('/privacy')
  })

  // Test Case 10: Clicking Terms of Service link navigates to terms page
  it('navigates to /terms when clicking Terms of Service link', () => {
    render(
      <TestWrapperWithMemoryRouter initialEntries={['/']}>
        <Footer />
      </TestWrapperWithMemoryRouter>
    )

    const termsLink = screen.getByTestId('footer-legal-terms-of-service')
    fireEvent.click(termsLink)

    const locationDisplay = screen.getByTestId('location-display')
    expect(locationDisplay).toHaveTextContent('/terms')
  })

  // Additional test: Footer has proper accessibility attributes
  it('has proper accessibility structure with role=contentinfo', () => {
    render(
      <TestWrapper>
        <Footer />
      </TestWrapper>
    )

    const footer = screen.getByTestId('footer')
    expect(footer).toHaveAttribute('role', 'contentinfo')
    expect(footer.tagName.toLowerCase()).toBe('footer')
  })

  // Additional test: Navigation sections have aria-labels
  it('has proper aria-labels for navigation sections', () => {
    render(
      <TestWrapper>
        <Footer />
      </TestWrapper>
    )

    const footerNav = screen.getByLabelText('Footer navigation')
    expect(footerNav).toBeInTheDocument()

    const legalNav = screen.getByLabelText('Legal links')
    expect(legalNav).toBeInTheDocument()
  })

  // Additional test: All navigation links are functional
  it.each([
    { testId: 'footer-nav-home', path: '/' },
    { testId: 'footer-nav-dashboard', path: '/dashboard' },
    { testId: 'footer-nav-login', path: '/login' },
    { testId: 'footer-nav-register', path: '/register' },
  ])('navigates correctly to $path when clicking $testId', ({ testId, path }) => {
    render(
      <TestWrapperWithMemoryRouter initialEntries={['/test']}>
        <Footer />
      </TestWrapperWithMemoryRouter>
    )

    const link = screen.getByTestId(testId)
    fireEvent.click(link)

    const locationDisplay = screen.getByTestId('location-display')
    expect(locationDisplay).toHaveTextContent(path)
  })
})
