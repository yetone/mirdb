import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import HeroSection from '../src/components/HeroSection'
import { AppRoutes } from '../src/App'

// Test Case 1: Hero section renders with headline containing value proposition
describe('HeroSection Integration Tests', () => {
  it('renders hero section with value proposition headline when navigating to "/" route', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <AppRoutes />
      </MemoryRouter>
    )

    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection).toBeInTheDocument()

    // Check for value proposition text (e.g., 'Shorten Links' or similar)
    const headline = screen.getByRole('heading', { level: 1 })
    expect(headline).toHaveTextContent(/shorten links/i)
  })
})

// Test Case 2: H1 element exists with compelling headline
describe('HeroSection Unit Tests - Headline', () => {
  it('has H1 element with compelling headline text communicating core value', () => {
    render(
      <MemoryRouter>
        <HeroSection />
      </MemoryRouter>
    )

    const h1Element = screen.getByRole('heading', { level: 1 })
    expect(h1Element).toBeInTheDocument()
    expect(h1Element.tagName).toBe('H1')
    // Verify headline communicates core value
    expect(h1Element).toHaveTextContent(/shorten links/i)
    expect(h1Element).toHaveTextContent(/amplify reach/i)
  })
})

// Test Case 3: Subheadline text exists with key benefit statement
describe('HeroSection Unit Tests - Subheadline', () => {
  it('has subheadline text with key benefit statement', () => {
    render(
      <MemoryRouter>
        <HeroSection />
      </MemoryRouter>
    )

    // Find the subheadline paragraph
    const subheadline = screen.getByText(/transform your long urls into powerful/i)
    expect(subheadline).toBeInTheDocument()
    // Check it contains benefit keywords
    expect(subheadline).toHaveTextContent(/trackable short links/i)
    expect(subheadline).toHaveTextContent(/analytics/i)
  })
})

// Test Case 4: Click 'Get Started Free' button navigates to /register
describe('HeroSection Integration Tests - Primary CTA', () => {
  it('navigates to /register when "Get Started Free" link is clicked', async () => {
    const user = userEvent.setup()

    render(
      <MemoryRouter initialEntries={['/']}>
        <AppRoutes />
      </MemoryRouter>
    )

    const getStartedLink = screen.getByRole('link', { name: /get started free/i })
    expect(getStartedLink).toBeInTheDocument()
    expect(getStartedLink).toHaveAttribute('href', '/register')

    await user.click(getStartedLink)

    // Verify navigation to register page
    expect(screen.getByRole('heading', { name: /create account/i })).toBeInTheDocument()
  })
})

// Test Case 5: Click 'Sign In' button navigates to /login
describe('HeroSection Integration Tests - Secondary CTA', () => {
  it('navigates to /login when "Sign In" link is clicked', async () => {
    const user = userEvent.setup()

    render(
      <MemoryRouter initialEntries={['/']}>
        <AppRoutes />
      </MemoryRouter>
    )

    const signInLink = screen.getByRole('link', { name: /sign in/i })
    expect(signInLink).toBeInTheDocument()
    expect(signInLink).toHaveAttribute('href', '/login')

    await user.click(signInLink)

    // Verify navigation to login page
    expect(screen.getByRole('heading', { name: /sign in/i })).toBeInTheDocument()
  })
})
