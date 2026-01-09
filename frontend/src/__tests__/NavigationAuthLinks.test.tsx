import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import NavigationHeader from '../components/NavigationHeader'
import HeroSection from '../components/HeroSection'

const renderWithRouter = (ui: React.ReactElement, { route = '/' } = {}) => {
  return render(<MemoryRouter initialEntries={[route]}>{ui}</MemoryRouter>)
}

describe('Navigation Authentication Links - Unit Tests', () => {
  // Test Case 5: Verify Sign Up button has correct href or onClick
  describe('Sign Up button routes to /register path', () => {
    it('Sign Up button in header has href="/register"', () => {
      renderWithRouter(<NavigationHeader />)

      const signUpButton = screen.getByTestId('nav-signup')
      expect(signUpButton).toBeInTheDocument()
      expect(signUpButton).toHaveAttribute('href', '/register')
    })

    it('Sign Up button in header is a link element', () => {
      renderWithRouter(<NavigationHeader />)

      const signUpButton = screen.getByTestId('nav-signup')
      expect(signUpButton.tagName).toBe('A')
    })

    it('Sign Up button has accessible text "Sign Up"', () => {
      renderWithRouter(<NavigationHeader />)

      const signUpButton = screen.getByTestId('nav-signup')
      expect(signUpButton).toHaveTextContent('Sign Up')
    })

    it('Sign Up button in mobile menu has href="/register"', () => {
      renderWithRouter(<NavigationHeader />)

      const mobileSignUpButton = screen.getByTestId('mobile-nav-signup')
      expect(mobileSignUpButton).toBeInTheDocument()
      expect(mobileSignUpButton).toHaveAttribute('href', '/register')
    })

    it('Get Started Free button in hero has href="/register"', () => {
      renderWithRouter(<HeroSection />)

      const getStartedButton = screen.getByTestId('hero-cta-primary')
      expect(getStartedButton).toBeInTheDocument()
      expect(getStartedButton).toHaveAttribute('href', '/register')
    })

    it('Get Started Free button has correct text', () => {
      renderWithRouter(<HeroSection />)

      const getStartedButton = screen.getByTestId('hero-cta-primary')
      expect(getStartedButton).toHaveTextContent('Get Started Free')
    })
  })

  // Test Case 6: Verify Log In button has correct href or onClick
  describe('Log In button routes to /login path', () => {
    it('Log In button in header has href="/login"', () => {
      renderWithRouter(<NavigationHeader />)

      const loginButton = screen.getByTestId('nav-login')
      expect(loginButton).toBeInTheDocument()
      expect(loginButton).toHaveAttribute('href', '/login')
    })

    it('Log In button in header is a link element', () => {
      renderWithRouter(<NavigationHeader />)

      const loginButton = screen.getByTestId('nav-login')
      expect(loginButton.tagName).toBe('A')
    })

    it('Log In button has accessible text "Log In"', () => {
      renderWithRouter(<NavigationHeader />)

      const loginButton = screen.getByTestId('nav-login')
      expect(loginButton).toHaveTextContent('Log In')
    })

    it('Log In button in mobile menu has href="/login"', () => {
      renderWithRouter(<NavigationHeader />)

      const mobileLoginButton = screen.getByTestId('mobile-nav-login')
      expect(mobileLoginButton).toBeInTheDocument()
      expect(mobileLoginButton).toHaveAttribute('href', '/login')
    })

    it('Log In button in hero has href="/login"', () => {
      renderWithRouter(<HeroSection />)

      const loginButton = screen.getByTestId('hero-cta-secondary')
      expect(loginButton).toBeInTheDocument()
      expect(loginButton).toHaveAttribute('href', '/login')
    })

    it('Log In button in hero has correct text', () => {
      renderWithRouter(<HeroSection />)

      const loginButton = screen.getByTestId('hero-cta-secondary')
      expect(loginButton).toHaveTextContent('Log In')
    })
  })

  // Additional unit tests for navigation accessibility
  describe('Navigation links accessibility', () => {
    it('all authentication links are keyboard accessible', () => {
      renderWithRouter(<NavigationHeader />)

      const signUpButton = screen.getByTestId('nav-signup')
      const loginButton = screen.getByTestId('nav-login')

      // Links should be focusable (A tags are focusable by default)
      expect(signUpButton.tagName).toBe('A')
      expect(loginButton.tagName).toBe('A')
    })

    it('hero CTA buttons are links for proper navigation', () => {
      renderWithRouter(<HeroSection />)

      const primaryCta = screen.getByTestId('hero-cta-primary')
      const secondaryCta = screen.getByTestId('hero-cta-secondary')

      // Both should be anchor elements for proper navigation
      expect(primaryCta.tagName).toBe('A')
      expect(secondaryCta.tagName).toBe('A')
    })
  })
})
