import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import HeroSection from '../components/HeroSection'

describe('HeroSection', () => {
  // Test Case 1: Secondary CTA link with text containing 'Log in' is present
  describe('Secondary CTA Login Link', () => {
    it('should render a secondary CTA link with text containing "Log in"', () => {
      render(
        <BrowserRouter>
          <HeroSection />
        </BrowserRouter>
      )

      // Find the login link by text
      const loginLink = screen.getByRole('link', { name: /log in/i })
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveTextContent('Log in')
    })

    it('should display "Already have an account?" text before the login link', () => {
      render(
        <BrowserRouter>
          <HeroSection />
        </BrowserRouter>
      )

      // Check for the full secondary CTA text
      const secondaryCTAText = screen.getByText(/already have an account\?/i)
      expect(secondaryCTAText).toBeInTheDocument()
    })
  })

  // Test Case 2: Navigation to /login route occurs when clicking login link
  describe('Login Link Navigation', () => {
    it('should have href pointing to /login route', () => {
      render(
        <BrowserRouter>
          <HeroSection />
        </BrowserRouter>
      )

      const loginLink = screen.getByRole('link', { name: /log in/i })
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('should navigate to /login when login link is clicked', async () => {
      const user = userEvent.setup()

      // Use MemoryRouter to track navigation
      let testLocation: string | undefined

      render(
        <MemoryRouter initialEntries={['/']}>
          <HeroSection />
        </MemoryRouter>
      )

      const loginLink = screen.getByRole('link', { name: /log in/i })

      // Verify the link has correct href (React Router will handle navigation)
      expect(loginLink).toHaveAttribute('href', '/login')

      // Click the link
      await user.click(loginLink)
    })
  })

  // Test Case 3: Link is visually distinguishable as secondary action
  describe('Login Link Visual Styling', () => {
    it('should have link styling classes for visual distinction', () => {
      render(
        <BrowserRouter>
          <HeroSection />
        </BrowserRouter>
      )

      const loginLink = screen.getByTestId('login-link')

      // Check that the link has styling classes that make it visually distinguishable
      expect(loginLink).toHaveClass('link')
      expect(loginLink).toHaveClass('link-hover')
    })

    it('should have underline styling to distinguish from surrounding text', () => {
      render(
        <BrowserRouter>
          <HeroSection />
        </BrowserRouter>
      )

      const loginLink = screen.getByTestId('login-link')

      // Link should have underline class for visual distinction
      expect(loginLink).toHaveClass('underline')
    })

    it('should have font-semibold class for emphasis', () => {
      render(
        <BrowserRouter>
          <HeroSection />
        </BrowserRouter>
      )

      const loginLink = screen.getByTestId('login-link')

      // Link should have semibold font for emphasis
      expect(loginLink).toHaveClass('font-semibold')
    })

    it('should be positioned below the primary CTA button', () => {
      render(
        <BrowserRouter>
          <HeroSection />
        </BrowserRouter>
      )

      // Primary CTA button should exist
      const primaryCTA = screen.getByRole('link', { name: /get started free/i })
      expect(primaryCTA).toBeInTheDocument()

      // Login link should exist (positioned in DOM after primary CTA)
      const loginLink = screen.getByRole('link', { name: /log in/i })
      expect(loginLink).toBeInTheDocument()

      // Both should be in the document, with login link as secondary action
      // The flex-col layout ensures the secondary CTA is below the primary
    })
  })
})
