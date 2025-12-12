import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { Hero } from './Hero'
import { AuthProvider, type User } from '../../context'

// Helper to render with router and auth context
const renderWithProviders = (
  ui: React.ReactElement,
  { initialUser }: { initialUser?: User | null } = {}
) => {
  return render(
    <MemoryRouter>
      <AuthProvider initialUser={initialUser}>
        {ui}
      </AuthProvider>
    </MemoryRouter>
  )
}

describe('Hero Component - Authenticated User Messaging (REQ-7, US-4)', () => {
  describe('TC3: Render homepage component with authenticated user context', () => {
    it('should display personalized greeting when user is authenticated', () => {
      const authenticatedUser: User = {
        id: '123',
        name: 'John Doe',
        email: 'john@example.com'
      }

      renderWithProviders(<Hero />, { initialUser: authenticatedUser })

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toHaveTextContent('Welcome back, John Doe!')
    })

    it('should display generic welcome back message when user has no name', () => {
      const authenticatedUser: User = {
        id: '123',
        name: '',
        email: 'user@example.com'
      }

      renderWithProviders(<Hero />, { initialUser: authenticatedUser })

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toHaveTextContent('Welcome back!')
    })

    it('should display default welcome message when user is not authenticated', () => {
      renderWithProviders(<Hero />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toHaveTextContent('Welcome to MirDB')
    })

    it('should display authenticated subheadline when user is authenticated', () => {
      const authenticatedUser: User = {
        id: '123',
        name: 'John Doe',
        email: 'john@example.com'
      }

      renderWithProviders(<Hero />, { initialUser: authenticatedUser })

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toHaveTextContent('Pick up where you left off')
    })

    it('should display dashboard CTA when user is authenticated', () => {
      const authenticatedUser: User = {
        id: '123',
        name: 'John Doe',
        email: 'john@example.com'
      }

      renderWithProviders(<Hero />, { initialUser: authenticatedUser })

      const cta = screen.getByTestId('hero-cta')
      expect(cta).toHaveTextContent('Go to Dashboard')
      expect(cta).toHaveAttribute('href', '/dashboard')
    })

    it('should display "Get Started" CTA when user is not authenticated', () => {
      renderWithProviders(<Hero />)

      const cta = screen.getByTestId('hero-cta')
      expect(cta).toHaveTextContent('Get Started')
      expect(cta).toHaveAttribute('href', '#getting-started')
    })

    it('should set data-authenticated attribute based on auth state', () => {
      const authenticatedUser: User = {
        id: '123',
        name: 'John Doe',
        email: 'john@example.com'
      }

      renderWithProviders(<Hero />, { initialUser: authenticatedUser })

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveAttribute('data-authenticated', 'true')
    })

    it('should allow custom authenticated headline override', () => {
      const authenticatedUser: User = {
        id: '123',
        name: 'John Doe',
        email: 'john@example.com'
      }

      renderWithProviders(
        <Hero authenticatedHeadline="Custom Welcome!" />,
        { initialUser: authenticatedUser }
      )

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toHaveTextContent('Custom Welcome!')
    })

    it('should allow custom authenticated subheadline override', () => {
      const authenticatedUser: User = {
        id: '123',
        name: 'John Doe',
        email: 'john@example.com'
      }

      renderWithProviders(
        <Hero authenticatedSubheadline="Custom subheadline text" />,
        { initialUser: authenticatedUser }
      )

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toHaveTextContent('Custom subheadline text')
    })
  })

  describe('Component displays authenticated user variant', () => {
    it('should render hero section as authenticated variant', () => {
      const authenticatedUser: User = {
        id: '123',
        name: 'Test User',
        email: 'test@example.com'
      }

      renderWithProviders(<Hero />, { initialUser: authenticatedUser })

      // Verify all authenticated elements are present
      expect(screen.getByTestId('hero-section')).toHaveAttribute('data-authenticated', 'true')
      expect(screen.getByTestId('hero-headline')).toHaveTextContent(/Welcome back/)
      expect(screen.getByTestId('hero-subheadline')).toHaveTextContent(/dashboard/i)
      expect(screen.getByTestId('hero-cta')).toHaveAttribute('href', '/dashboard')
    })
  })
})
