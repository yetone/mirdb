import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { Navigation, AUTH_NAV_LINKS } from './Navigation'
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

describe('Navigation Component - Authenticated User (REQ-7, US-4)', () => {
  describe('TC2: Verify navigation for authenticated user', () => {
    it('should display Dashboard link when user is authenticated', () => {
      const authenticatedUser: User = {
        id: '123',
        name: 'John Doe',
        email: 'john@example.com'
      }

      renderWithProviders(<Navigation />, { initialUser: authenticatedUser })

      const dashboardLink = screen.getByRole('link', { name: 'Dashboard' })
      expect(dashboardLink).toBeInTheDocument()
      expect(dashboardLink).toHaveAttribute('href', '/dashboard')
    })

    it('should display Account link when user is authenticated', () => {
      const authenticatedUser: User = {
        id: '123',
        name: 'John Doe',
        email: 'john@example.com'
      }

      renderWithProviders(<Navigation />, { initialUser: authenticatedUser })

      const accountLink = screen.getByRole('link', { name: 'Account' })
      expect(accountLink).toBeInTheDocument()
      expect(accountLink).toHaveAttribute('href', '/account')
    })

    it('should NOT display Dashboard or Account links when user is unauthenticated', () => {
      renderWithProviders(<Navigation />)

      expect(screen.queryByRole('link', { name: 'Dashboard' })).not.toBeInTheDocument()
      expect(screen.queryByRole('link', { name: 'Account' })).not.toBeInTheDocument()
    })

    it('should display all auth navigation links for authenticated user', () => {
      const authenticatedUser: User = {
        id: '123',
        name: 'John Doe',
        email: 'john@example.com'
      }

      renderWithProviders(<Navigation />, { initialUser: authenticatedUser })

      AUTH_NAV_LINKS.forEach((link) => {
        const navLink = screen.getByRole('link', { name: link.label })
        expect(navLink).toBeInTheDocument()
        expect(navLink).toHaveAttribute('href', link.href)
      })
    })

    it('should render auth links with proper test id', () => {
      const authenticatedUser: User = {
        id: '123',
        name: 'John Doe',
        email: 'john@example.com'
      }

      renderWithProviders(<Navigation />, { initialUser: authenticatedUser })

      const authLinks = screen.getAllByTestId('nav-link-auth')
      expect(authLinks).toHaveLength(AUTH_NAV_LINKS.length)
    })

    it('should allow hiding auth links via prop when authenticated', () => {
      const authenticatedUser: User = {
        id: '123',
        name: 'John Doe',
        email: 'john@example.com'
      }

      renderWithProviders(
        <Navigation showAuthLinks={false} />,
        { initialUser: authenticatedUser }
      )

      expect(screen.queryByRole('link', { name: 'Dashboard' })).not.toBeInTheDocument()
      expect(screen.queryByRole('link', { name: 'Account' })).not.toBeInTheDocument()
    })
  })

  describe('Dashboard or account link is visible in navigation', () => {
    it('should have visible Dashboard link in navigation for authenticated users', () => {
      const authenticatedUser: User = {
        id: '123',
        name: 'Jane Doe',
        email: 'jane@example.com'
      }

      renderWithProviders(<Navigation />, { initialUser: authenticatedUser })

      const dashboardLink = screen.getByRole('link', { name: 'Dashboard' })
      expect(dashboardLink).toBeVisible()
    })

    it('should have visible Account link in navigation for authenticated users', () => {
      const authenticatedUser: User = {
        id: '123',
        name: 'Jane Doe',
        email: 'jane@example.com'
      }

      renderWithProviders(<Navigation />, { initialUser: authenticatedUser })

      const accountLink = screen.getByRole('link', { name: 'Account' })
      expect(accountLink).toBeVisible()
    })

    it('should maintain regular nav links alongside auth links', () => {
      const authenticatedUser: User = {
        id: '123',
        name: 'Jane Doe',
        email: 'jane@example.com'
      }

      renderWithProviders(<Navigation />, { initialUser: authenticatedUser })

      // Regular links should still be present
      expect(screen.getByRole('link', { name: 'Home' })).toBeInTheDocument()
      expect(screen.getByRole('link', { name: 'Features' })).toBeInTheDocument()
      expect(screen.getByRole('link', { name: 'About' })).toBeInTheDocument()
      expect(screen.getByRole('link', { name: 'Contact' })).toBeInTheDocument()

      // Auth links should also be present
      expect(screen.getByRole('link', { name: 'Dashboard' })).toBeInTheDocument()
      expect(screen.getByRole('link', { name: 'Account' })).toBeInTheDocument()
    })
  })
})
