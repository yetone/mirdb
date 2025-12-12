import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { Homepage } from './Homepage'
import { AuthProvider } from '../context'
import type { FeaturedItem } from '../types/FeaturedContent.types'

/**
 * Integration tests for Homepage Component Assembly
 * Scenario: Component Integration - Homepage Assembly
 *
 * These tests verify that all homepage components (Navigation, Hero, Content, Footer)
 * integrate correctly and work together as expected.
 */

// Helper function to render Homepage with required providers
const renderHomepage = (props = {}, options: { initialUser?: { id: string; name: string; email: string } | null } = {}) => {
  return render(
    <BrowserRouter>
      <AuthProvider initialUser={options.initialUser}>
        <Homepage {...props} />
      </AuthProvider>
    </BrowserRouter>
  )
}

describe('Homepage Component Integration', () => {
  // Test Case 1: Render complete Homepage component
  describe('TC1: All child components render without errors', () => {
    it('renders the complete Homepage component with all sections', () => {
      renderHomepage()

      // Verify Homepage container is rendered
      expect(screen.getByTestId('homepage')).toBeInTheDocument()

      // Verify Navigation is rendered
      expect(screen.getByTestId('main-navigation')).toBeInTheDocument()
      expect(screen.getByTestId('navigation')).toBeInTheDocument()

      // Verify Hero is rendered
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('hero-headline')).toBeInTheDocument()
      expect(screen.getByTestId('hero-subheadline')).toBeInTheDocument()
      expect(screen.getByTestId('hero-cta')).toBeInTheDocument()

      // Verify FeaturedContent is rendered
      expect(screen.getByTestId('featured-content-section')).toBeInTheDocument()

      // Verify Footer is rendered
      expect(screen.getByTestId('footer')).toBeInTheDocument()
      expect(screen.getByTestId('footer-content')).toBeInTheDocument()
    })

    it('renders all components in the DOM tree simultaneously', () => {
      const { container } = renderHomepage()

      // All main sections should exist in a single render
      const navigation = container.querySelector('[data-testid="main-navigation"]')
      const hero = container.querySelector('[data-testid="hero-section"]')
      const featuredContent = container.querySelector('[data-testid="featured-content-section"]')
      const footer = container.querySelector('[data-testid="footer"]')

      expect(navigation).not.toBeNull()
      expect(hero).not.toBeNull()
      expect(featuredContent).not.toBeNull()
      expect(footer).not.toBeNull()
    })

    it('renders without throwing any errors', () => {
      expect(() => renderHomepage()).not.toThrow()
    })

    it('renders with custom props without errors', () => {
      const customProps = {
        headline: 'Custom Headline',
        subheadline: 'Custom subheadline text',
        ctaText: 'Click Me',
        ctaHref: '/custom-action',
      }

      expect(() => renderHomepage(customProps)).not.toThrow()
      expect(screen.getByText('Custom Headline')).toBeInTheDocument()
    })
  })

  // Test Case 2: Check component rendering order
  describe('TC2: Components appear in correct visual order: Nav, Hero, Content, Footer', () => {
    it('renders components in correct DOM order', () => {
      const { container } = renderHomepage()

      const homepage = container.querySelector('[data-testid="homepage"]')
      expect(homepage).not.toBeNull()

      const children = homepage!.children
      const childElements = Array.from(children)

      // Expected order: header (navigation), main (hero + content), footer
      // Navigation should be first (header element)
      const firstChild = childElements[0]
      expect(firstChild.querySelector('[data-testid="navigation"]')).not.toBeNull()

      // Main content should be second (contains hero and featured content)
      const mainContent = childElements[1]
      expect(mainContent.getAttribute('data-testid')).toBe('homepage-main')

      // Footer should be last
      const lastChild = childElements[childElements.length - 1]
      expect(lastChild.getAttribute('data-testid')).toBe('footer')
    })

    it('renders Hero before FeaturedContent within main', () => {
      const { container } = renderHomepage()

      const main = container.querySelector('[data-testid="homepage-main"]')
      expect(main).not.toBeNull()

      const mainChildren = Array.from(main!.children)

      // Find indices of hero and featured content
      const heroIndex = mainChildren.findIndex(el =>
        el.getAttribute('data-testid') === 'hero-section' ||
        el.getAttribute('role') === 'banner'
      )
      const featuredIndex = mainChildren.findIndex(el =>
        el.getAttribute('data-testid') === 'featured-content-section'
      )

      expect(heroIndex).toBeGreaterThanOrEqual(0)
      expect(featuredIndex).toBeGreaterThanOrEqual(0)
      expect(heroIndex).toBeLessThan(featuredIndex)
    })

    it('Navigation appears at the top of the page structure', () => {
      renderHomepage()

      // Navigation should be in a header element at the top
      const header = screen.getByTestId('main-navigation')
      expect(header.tagName.toLowerCase()).toBe('header')
    })

    it('Footer appears at the bottom of the page structure', () => {
      renderHomepage()

      // Footer should be a footer element (contentinfo role)
      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
      expect(footer.tagName.toLowerCase()).toBe('footer')
    })
  })

  // Test Case 3: Test state sharing between components
  describe('TC3: Auth state is correctly shared between Nav and content sections', () => {
    const authenticatedUser = {
      id: 'user-123',
      name: 'John Doe',
      email: 'john@example.com',
    }

    it('Navigation shows unauthenticated state when no user', () => {
      renderHomepage()

      // Should not show auth-specific links (Dashboard, Account)
      expect(screen.queryByTestId('nav-link-auth')).not.toBeInTheDocument()

      // Should show regular navigation links
      expect(screen.getByTestId('navigation-link-home')).toBeInTheDocument()
      expect(screen.getByTestId('navigation-link-features')).toBeInTheDocument()
    })

    it('Navigation shows authenticated links when user is logged in', () => {
      renderHomepage({}, { initialUser: authenticatedUser })

      // Should show auth-specific links
      const authLinks = screen.getAllByTestId('nav-link-auth')
      expect(authLinks.length).toBeGreaterThan(0)
    })

    it('Hero shows personalized content for authenticated users', () => {
      renderHomepage({}, { initialUser: authenticatedUser })

      // Hero should show personalized greeting
      expect(screen.getByText(/Welcome back, John Doe!/i)).toBeInTheDocument()

      // Should show authenticated CTA
      expect(screen.getByText(/Go to Dashboard/i)).toBeInTheDocument()
    })

    it('Hero shows default content for unauthenticated users', () => {
      renderHomepage()

      // Hero should show default greeting
      expect(screen.getByText(/Welcome to MirDB/i)).toBeInTheDocument()

      // Should show default CTA
      expect(screen.getByText(/Get Started/i)).toBeInTheDocument()
    })

    it('Auth state is consistent across Navigation and Hero', () => {
      // Test unauthenticated state consistency
      renderHomepage()

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveAttribute('data-authenticated', 'false')
      expect(screen.queryByTestId('nav-link-auth')).not.toBeInTheDocument()
    })

    it('Auth state is consistent when authenticated', () => {
      renderHomepage({}, { initialUser: authenticatedUser })

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveAttribute('data-authenticated', 'true')
      expect(screen.getAllByTestId('nav-link-auth').length).toBeGreaterThan(0)
    })

    it('Footer remains consistent regardless of auth state', () => {
      // Unauthenticated
      const { unmount: unmount1 } = renderHomepage()
      const footerUnauth = screen.getByTestId('footer')
      expect(footerUnauth).toBeInTheDocument()
      const footerContent1 = footerUnauth.textContent
      unmount1()

      // Authenticated
      renderHomepage({}, { initialUser: authenticatedUser })
      const footerAuth = screen.getByTestId('footer')
      expect(footerAuth).toBeInTheDocument()
      const footerContent2 = footerAuth.textContent

      // Footer content should be the same regardless of auth state
      expect(footerContent1).toBe(footerContent2)
    })
  })

  // Test Case 4: Verify no console errors on page load
  describe('TC4: No JavaScript errors in browser console', () => {
    let consoleErrorSpy: ReturnType<typeof vi.spyOn>
    let consoleWarnSpy: ReturnType<typeof vi.spyOn>

    beforeEach(() => {
      consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
      consoleWarnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
    })

    afterEach(() => {
      consoleErrorSpy.mockRestore()
      consoleWarnSpy.mockRestore()
    })

    it('renders without console errors', () => {
      renderHomepage()

      // Check that console.error was not called
      expect(consoleErrorSpy).not.toHaveBeenCalled()
    })

    it('renders without console warnings', () => {
      renderHomepage()

      // Check that console.warn was not called (excluding known React warnings)
      const warningCalls = consoleWarnSpy.mock.calls.filter(
        call => !call[0]?.includes?.('React') // Filter out React dev mode warnings
      )
      expect(warningCalls).toHaveLength(0)
    })

    it('renders with authenticated user without errors', () => {
      renderHomepage({}, {
        initialUser: { id: '1', name: 'Test User', email: 'test@example.com' },
      })

      expect(consoleErrorSpy).not.toHaveBeenCalled()
    })

    it('renders with custom featured items without errors', () => {
      const customItems: FeaturedItem[] = [
        {
          id: 'custom-1',
          title: 'Custom Feature',
          description: 'Custom description',
          link: '/custom',
        },
      ]

      renderHomepage({ featuredItems: customItems })

      expect(consoleErrorSpy).not.toHaveBeenCalled()
    })

    it('renders with empty featured items without errors', () => {
      renderHomepage({ featuredItems: [] })

      expect(consoleErrorSpy).not.toHaveBeenCalled()
      // Should show empty state message
      expect(screen.getByTestId('featured-content-empty')).toBeInTheDocument()
    })
  })

  // Additional integration tests
  describe('Component interactions and accessibility', () => {
    it('all navigation links are functional', () => {
      renderHomepage()

      const homeLink = screen.getByTestId('navigation-link-home')
      const featuresLink = screen.getByTestId('navigation-link-features')
      const aboutLink = screen.getByTestId('navigation-link-about')
      const contactLink = screen.getByTestId('navigation-link-contact')

      expect(homeLink).toHaveAttribute('href', '/')
      expect(featuresLink).toHaveAttribute('href', '/features')
      expect(aboutLink).toHaveAttribute('href', '/about')
      expect(contactLink).toHaveAttribute('href', '/contact')
    })

    it('hero CTA link is properly configured', () => {
      renderHomepage()

      const ctaLink = screen.getByTestId('hero-cta')
      expect(ctaLink).toHaveAttribute('href', '#getting-started')
    })

    it('all sections have proper accessibility landmarks', () => {
      renderHomepage()

      // Navigation should be in header with nav element
      expect(screen.getByRole('navigation', { name: /main navigation/i })).toBeInTheDocument()

      // Hero section should have banner role (using testid since navigation header is also a banner)
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveAttribute('role', 'banner')

      // Footer should have contentinfo role
      expect(screen.getByRole('contentinfo')).toBeInTheDocument()
    })

    it('featured content section has proper structure', () => {
      renderHomepage()

      const featuredSection = screen.getByTestId('featured-content-section')
      expect(featuredSection).toBeInTheDocument()

      // Should have a title
      expect(within(featuredSection).getByText('Featured')).toBeInTheDocument()
    })

    it('footer contains expected sections', () => {
      renderHomepage()

      const footer = screen.getByTestId('footer-content')

      // Check for footer sections
      expect(within(footer).getByText('Site Map')).toBeInTheDocument()
      expect(within(footer).getByText('Legal')).toBeInTheDocument()
      expect(within(footer).getByText('Follow Us')).toBeInTheDocument()
      expect(within(footer).getByText('Contact Us')).toBeInTheDocument()
    })
  })
})
