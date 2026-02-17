/**
 * Component Integration tests for Navbar.
 * Scenario 13 - Component Integration
 *
 * Tests:
 * - Global Navbar component is included at top of page
 * - Navbar integrates with theme context
 * - Navigation links work correctly
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { Navbar } from '../../../../src/components/layout/Navbar'
import { Home } from '../../../../src/pages/Home'
import { ThemeProvider } from '../../../../src/contexts/ThemeContext'

// Helper to wrap components with necessary providers
function renderWithProviders(ui: React.ReactElement, { route = '/' } = {}) {
  return render(
    <MemoryRouter initialEntries={[route]}>
      <ThemeProvider>{ui}</ThemeProvider>
    </MemoryRouter>
  )
}

describe('Navbar Component', () => {
  it('renders navigation element', () => {
    renderWithProviders(<Navbar />)

    expect(screen.getByRole('navigation')).toBeInTheDocument()
  })

  it('displays brand/logo link', () => {
    renderWithProviders(<Navbar />)

    expect(screen.getByText('URLShort')).toBeInTheDocument()
  })

  it('includes navigation links', () => {
    renderWithProviders(<Navbar />)

    // Desktop navigation links
    const homeLinks = screen.getAllByText('Home')
    expect(homeLinks.length).toBeGreaterThan(0)

    const loginLinks = screen.getAllByText('Login')
    expect(loginLinks.length).toBeGreaterThan(0)

    const registerLinks = screen.getAllByText('Register')
    expect(registerLinks.length).toBeGreaterThan(0)
  })

  it('includes theme selector', () => {
    renderWithProviders(<Navbar />)

    const themeSelect = screen.getByRole('combobox', { name: /select theme/i })
    expect(themeSelect).toBeInTheDocument()
  })

  it('has sticky positioning', () => {
    renderWithProviders(<Navbar />)

    const nav = screen.getByRole('navigation')
    expect(nav).toHaveClass('sticky')
    expect(nav).toHaveClass('top-0')
    expect(nav).toHaveClass('z-50')
  })

  it('has glassmorphism-style backdrop', () => {
    renderWithProviders(<Navbar />)

    const nav = screen.getByRole('navigation')
    expect(nav).toHaveClass('backdrop-blur-md')
    expect(nav).toHaveClass('bg-base-100/80')
  })
})

describe('Navbar Integration - Home Page', () => {
  it('Global Navbar component is included at top of page', () => {
    const { container } = renderWithProviders(<Home />)

    // Navbar should be present - it has the 'navbar' class
    const navbar = container.querySelector('nav.navbar')
    expect(navbar).toBeInTheDocument()

    // Should contain brand
    expect(screen.getByText('URLShort')).toBeInTheDocument()

    // Should contain theme selector
    const themeSelect = screen.getByRole('combobox', { name: /select theme/i })
    expect(themeSelect).toBeInTheDocument()
  })

  it('Navbar is positioned at the top of the page structure', () => {
    const { container } = renderWithProviders(<Home />)

    // Get the main wrapper div
    const mainWrapper = container.querySelector('.min-h-screen')
    expect(mainWrapper).toBeInTheDocument()

    // Navbar should be rendered early in the DOM (after BackgroundEffect)
    const navbar = container.querySelector('nav.navbar')
    const main = container.querySelector('main')

    // Navbar should come before main content
    if (navbar && main) {
      const navPosition = navbar.compareDocumentPosition(main)
      // DOCUMENT_POSITION_FOLLOWING (4) means main comes after nav
      expect(navPosition & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    }
  })

  it('Navbar navigation links point to correct routes', () => {
    renderWithProviders(<Home />)

    // Get all home links (desktop and mobile menu)
    const homeLinks = screen.getAllByRole('link', { name: /home/i })
    expect(homeLinks.some((link) => link.getAttribute('href') === '/')).toBe(true)

    // Get all login links
    const loginLinks = screen.getAllByRole('link', { name: /login/i })
    expect(loginLinks.some((link) => link.getAttribute('href') === '/login')).toBe(true)

    // Get all register links
    const registerLinks = screen.getAllByRole('link', { name: /register/i })
    expect(registerLinks.some((link) => link.getAttribute('href') === '/register')).toBe(true)
  })

  it('Navbar has consistent styling with authenticated pages', () => {
    const { container } = renderWithProviders(<Home />)

    // Navbar should use DaisyUI navbar classes
    const navbar = container.querySelector('nav.navbar')
    expect(navbar).toBeInTheDocument()
    expect(navbar).toHaveClass('navbar')
    expect(navbar).toHaveClass('bg-base-100/80')

    // Should have border styling
    expect(navbar).toHaveClass('border-b')
    expect(navbar).toHaveClass('border-base-content/10')
  })
})
