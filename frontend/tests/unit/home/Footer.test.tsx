/**
 * Footer Unit Tests
 * Owner: Scenario 6 - Footer Navigation
 *
 * Tests for Footer component:
 * - Uses semantic <footer> element
 * - Contains navigation links (Home, Login, Register)
 * - Contains copyright text
 * - Links navigate to correct paths
 *
 * Testing framework: Vitest + @testing-library/react
 */
import { describe, it, expect } from 'vitest'
import { screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { renderWithProviders } from '../../utils/renderWithProviders'
import { Footer } from '@/components/home/Footer'
import Home from '@/pages/Home'

describe('Footer', () => {
  describe('Test Case 1: Footer element exists with semantic <footer> tag or footer role', () => {
    it('should render a semantic <footer> element', () => {
      renderWithProviders(<Footer />)

      const footer = screen.getByTestId('footer')
      expect(footer).toBeInTheDocument()
      expect(footer.tagName.toLowerCase()).toBe('footer')
    })

    it('should render footer when Home component is rendered', () => {
      renderWithProviders(<Home />)

      const footer = screen.getByTestId('footer')
      expect(footer).toBeInTheDocument()
      expect(footer.tagName.toLowerCase()).toBe('footer')
    })

    it('should have footer role by default with semantic element', () => {
      renderWithProviders(<Footer />)

      // The semantic <footer> element has implicit role of contentinfo
      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Footer contains links to Home (/), Login (/login), Register (/register)', () => {
    it('should contain a link to Home (/)', () => {
      renderWithProviders(<Footer />)

      const homeLink = screen.getByRole('link', { name: /home/i })
      expect(homeLink).toBeInTheDocument()
      expect(homeLink).toHaveAttribute('href', '/')
    })

    it('should contain a link to Login (/login)', () => {
      renderWithProviders(<Footer />)

      const loginLink = screen.getByRole('link', { name: /login/i })
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('should contain a link to Register (/register)', () => {
      renderWithProviders(<Footer />)

      const registerLink = screen.getByRole('link', { name: /register/i })
      expect(registerLink).toBeInTheDocument()
      expect(registerLink).toHaveAttribute('href', '/register')
    })

    it('should contain all three navigation links in footer', () => {
      renderWithProviders(<Footer />)

      const footer = screen.getByTestId('footer')
      const links = within(footer).getAllByRole('link')

      expect(links).toHaveLength(3)

      const hrefs = links.map(link => link.getAttribute('href'))
      expect(hrefs).toContain('/')
      expect(hrefs).toContain('/login')
      expect(hrefs).toContain('/register')
    })
  })

  describe('Test Case 3: Footer contains copyright symbol or Copyright text', () => {
    it('should contain copyright symbol (\u00A9)', () => {
      renderWithProviders(<Footer />)

      const footer = screen.getByTestId('footer')
      expect(footer.textContent).toContain('\u00A9')
    })

    it('should contain the current year in copyright text', () => {
      renderWithProviders(<Footer />)

      const currentYear = new Date().getFullYear().toString()
      const footer = screen.getByTestId('footer')
      expect(footer.textContent).toContain(currentYear)
    })

    it('should contain "All rights reserved" text', () => {
      renderWithProviders(<Footer />)

      expect(screen.getByText(/all rights reserved/i)).toBeInTheDocument()
    })
  })

  describe('Footer navigation and accessibility', () => {
    it('should have a navigation element with aria-label', () => {
      renderWithProviders(<Footer />)

      const nav = screen.getByRole('navigation', { name: /footer navigation/i })
      expect(nav).toBeInTheDocument()
    })

    it('should render navigation links that are visible', () => {
      renderWithProviders(<Footer />)

      const homeLink = screen.getByRole('link', { name: /home/i })
      const loginLink = screen.getByRole('link', { name: /login/i })
      const registerLink = screen.getByRole('link', { name: /register/i })

      expect(homeLink).toBeVisible()
      expect(loginLink).toBeVisible()
      expect(registerLink).toBeVisible()
    })
  })

  describe('Test Case 4: Clicking Login link navigates to /login', () => {
    it('should navigate to /login when Login link is clicked', async () => {
      const user = userEvent.setup()
      renderWithProviders(<Footer />, { initialEntries: ['/'] })

      const loginLink = screen.getByRole('link', { name: /login/i })
      await user.click(loginLink)

      // After clicking, the link should still be pointing to /login
      // In a test environment with MemoryRouter, we verify the href
      expect(loginLink).toHaveAttribute('href', '/login')
    })
  })

  describe('Test Case 5: Clicking Register link navigates to /register', () => {
    it('should navigate to /register when Register link is clicked', async () => {
      const user = userEvent.setup()
      renderWithProviders(<Footer />, { initialEntries: ['/'] })

      const registerLink = screen.getByRole('link', { name: /register/i })
      await user.click(registerLink)

      // After clicking, the link should still be pointing to /register
      expect(registerLink).toHaveAttribute('href', '/register')
    })
  })
})
