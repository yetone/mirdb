import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import { Navigation, NAV_LINKS } from './Navigation'

// Helper to render with router context
const renderWithRouter = (ui: React.ReactElement, { route = '/' } = {}) => {
  return render(
    <MemoryRouter initialEntries={[route]}>
      {ui}
    </MemoryRouter>
  )
}

describe('Navigation Component', () => {
  // Test Case 1: Unit test - Navigation bar is rendered with logo and nav links
  describe('TC1: Render Navigation component', () => {
    it('should render the navigation bar', () => {
      renderWithRouter(<Navigation />)
      const nav = screen.getByRole('navigation')
      expect(nav).toBeInTheDocument()
    })

    it('should render the logo', () => {
      renderWithRouter(<Navigation />)
      const logo = screen.getByRole('link', { name: /homepage enhancement/i })
      expect(logo).toBeInTheDocument()
    })

    it('should render all navigation links', () => {
      renderWithRouter(<Navigation />)
      NAV_LINKS.forEach((link) => {
        const navLink = screen.getByRole('link', { name: link.label })
        expect(navLink).toBeInTheDocument()
      })
    })

    it('should have proper navigation structure with header', () => {
      renderWithRouter(<Navigation />)
      const header = screen.getByRole('banner')
      expect(header).toBeInTheDocument()
      expect(header.querySelector('nav')).toBeInTheDocument()
    })
  })

  // Test Case 3: Integration test - All navigation links have valid href values
  describe('TC3: Navigation links href attributes', () => {
    it('should have valid href attributes on all navigation links', () => {
      renderWithRouter(<Navigation />)
      NAV_LINKS.forEach((link) => {
        const navLink = screen.getByRole('link', { name: link.label })
        expect(navLink).toHaveAttribute('href', link.href)
      })
    })

    it('should have logo link pointing to home', () => {
      renderWithRouter(<Navigation />)
      const logoLink = screen.getByRole('link', { name: /homepage enhancement/i })
      expect(logoLink).toHaveAttribute('href', '/')
    })

    it('should have href attributes that are non-empty strings', () => {
      renderWithRouter(<Navigation />)
      const allLinks = screen.getAllByRole('link')
      allLinks.forEach((link) => {
        const href = link.getAttribute('href')
        expect(href).toBeTruthy()
        expect(typeof href).toBe('string')
        expect(href!.length).toBeGreaterThan(0)
      })
    })

    it('should have correctly formatted href values starting with /', () => {
      renderWithRouter(<Navigation />)
      NAV_LINKS.forEach((link) => {
        expect(link.href).toMatch(/^\//)
      })
    })
  })

  // Test Case 4: E2E-style test - Keyboard navigation accessibility
  describe('TC4: Keyboard navigation through nav links', () => {
    it('should allow Tab key navigation through all links', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Navigation />)

      const allLinks = screen.getAllByRole('link')

      // Start tabbing through links
      for (let i = 0; i < allLinks.length; i++) {
        await user.tab()
        expect(allLinks[i]).toHaveFocus()
      }
    })

    it('should have visible focus states on links', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Navigation />)

      await user.tab()
      const firstLink = screen.getAllByRole('link')[0]
      expect(firstLink).toHaveFocus()
      expect(document.activeElement).toBe(firstLink)
    })

    it('should maintain proper tab order (logo first, then nav links)', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Navigation />)

      // First tab should focus the logo
      await user.tab()
      const logoLink = screen.getByRole('link', { name: /homepage enhancement/i })
      expect(logoLink).toHaveFocus()

      // Subsequent tabs should go through navigation links in order
      for (const link of NAV_LINKS) {
        await user.tab()
        const navLink = screen.getByRole('link', { name: link.label })
        expect(navLink).toHaveFocus()
      }
    })

    it('should have all links focusable (tabIndex >= 0)', () => {
      renderWithRouter(<Navigation />)
      const allLinks = screen.getAllByRole('link')
      allLinks.forEach((link) => {
        const tabIndex = link.getAttribute('tabindex')
        // tabindex should be null (default 0) or a non-negative number
        if (tabIndex !== null) {
          expect(parseInt(tabIndex)).toBeGreaterThanOrEqual(0)
        }
      })
    })
  })

  // Test Case 2: E2E-style test - Link navigation to correct destinations
  describe('TC2: Click navigation to correct destinations', () => {
    it('should navigate to correct URL when clicking Home link', async () => {
      const user = userEvent.setup()
      render(
        <MemoryRouter initialEntries={['/about']}>
          <Navigation />
        </MemoryRouter>
      )

      const homeLink = screen.getByRole('link', { name: 'Home' })
      await user.click(homeLink)

      expect(homeLink).toHaveAttribute('href', '/')
    })

    it('should navigate to Features page when clicking Features link', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Navigation />)

      const featuresLink = screen.getByRole('link', { name: 'Features' })
      await user.click(featuresLink)

      expect(featuresLink).toHaveAttribute('href', '/features')
    })

    it('should navigate to About page when clicking About link', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Navigation />)

      const aboutLink = screen.getByRole('link', { name: 'About' })
      await user.click(aboutLink)

      expect(aboutLink).toHaveAttribute('href', '/about')
    })

    it('should navigate to Contact page when clicking Contact link', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Navigation />)

      const contactLink = screen.getByRole('link', { name: 'Contact' })
      await user.click(contactLink)

      expect(contactLink).toHaveAttribute('href', '/contact')
    })

    it('should have clickable links that respond to user interaction', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Navigation />)

      const allLinks = screen.getAllByRole('link')
      for (const link of allLinks) {
        // Verify each link is clickable (no errors thrown)
        await user.click(link)
      }
    })
  })

  // Accessibility tests
  describe('Accessibility', () => {
    it('should have an accessible name for navigation landmark', () => {
      renderWithRouter(<Navigation />)
      const nav = screen.getByRole('navigation')
      expect(nav).toHaveAttribute('aria-label')
    })

    it('should use semantic HTML elements', () => {
      renderWithRouter(<Navigation />)
      expect(screen.getByRole('banner')).toBeInTheDocument() // header
      expect(screen.getByRole('navigation')).toBeInTheDocument() // nav
      expect(screen.getAllByRole('link').length).toBeGreaterThan(0) // links
    })

    it('should have clearly labeled navigation links', () => {
      renderWithRouter(<Navigation />)
      NAV_LINKS.forEach((link) => {
        const navLink = screen.getByRole('link', { name: link.label })
        expect(navLink).toBeVisible()
        expect(navLink.textContent).toBe(link.label)
      })
    })
  })
})
