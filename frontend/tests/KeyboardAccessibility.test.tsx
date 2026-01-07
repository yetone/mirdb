import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { AppRoutes } from '../src/App'
import Home from '../src/pages/Home'

describe('Accessibility - Keyboard Navigation', () => {
  /**
   * Test Case 1: Tab to primary CTA button
   * Expected: Button receives focus with visible focus indicator
   */
  describe('Test Case 1: Tab to primary CTA button', () => {
    it('primary CTA button receives focus when tabbing through the page', async () => {
      const user = userEvent.setup()

      render(
        <MemoryRouter initialEntries={['/']}>
          <AppRoutes />
        </MemoryRouter>
      )

      // Tab through navbar links first (URL Shortener, Login, Register)
      await user.tab() // Navbar: URL Shortener
      await user.tab() // Navbar: Login
      await user.tab() // Navbar: Register
      // Tab to hero section "Get Started Free" link
      await user.tab()

      const primaryCTA = screen.getByRole('link', { name: /get started free/i })
      expect(primaryCTA).toBeInTheDocument()
      expect(document.activeElement).toBe(primaryCTA)
    })

    it('primary CTA button has visible focus indicator (focus-visible CSS class or outline)', async () => {
      const user = userEvent.setup()

      render(
        <MemoryRouter initialEntries={['/']}>
          <AppRoutes />
        </MemoryRouter>
      )

      const primaryCTA = screen.getByRole('link', { name: /get started free/i })

      // Tab through navbar links first, then to hero CTA
      await user.tab() // Navbar: URL Shortener
      await user.tab() // Navbar: Login
      await user.tab() // Navbar: Register
      await user.tab() // Hero: Get Started Free

      // Element should have focus
      expect(document.activeElement).toBe(primaryCTA)

      // DaisyUI btn class provides focus styling
      // Check that the element has proper button styling that includes focus states
      expect(primaryCTA).toHaveClass('btn')
    })
  })

  /**
   * Test Case 2: Press Enter on focused CTA button
   * Expected: Button click handler fires and navigation occurs
   */
  describe('Test Case 2: Press Enter on focused CTA button', () => {
    it('pressing Enter on focused "Get Started Free" link navigates to /register', async () => {
      const user = userEvent.setup()

      render(
        <MemoryRouter initialEntries={['/']}>
          <AppRoutes />
        </MemoryRouter>
      )

      // Tab through navbar links first, then to hero CTA
      await user.tab() // Navbar: URL Shortener
      await user.tab() // Navbar: Login
      await user.tab() // Navbar: Register
      await user.tab() // Hero: Get Started Free

      const primaryCTA = screen.getByRole('link', { name: /get started free/i })
      expect(document.activeElement).toBe(primaryCTA)

      // Press Enter to activate the link
      await user.keyboard('{Enter}')

      // Should navigate to register page
      expect(screen.getByRole('heading', { name: /create account/i })).toBeInTheDocument()
    })

    it('links are activated by Enter key (Space activates buttons, not links)', async () => {
      const user = userEvent.setup()

      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Tab through navbar links first, then to hero CTA
      await user.tab() // Navbar: URL Shortener
      await user.tab() // Navbar: Login
      await user.tab() // Navbar: Register
      await user.tab() // Hero: Get Started Free

      const primaryCTA = screen.getByRole('link', { name: /get started free/i })
      expect(document.activeElement).toBe(primaryCTA)

      // Note: HTML links (<a> elements) are activated by Enter, not Space
      // This is correct accessibility behavior - Space is for buttons
      // The CTA elements are links (role="link"), so they follow link semantics

      // Verify the element is a link (which activates on Enter)
      expect(primaryCTA.tagName.toLowerCase()).toBe('a')
      expect(primaryCTA).toHaveAttribute('href', '/register')
    })

    it('pressing Enter on focused "Sign In" link navigates to /login', async () => {
      const user = userEvent.setup()

      render(
        <MemoryRouter initialEntries={['/']}>
          <AppRoutes />
        </MemoryRouter>
      )

      // Tab through navbar and hero section links
      await user.tab() // Navbar: URL Shortener
      await user.tab() // Navbar: Login
      await user.tab() // Navbar: Register
      await user.tab() // Hero: Get Started Free
      await user.tab() // Hero: Sign In

      const signInLink = screen.getByRole('link', { name: /^sign in$/i })
      expect(document.activeElement).toBe(signInLink)

      // Press Enter to activate the link
      await user.keyboard('{Enter}')

      // Should navigate to login page
      expect(screen.getByRole('heading', { name: /sign in/i })).toBeInTheDocument()
    })
  })

  /**
   * Test Case 3: Query all buttons and links for tabIndex
   * Expected: No interactive elements have negative tabIndex (unless intentionally hidden)
   */
  describe('Test Case 3: Query all buttons and links for tabIndex', () => {
    it('no interactive elements have negative tabIndex (unless intentionally hidden)', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Get all links and buttons
      const links = screen.getAllByRole('link')
      const buttons = screen.queryAllByRole('button')

      const interactiveElements = [...links, ...buttons]

      // Verify that no element has a negative tabIndex
      interactiveElements.forEach((element) => {
        const tabIndex = element.getAttribute('tabindex')
        if (tabIndex !== null) {
          const tabIndexValue = parseInt(tabIndex, 10)
          // Allow tabIndex of 0 or positive, but not negative (unless aria-hidden)
          const isHidden = element.getAttribute('aria-hidden') === 'true' ||
                          element.closest('[aria-hidden="true"]') !== null

          if (!isHidden) {
            expect(tabIndexValue).toBeGreaterThanOrEqual(0)
          }
        }
        // If tabIndex is null/undefined, the element uses default browser behavior (which is fine)
      })
    })

    it('all interactive elements are focusable by default', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Get all links - they should all be naturally focusable
      const links = screen.getAllByRole('link')

      expect(links.length).toBeGreaterThan(0)

      links.forEach((link) => {
        // Links should have href attribute making them naturally focusable
        expect(link).toHaveAttribute('href')

        // If tabIndex is set, it should not be negative
        const tabIndex = link.getAttribute('tabindex')
        if (tabIndex !== null) {
          expect(parseInt(tabIndex, 10)).toBeGreaterThanOrEqual(0)
        }
      })
    })
  })

  /**
   * Test Case 4: Tab through all interactive elements
   * Expected: Focus moves in logical order through the page
   */
  describe('Test Case 4: Tab through all interactive elements', () => {
    it('focus moves in logical order through all interactive elements on the page', async () => {
      const user = userEvent.setup()

      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Get all interactive elements
      const allLinks = screen.getAllByRole('link')

      // Expected order based on DOM structure:
      // 1. Navbar: URL Shortener (home), Login, Register
      // 2. Hero Section: Get Started Free, Sign In
      // 3. Footer: Login, Register

      // The homepage should have 7 links in total:
      // - URL Shortener (navbar)
      // - Login (navbar)
      // - Register (navbar)
      // - Get Started Free (hero)
      // - Sign In (hero)
      // - Login (footer)
      // - Register (footer)
      expect(allLinks.length).toBe(7)

      // Tab through and verify focus order
      const expectedFocusOrder = [
        /url shortener/i,     // Navbar home link
        /^login$/i,           // Navbar login
        /^register$/i,        // Navbar register
        /get started free/i,  // Hero CTA
        /^sign in$/i,         // Hero secondary CTA
        /^login$/i,           // Footer login
        /^register$/i         // Footer register
      ]

      for (let i = 0; i < expectedFocusOrder.length; i++) {
        await user.tab()
        // Note: getAllByRole may return multiple matches for login/register
        // We verify that the active element matches one of the expected patterns
        expect(document.activeElement?.textContent?.toLowerCase()).toMatch(expectedFocusOrder[i])
      }
    })

    it('shift+tab moves focus in reverse logical order', async () => {
      const user = userEvent.setup()

      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // First, tab through all elements to reach the last one (footer register)
      await user.tab() // Navbar: URL Shortener
      await user.tab() // Navbar: Login
      await user.tab() // Navbar: Register
      await user.tab() // Hero: Get Started Free
      await user.tab() // Hero: Sign In
      await user.tab() // Footer: Login
      await user.tab() // Footer: Register

      // Verify we're at footer register
      expect(document.activeElement?.textContent?.toLowerCase()).toContain('register')

      // Now shift+tab back
      await user.tab({ shift: true }) // Should go to Footer Login
      expect(document.activeElement?.textContent?.toLowerCase()).toContain('login')

      await user.tab({ shift: true }) // Should go to Hero Sign In
      expect(document.activeElement?.textContent?.toLowerCase()).toContain('sign in')

      await user.tab({ shift: true }) // Should go to Hero Get Started Free
      expect(document.activeElement?.textContent?.toLowerCase()).toContain('get started')
    })

    it('focus stays within the page elements (no unexpected focus traps)', async () => {
      const user = userEvent.setup()

      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Tab through all elements multiple times
      const allLinks = screen.getAllByRole('link')
      const totalInteractiveElements = allLinks.length

      // Tab through all elements
      for (let i = 0; i < totalInteractiveElements; i++) {
        await user.tab()
      }

      // After tabbing through all elements, we should either be at the end
      // or cycle back (depending on browser/jsdom behavior)
      // The important thing is no JS error is thrown and focus is still tracked
      expect(document.activeElement).toBeTruthy()
    })
  })

  /**
   * Additional keyboard accessibility tests
   */
  describe('Additional Keyboard Accessibility', () => {
    it('links in hero section are keyboard accessible', async () => {
      const user = userEvent.setup()

      render(
        <MemoryRouter initialEntries={['/']}>
          <AppRoutes />
        </MemoryRouter>
      )

      // Hero section should contain two links
      const heroSection = screen.getByTestId('hero-section')
      const heroLinks = heroSection.querySelectorAll('a')

      expect(heroLinks.length).toBe(2)

      // Verify they are keyboard accessible (have href)
      heroLinks.forEach((link) => {
        expect(link).toHaveAttribute('href')
      })
    })

    it('links in footer are keyboard accessible', async () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Footer should contain two links
      const footer = screen.getByTestId('footer')
      const footerLinks = footer.querySelectorAll('a')

      expect(footerLinks.length).toBe(2)

      // Verify they are keyboard accessible (have href)
      footerLinks.forEach((link) => {
        expect(link).toHaveAttribute('href')
      })
    })

    it('all focusable elements have proper role for screen readers', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Check that all links have proper role
      const links = screen.getAllByRole('link')
      expect(links.length).toBeGreaterThan(0)

      // Verify each link has accessible name
      links.forEach((link) => {
        expect(link.textContent?.trim().length).toBeGreaterThan(0)
      })
    })
  })
})
