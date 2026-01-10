import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import Home from '../pages/Home'
import HeroSection from '../components/HeroSection'
import FooterSection from '../components/FooterSection'

// Helper to render with router
const renderWithRouter = (initialEntries: string[] = ['/']) => {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
        <Route path="/login" element={<div data-testid="login-page">Login Page</div>} />
        <Route path="/privacy" element={<div data-testid="privacy-page">Privacy Page</div>} />
        <Route path="/terms" element={<div data-testid="terms-page">Terms Page</div>} />
      </Routes>
    </MemoryRouter>
  )
}

describe('Keyboard Accessibility (NFR-4)', () => {
  // Test Case 1: Tab to primary CTA button - Button receives focus and has visible focus indicator
  describe('Test Case 1: Tab to primary CTA button', () => {
    it('should allow the primary CTA button to receive focus via Tab key', async () => {
      const user = userEvent.setup()
      renderWithRouter()

      // Tab to navigate to the first focusable element (primary CTA button)
      await user.tab()

      // The primary CTA button should be focusable and receive focus
      const primaryCTA = screen.getByTestId('cta-register')
      expect(primaryCTA).toBeInTheDocument()
      expect(document.activeElement).toBe(primaryCTA)
    })

    it('should have a visible focus indicator on the primary CTA button', async () => {
      const user = userEvent.setup()
      renderWithRouter()

      await user.tab()

      const primaryCTA = screen.getByTestId('cta-register')
      expect(document.activeElement).toBe(primaryCTA)

      // Check that the button has the btn class which provides focus styles via DaisyUI
      expect(primaryCTA).toHaveClass('btn')
    })

    it('should have keyboard focusable primary CTA with proper role', () => {
      renderWithRouter()

      const primaryCTA = screen.getByTestId('cta-register')

      // Links are focusable by default and should be accessible via keyboard
      expect(primaryCTA.tagName.toLowerCase()).toBe('a')
      expect(primaryCTA).toHaveAttribute('href', '/register')
    })
  })

  // Test Case 2: Press Enter on focused CTA button - Button activates and navigates to registration
  describe('Test Case 2: Press Enter on focused CTA button', () => {
    it('should navigate to registration page when Enter is pressed on focused CTA', async () => {
      const user = userEvent.setup()
      renderWithRouter()

      // Tab to the primary CTA
      await user.tab()

      const primaryCTA = screen.getByTestId('cta-register')
      expect(document.activeElement).toBe(primaryCTA)

      // Press Enter to activate the link
      await user.keyboard('{Enter}')

      // Should navigate to registration page
      expect(screen.getByTestId('register-page')).toBeInTheDocument()
    })

    it('should be activatable by clicking (simulating Space key behavior)', async () => {
      const user = userEvent.setup()
      renderWithRouter()

      const primaryCTA = screen.getByTestId('cta-register')

      // Click the button (links respond to click on Enter/Space)
      await user.click(primaryCTA)

      // Should navigate to registration page
      expect(screen.getByTestId('register-page')).toBeInTheDocument()
    })

    it('should have accessible text content for screen readers', () => {
      renderWithRouter()

      const primaryCTA = screen.getByRole('link', { name: /get started free/i })
      expect(primaryCTA).toBeInTheDocument()
      expect(primaryCTA).toHaveTextContent('Get Started Free')
    })
  })

  // Test Case 3: Tab through all footer links - All links are reachable via keyboard with visible focus states
  describe('Test Case 3: Tab through all footer links', () => {
    it('should make all footer navigation links reachable via Tab key', async () => {
      const user = userEvent.setup()
      render(
        <MemoryRouter>
          <FooterSection />
        </MemoryRouter>
      )

      // Get all footer links
      const homeLink = screen.getByTestId('footer-link-home')
      const loginLink = screen.getByTestId('footer-link-login')
      const registerLink = screen.getByTestId('footer-link-register')
      const privacyLink = screen.getByTestId('footer-link-privacy')
      const termsLink = screen.getByTestId('footer-link-terms')

      // Tab through all links
      await user.tab()
      expect(document.activeElement).toBe(homeLink)

      await user.tab()
      expect(document.activeElement).toBe(loginLink)

      await user.tab()
      expect(document.activeElement).toBe(registerLink)

      await user.tab()
      expect(document.activeElement).toBe(privacyLink)

      await user.tab()
      expect(document.activeElement).toBe(termsLink)
    })

    it('should have visible focus states on all footer links', async () => {
      const user = userEvent.setup()
      render(
        <MemoryRouter>
          <FooterSection />
        </MemoryRouter>
      )

      const footerLinks = [
        screen.getByTestId('footer-link-home'),
        screen.getByTestId('footer-link-login'),
        screen.getByTestId('footer-link-register'),
        screen.getByTestId('footer-link-privacy'),
        screen.getByTestId('footer-link-terms'),
      ]

      // Each link should have the link class which provides focus styles
      footerLinks.forEach((link) => {
        expect(link).toHaveClass('link')
        expect(link).toHaveClass('link-hover')
      })

      // Tab through and verify each receives focus
      for (const expectedLink of footerLinks) {
        await user.tab()
        expect(document.activeElement).toBe(expectedLink)
      }
    })

    it('should have all footer links accessible via keyboard role', () => {
      render(
        <MemoryRouter>
          <FooterSection />
        </MemoryRouter>
      )

      // All footer links should be accessible via their role
      const homeLink = screen.getByRole('link', { name: /home/i })
      const loginLink = screen.getByRole('link', { name: /login/i })
      const registerLink = screen.getByRole('link', { name: /register/i })
      const privacyLink = screen.getByRole('link', { name: /privacy policy/i })
      const termsLink = screen.getByRole('link', { name: /terms of service/i })

      expect(homeLink).toBeInTheDocument()
      expect(loginLink).toBeInTheDocument()
      expect(registerLink).toBeInTheDocument()
      expect(privacyLink).toBeInTheDocument()
      expect(termsLink).toBeInTheDocument()
    })
  })

  // Test Case 4: Check tab order - Tab order follows logical visual flow (top to bottom, left to right)
  describe('Test Case 4: Check tab order', () => {
    it('should follow logical tab order from hero to footer (top to bottom)', async () => {
      const user = userEvent.setup()
      renderWithRouter()

      // Expected tab order based on DOM structure:
      // 1. Hero section: Primary CTA (Get Started Free)
      // 2. Hero section: Login link
      // 3. Footer: Home link
      // 4. Footer: Login link
      // 5. Footer: Register link
      // 6. Footer: Privacy Policy link
      // 7. Footer: Terms of Service link

      // First tab: Primary CTA in hero
      await user.tab()
      const primaryCTA = screen.getByTestId('cta-register')
      expect(document.activeElement).toBe(primaryCTA)

      // Second tab: Login link in hero
      await user.tab()
      const loginLink = screen.getByTestId('login-link')
      expect(document.activeElement).toBe(loginLink)

      // Third tab: Footer Home link
      await user.tab()
      const footerHome = screen.getByTestId('footer-link-home')
      expect(document.activeElement).toBe(footerHome)

      // Fourth tab: Footer Login link
      await user.tab()
      const footerLogin = screen.getByTestId('footer-link-login')
      expect(document.activeElement).toBe(footerLogin)

      // Fifth tab: Footer Register link
      await user.tab()
      const footerRegister = screen.getByTestId('footer-link-register')
      expect(document.activeElement).toBe(footerRegister)

      // Sixth tab: Footer Privacy link
      await user.tab()
      const footerPrivacy = screen.getByTestId('footer-link-privacy')
      expect(document.activeElement).toBe(footerPrivacy)

      // Seventh tab: Footer Terms link
      await user.tab()
      const footerTerms = screen.getByTestId('footer-link-terms')
      expect(document.activeElement).toBe(footerTerms)
    })

    it('should allow reverse tab navigation using Shift+Tab', async () => {
      const user = userEvent.setup()
      renderWithRouter()

      // Tab to the third element (footer home link)
      await user.tab() // Primary CTA
      await user.tab() // Login link
      await user.tab() // Footer home

      const footerHome = screen.getByTestId('footer-link-home')
      expect(document.activeElement).toBe(footerHome)

      // Shift+Tab back to login link
      await user.tab({ shift: true })
      const loginLink = screen.getByTestId('login-link')
      expect(document.activeElement).toBe(loginLink)

      // Shift+Tab back to primary CTA
      await user.tab({ shift: true })
      const primaryCTA = screen.getByTestId('cta-register')
      expect(document.activeElement).toBe(primaryCTA)
    })

    it('should not have any tabindex values that break natural tab order', () => {
      renderWithRouter()

      // Get all interactive elements
      const primaryCTA = screen.getByTestId('cta-register')
      const loginLink = screen.getByTestId('login-link')
      const footerLinks = [
        screen.getByTestId('footer-link-home'),
        screen.getByTestId('footer-link-login'),
        screen.getByTestId('footer-link-register'),
        screen.getByTestId('footer-link-privacy'),
        screen.getByTestId('footer-link-terms'),
      ]

      // Check that no element has a positive tabindex (which would disrupt natural order)
      const allInteractiveElements = [primaryCTA, loginLink, ...footerLinks]

      allInteractiveElements.forEach((element) => {
        const tabindex = element.getAttribute('tabindex')
        // tabindex should be null, '0', or '-1' (not a positive number)
        if (tabindex !== null) {
          expect(parseInt(tabindex)).toBeLessThanOrEqual(0)
        }
      })
    })

    it('should ensure all links have minimum touch target size for accessibility', () => {
      renderWithRouter()

      // All interactive elements should have minimum height for accessibility (44px recommended)
      const primaryCTA = screen.getByTestId('cta-register')
      const loginLink = screen.getByTestId('login-link')
      const footerLinks = [
        screen.getByTestId('footer-link-home'),
        screen.getByTestId('footer-link-login'),
        screen.getByTestId('footer-link-register'),
        screen.getByTestId('footer-link-privacy'),
        screen.getByTestId('footer-link-terms'),
      ]

      // Check that CTA and footer links have min-h-[44px] class for accessibility
      expect(primaryCTA).toHaveClass('min-h-[44px]')
      expect(loginLink).toHaveClass('min-h-[44px]')

      footerLinks.forEach((link) => {
        expect(link).toHaveClass('min-h-[44px]')
      })
    })
  })
})
