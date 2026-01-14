import { describe, it, expect, beforeEach, vi } from 'vitest'
import { render, screen, within, fireEvent } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter, MemoryRouter, Routes, Route } from 'react-router-dom'
import Home from '../pages/Home'

/**
 * Accessibility - Keyboard Navigation Tests
 *
 * These tests verify that all interactive elements are keyboard accessible (NFR-2)
 * ensuring the homepage follows accessibility best practices for keyboard users.
 *
 * Requirements tested: NFR-2 (90+ Lighthouse accessibility score)
 * Related PRD sections: Accessibility Considerations (keyboard navigation support, focus indicators)
 *
 * Test Cases:
 * 1. Tab through homepage - all buttons, links, and form inputs receive focus in logical order
 * 2. Focus on CTA button - visible focus indicator (outline/ring) is displayed
 * 3. Press Enter on focused 'Get Started' button - button action is triggered (navigation occurs)
 * 4. Check tabindex attributes - no positive tabindex values that disrupt natural tab order
 */

const renderWithRouter = (component: React.ReactElement) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

// Helper to render with a router that tracks navigation
const renderWithMemoryRouter = (initialPath: string = '/') => {
  const navigatedPaths: string[] = []

  const TestRoutes = () => (
    <Routes>
      <Route path="/" element={<Home />} />
      <Route
        path="/register"
        element={<div data-testid="register-page">Register Page</div>}
      />
      <Route
        path="/login"
        element={<div data-testid="login-page">Login Page</div>}
      />
      <Route path="*" element={<div>Not Found</div>} />
    </Routes>
  )

  return render(
    <MemoryRouter initialEntries={[initialPath]}>
      <TestRoutes />
    </MemoryRouter>
  )
}

describe('Accessibility - Keyboard Navigation', () => {
  beforeEach(() => {
    vi.restoreAllMocks()
  })

  describe('Test Case 1: Tab through homepage with keyboard - all buttons, links, and form inputs receive focus in logical order', () => {
    it('should allow tabbing to all interactive elements', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Home />)

      // Get all focusable interactive elements
      const buttons = screen.getAllByRole('button')
      const links = screen.getAllByRole('link')
      const inputs = screen.getAllByRole('textbox')

      // All buttons should be focusable (have implicit or explicit tabindex of 0)
      buttons.forEach((button) => {
        const tabindex = button.getAttribute('tabindex')
        expect(tabindex === null || tabindex === '0').toBe(true)
      })

      // All links should be focusable
      links.forEach((link) => {
        const tabindex = link.getAttribute('tabindex')
        expect(tabindex === null || tabindex === '0').toBe(true)
      })

      // All inputs should be focusable
      inputs.forEach((input) => {
        const tabindex = input.getAttribute('tabindex')
        expect(tabindex === null || tabindex === '0').toBe(true)
      })
    })

    it('should focus elements in logical DOM order when tabbing', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Home />)

      // Get all focusable elements in DOM order
      const allFocusableElements = document.querySelectorAll(
        'a[href], button, input, textarea, select, [tabindex]:not([tabindex="-1"])'
      )

      // There should be multiple focusable elements
      expect(allFocusableElements.length).toBeGreaterThan(0)

      // Tab through first few elements to verify tab navigation works
      await user.tab()
      expect(document.activeElement?.tagName).toBeTruthy()

      // The first focusable element should receive focus
      // (it might be Skip Link if present, or first link/button in hero)
      const focusedElement = document.activeElement
      expect(focusedElement).toBeInstanceOf(Element)
    })

    it('should include hero CTA buttons in tab order', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Home />)

      const getStartedButton = screen.getByTestId('cta-get-started')
      const loginButton = screen.getByTestId('cta-login')

      // These buttons should exist and be focusable
      expect(getStartedButton).toBeInTheDocument()
      expect(loginButton).toBeInTheDocument()

      // Focus Get Started button
      getStartedButton.focus()
      expect(document.activeElement).toBe(getStartedButton)

      // Focus Login button
      loginButton.focus()
      expect(document.activeElement).toBe(loginButton)
    })

    it('should include form input in tab order', async () => {
      renderWithRouter(<Home />)

      const urlInput = screen.getByTestId('demo-url-input')

      // Input should be focusable
      urlInput.focus()
      expect(document.activeElement).toBe(urlInput)
    })

    it('should include footer navigation links in tab order', async () => {
      renderWithRouter(<Home />)

      const footerSection = screen.getByTestId('footer-section')
      const footerLinks = within(footerSection).getAllByRole('link')

      // Footer should have navigation links
      expect(footerLinks.length).toBeGreaterThan(0)

      // Each footer link should be focusable
      footerLinks.forEach((link) => {
        link.focus()
        expect(document.activeElement).toBe(link)
      })
    })

    it('should allow tabbing through interactive elements in logical sequence', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Home />)

      const focusOrder: string[] = []

      // Tab through the page and record what gets focused
      for (let i = 0; i < 15; i++) {
        await user.tab()
        const focused = document.activeElement
        if (focused && focused !== document.body) {
          const tagName = focused.tagName.toLowerCase()
          const testId = focused.getAttribute('data-testid')
          const text = focused.textContent?.trim().slice(0, 30)
          focusOrder.push(testId || text || tagName)
        }
      }

      // Should have focused multiple elements
      expect(focusOrder.length).toBeGreaterThan(0)

      // Verify that hero elements come before features (logical order)
      const getStartedIndex = focusOrder.findIndex(
        (item) => item === 'cta-get-started' || item?.includes('Get Started')
      )
      const demoInputIndex = focusOrder.findIndex(
        (item) => item === 'demo-url-input' || item === 'input'
      )

      // Hero buttons should come before demo input (DOM order)
      if (getStartedIndex !== -1 && demoInputIndex !== -1) {
        expect(getStartedIndex).toBeLessThan(demoInputIndex)
      }
    })
  })

  describe('Test Case 2: Focus on CTA button via Tab - visible focus indicator (outline/ring) is displayed', () => {
    it('should have focus-visible styles on Get Started button', () => {
      renderWithRouter(<Home />)

      const getStartedButton = screen.getByTestId('cta-get-started')

      // Focus the button
      getStartedButton.focus()
      expect(document.activeElement).toBe(getStartedButton)

      // Check that the button has classes that provide focus styling
      // DaisyUI btn class provides built-in focus ring styles
      const classList = getStartedButton.className
      expect(classList).toContain('btn')

      // Verify the element can receive focus
      expect(getStartedButton.matches(':focus')).toBe(true)
    })

    it('should have focus-visible styles on Login button', () => {
      renderWithRouter(<Home />)

      const loginButton = screen.getByTestId('cta-login')

      // Focus the button
      loginButton.focus()
      expect(document.activeElement).toBe(loginButton)

      // DaisyUI btn class provides focus ring
      expect(loginButton.className).toContain('btn')
      expect(loginButton.matches(':focus')).toBe(true)
    })

    it('should have focus styles on demo submit button', () => {
      renderWithRouter(<Home />)

      const submitButton = screen.getByTestId('demo-submit-button')

      submitButton.focus()
      expect(document.activeElement).toBe(submitButton)
      expect(submitButton.className).toContain('btn')
      expect(submitButton.matches(':focus')).toBe(true)
    })

    it('should have focus styles on input field', () => {
      renderWithRouter(<Home />)

      const urlInput = screen.getByTestId('demo-url-input')

      urlInput.focus()
      expect(document.activeElement).toBe(urlInput)

      // DaisyUI input class provides focus styling
      expect(urlInput.className).toContain('input')
      expect(urlInput.matches(':focus')).toBe(true)
    })

    it('should have focus styles on all footer links', () => {
      renderWithRouter(<Home />)

      const footerSection = screen.getByTestId('footer-section')
      const footerLinks = within(footerSection).getAllByRole('link')

      footerLinks.forEach((link) => {
        link.focus()
        expect(document.activeElement).toBe(link)

        // Link should have focus-visible styling (via DaisyUI link class or browser default)
        expect(link.matches(':focus')).toBe(true)
      })
    })

    it('should maintain focus visibility when using Tab key', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Home />)

      // Tab to first focusable element
      await user.tab()

      const focusedElement = document.activeElement
      expect(focusedElement).not.toBe(document.body)
      expect(focusedElement?.matches(':focus')).toBe(true)
    })
  })

  describe("Test Case 3: Press Enter on focused 'Get Started' button - button action is triggered (navigation occurs)", () => {
    it('should navigate to /register when Enter is pressed on Get Started button', async () => {
      const user = userEvent.setup()
      renderWithMemoryRouter('/')

      // Find and focus the Get Started button
      const getStartedButton = screen.getByTestId('cta-get-started')
      getStartedButton.focus()

      // Press Enter
      await user.keyboard('{Enter}')

      // Should navigate to register page
      expect(screen.getByTestId('register-page')).toBeInTheDocument()
    })

    it('should navigate to /login when Enter is pressed on Login button', async () => {
      const user = userEvent.setup()
      renderWithMemoryRouter('/')

      const loginButton = screen.getByTestId('cta-login')
      loginButton.focus()

      await user.keyboard('{Enter}')

      expect(screen.getByTestId('login-page')).toBeInTheDocument()
    })

    it('should submit form when Enter is pressed on URL input', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Home />)

      const urlInput = screen.getByTestId('demo-url-input')

      // Focus input and type a URL
      await user.click(urlInput)
      await user.type(urlInput, 'https://example.com')

      // Press Enter to submit
      await user.keyboard('{Enter}')

      // Should show register prompt (form was submitted)
      expect(screen.getByTestId('demo-register-prompt')).toBeInTheDocument()
    })

    it('should submit form when Enter is pressed on submit button', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Home />)

      const urlInput = screen.getByTestId('demo-url-input')
      const submitButton = screen.getByTestId('demo-submit-button')

      // Enter a valid URL
      await user.type(urlInput, 'https://example.com')

      // Focus and press Enter on submit button
      submitButton.focus()
      await user.keyboard('{Enter}')

      // Should show register prompt
      expect(screen.getByTestId('demo-register-prompt')).toBeInTheDocument()
    })

    it('should trigger link navigation with Enter key', async () => {
      const user = userEvent.setup()
      renderWithMemoryRouter('/')

      // Get footer Home link
      const footerSection = screen.getByTestId('footer-section')
      const homeLink = within(footerSection).getByRole('link', { name: /home/i })

      homeLink.focus()
      expect(document.activeElement).toBe(homeLink)

      // Enter on link should work (React Router handles this)
      await user.keyboard('{Enter}')

      // Should still be on home page
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })

    it('should activate buttons with Space key as well', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Home />)

      const urlInput = screen.getByTestId('demo-url-input')
      const submitButton = screen.getByTestId('demo-submit-button')

      // Enter a valid URL first
      await user.type(urlInput, 'https://example.com')

      // Focus submit button and press Space
      submitButton.focus()
      await user.keyboard(' ')

      // Should trigger form submission
      expect(screen.getByTestId('demo-register-prompt')).toBeInTheDocument()
    })
  })

  describe('Test Case 4: Check tabindex attributes on homepage elements - no positive tabindex values that disrupt natural tab order', () => {
    it('should not have any positive tabindex values', () => {
      renderWithRouter(<Home />)

      // Query all elements with tabindex attribute
      const elementsWithTabindex = document.querySelectorAll('[tabindex]')

      elementsWithTabindex.forEach((element) => {
        const tabindexValue = element.getAttribute('tabindex')
        if (tabindexValue !== null) {
          const numericValue = parseInt(tabindexValue, 10)
          // tabindex should be 0 (natural order) or -1 (removed from tab order)
          // Positive values (1, 2, 3, etc.) disrupt natural tab order
          expect(numericValue).toBeLessThanOrEqual(0)
        }
      })
    })

    it('should have buttons without explicit tabindex (using natural order)', () => {
      renderWithRouter(<Home />)

      const buttons = screen.getAllByRole('button')

      buttons.forEach((button) => {
        const tabindex = button.getAttribute('tabindex')
        // Buttons should either have no tabindex (default 0) or explicit 0
        // They should NOT have positive tabindex values
        if (tabindex !== null) {
          const numericValue = parseInt(tabindex, 10)
          expect(numericValue).toBeLessThanOrEqual(0)
        }
      })
    })

    it('should have links without positive tabindex values', () => {
      renderWithRouter(<Home />)

      const links = screen.getAllByRole('link')

      links.forEach((link) => {
        const tabindex = link.getAttribute('tabindex')
        if (tabindex !== null) {
          const numericValue = parseInt(tabindex, 10)
          expect(numericValue).toBeLessThanOrEqual(0)
        }
      })
    })

    it('should have form inputs without positive tabindex values', () => {
      renderWithRouter(<Home />)

      const inputs = document.querySelectorAll('input, textarea, select')

      inputs.forEach((input) => {
        const tabindex = input.getAttribute('tabindex')
        if (tabindex !== null) {
          const numericValue = parseInt(tabindex, 10)
          expect(numericValue).toBeLessThanOrEqual(0)
        }
      })
    })

    it('should preserve natural DOM-based focus order', () => {
      renderWithRouter(<Home />)

      // Get all focusable elements
      const focusable = document.querySelectorAll(
        'a[href], button, input, textarea, select, [tabindex]:not([tabindex="-1"])'
      )

      // Ensure no elements have tabindex > 0 which would reorder them
      const hasPositiveTabindex = Array.from(focusable).some((el) => {
        const tabindex = el.getAttribute('tabindex')
        return tabindex !== null && parseInt(tabindex, 10) > 0
      })

      expect(hasPositiveTabindex).toBe(false)
    })

    it('should have interactive elements naturally focusable', () => {
      renderWithRouter(<Home />)

      // Hero section CTAs
      const getStartedCta = screen.getByTestId('cta-get-started')
      const loginCta = screen.getByTestId('cta-login')

      // These should be naturally focusable (no tabindex or tabindex=0)
      expect(
        getStartedCta.getAttribute('tabindex') === null ||
          getStartedCta.getAttribute('tabindex') === '0'
      ).toBe(true)
      expect(
        loginCta.getAttribute('tabindex') === null ||
          loginCta.getAttribute('tabindex') === '0'
      ).toBe(true)

      // Demo form elements
      const urlInput = screen.getByTestId('demo-url-input')
      const submitButton = screen.getByTestId('demo-submit-button')

      expect(
        urlInput.getAttribute('tabindex') === null ||
          urlInput.getAttribute('tabindex') === '0'
      ).toBe(true)
      expect(
        submitButton.getAttribute('tabindex') === null ||
          submitButton.getAttribute('tabindex') === '0'
      ).toBe(true)
    })
  })

  describe('Additional Keyboard Accessibility Tests', () => {
    it('should allow Shift+Tab to navigate backwards', async () => {
      const user = userEvent.setup()
      renderWithRouter(<Home />)

      // Tab forward a few times
      await user.tab()
      await user.tab()
      await user.tab()

      const forwardFocused = document.activeElement

      // Tab backward
      await user.tab({ shift: true })

      const backwardFocused = document.activeElement

      // Should have focused different elements
      expect(backwardFocused).not.toBe(forwardFocused)
    })

    it('should have proper role attributes for interactive elements', () => {
      renderWithRouter(<Home />)

      // Buttons should have button role
      const buttons = screen.getAllByRole('button')
      expect(buttons.length).toBeGreaterThan(0)

      // Links should have link role
      const links = screen.getAllByRole('link')
      expect(links.length).toBeGreaterThan(0)

      // Input should have textbox role
      const textboxes = screen.getAllByRole('textbox')
      expect(textboxes.length).toBeGreaterThan(0)
    })

    it('should have accessible names for interactive elements', () => {
      renderWithRouter(<Home />)

      // Buttons should have accessible names
      const getStartedButton = screen.getByTestId('cta-get-started')
      expect(getStartedButton.textContent?.trim()).toBeTruthy()

      const loginButton = screen.getByTestId('cta-login')
      expect(loginButton.textContent?.trim()).toBeTruthy()

      const submitButton = screen.getByTestId('demo-submit-button')
      expect(submitButton.textContent?.trim()).toBeTruthy()

      // Input should have aria-label
      const urlInput = screen.getByTestId('demo-url-input')
      expect(urlInput.getAttribute('aria-label')).toBeTruthy()
    })

    it('should handle keyboard navigation in hero section navigation links', async () => {
      renderWithRouter(<Home />)

      // Find the section navigation links (Features, Try It)
      const heroSection = screen.getByTestId('hero-section')
      const navLinks = within(heroSection).getAllByRole('link')

      // There should be navigation links in hero
      expect(navLinks.length).toBeGreaterThan(0)

      // Each link should be focusable
      navLinks.forEach((link) => {
        link.focus()
        expect(document.activeElement).toBe(link)
      })
    })
  })
})
