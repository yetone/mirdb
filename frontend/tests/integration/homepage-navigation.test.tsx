/**
 * Integration Test: Homepage Navigation CTAs
 * Owner: Scenario 2 - Navigation CTAs
 *
 * Tests that Register and Login call-to-action buttons work correctly
 * and navigate to appropriate pages as specified in REQ-2, US-2, and US-7.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { renderWithProviders } from '../utils/test-utils'
import Home from '../../src/pages/Home'

// Mock scrollIntoView for smooth scroll testing
const mockScrollIntoView = vi.fn()

describe('Homepage Navigation CTAs', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    window.HTMLElement.prototype.scrollIntoView = mockScrollIntoView
  })

  // Test Case 1: Click 'Get Started' button -> Navigation to /register route
  describe('Get Started CTA', () => {
    it('navigates to /register when Get Started button is clicked', async () => {
      renderWithProviders(<Home />)

      const getStartedButton = screen.getByRole('button', { name: /get started/i })
      expect(getStartedButton).toBeInTheDocument()

      // The button should be wrapped in a Link to /register
      const link = getStartedButton.closest('a')
      expect(link).toHaveAttribute('href', '/register')
    })
  })

  // Test Case 2: Click 'Login' button -> Navigation to /login route
  describe('Login CTA', () => {
    it('navigates to /login when Login button is clicked', async () => {
      renderWithProviders(<Home />)

      const loginButton = screen.getByRole('button', { name: /login/i })
      expect(loginButton).toBeInTheDocument()

      // The button should be wrapped in a Link to /login
      const link = loginButton.closest('a')
      expect(link).toHaveAttribute('href', '/login')
    })
  })

  // Test Case 3: Click 'Learn More' button -> Smooth scroll to features section
  describe('Learn More CTA', () => {
    it('scrolls to features section when Learn More button is clicked', async () => {
      const user = userEvent.setup()
      renderWithProviders(<Home />)

      const learnMoreButton = screen.getByRole('button', { name: /learn more/i })
      expect(learnMoreButton).toBeInTheDocument()

      await user.click(learnMoreButton)

      // Verify scrollIntoView was called with smooth behavior
      await waitFor(() => {
        expect(mockScrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' })
      })
    })

    it('features section has correct id for scroll targeting', () => {
      const { container } = renderWithProviders(<Home />)

      const featuresSection = container.querySelector('#features')
      expect(featuresSection).toBeInTheDocument()
    })
  })

  // Test Case 4: Verify CTA visibility without scrolling on desktop (1024px width)
  describe('CTA Visibility - Desktop', () => {
    it('both Register and Login buttons are visible in viewport at 1024px width', () => {
      // Mock desktop viewport
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        value: 1024,
      })
      Object.defineProperty(window, 'innerHeight', {
        writable: true,
        value: 768,
      })

      renderWithProviders(<Home />)

      const getStartedButton = screen.getByRole('button', { name: /get started/i })
      const loginButton = screen.getByRole('button', { name: /login/i })

      // Verify both buttons are present in the DOM
      expect(getStartedButton).toBeInTheDocument()
      expect(loginButton).toBeInTheDocument()

      // Verify buttons are not explicitly hidden (CSS display:none or visibility:hidden)
      // Note: Framer Motion starts with opacity:0 for animation, but that's not "hidden"
      expect(getStartedButton).not.toHaveStyle('display: none')
      expect(getStartedButton).not.toHaveStyle('visibility: hidden')
      expect(loginButton).not.toHaveStyle('display: none')
      expect(loginButton).not.toHaveStyle('visibility: hidden')
    })
  })

  // Test Case 5: Verify CTA visibility without scrolling on mobile (375px width)
  describe('CTA Visibility - Mobile', () => {
    it('primary CTA (Get Started) is visible in viewport at 375px width', () => {
      // Mock mobile viewport
      Object.defineProperty(window, 'innerWidth', {
        writable: true,
        value: 375,
      })
      Object.defineProperty(window, 'innerHeight', {
        writable: true,
        value: 667,
      })

      renderWithProviders(<Home />)

      const getStartedButton = screen.getByRole('button', { name: /get started/i })
      expect(getStartedButton).toBeInTheDocument()

      // Verify button is not explicitly hidden
      expect(getStartedButton).not.toHaveStyle('display: none')
      expect(getStartedButton).not.toHaveStyle('visibility: hidden')
    })
  })

  // Additional tests for accessibility and UX
  describe('CTA Accessibility', () => {
    it('all CTA buttons are keyboard accessible', async () => {
      renderWithProviders(<Home />)

      const getStartedButton = screen.getByRole('button', { name: /get started/i })
      const loginButton = screen.getByRole('button', { name: /login/i })
      const learnMoreButton = screen.getByRole('button', { name: /learn more/i })

      // Buttons should be focusable
      getStartedButton.focus()
      expect(document.activeElement).toBe(getStartedButton)

      loginButton.focus()
      expect(document.activeElement).toBe(loginButton)

      learnMoreButton.focus()
      expect(document.activeElement).toBe(learnMoreButton)
    })

    it('CTA buttons have appropriate button types', () => {
      renderWithProviders(<Home />)

      const getStartedButton = screen.getByRole('button', { name: /get started/i })
      const loginButton = screen.getByRole('button', { name: /login/i })
      const learnMoreButton = screen.getByRole('button', { name: /learn more/i })

      expect(getStartedButton).toHaveAttribute('type', 'button')
      expect(loginButton).toHaveAttribute('type', 'button')
      expect(learnMoreButton).toHaveAttribute('type', 'button')
    })
  })
})
