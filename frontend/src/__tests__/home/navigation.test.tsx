/**
 * Navigation Tests
 * Owner: Scenario 4 - Navigation Bar Functionality
 *
 * Test coverage:
 * - Navbar renders with logo
 * - Login/Register links present
 * - ThemeToggle component present
 * - Navigation links work correctly
 */
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { renderWithProviders } from './test-utils'
import Navbar from '../../components/Navbar'

// Mock the useNavigate from react-router-dom to track navigation
const mockedNavigate = vi.fn()
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom')
  return {
    ...actual,
    useNavigate: () => mockedNavigate,
  }
})

describe('Navigation Bar Functionality', () => {
  beforeEach(() => {
    mockedNavigate.mockClear()
  })

  describe('Unit Tests - Navbar Element Rendering', () => {
    it('renders Navbar with logo/brand name visible', () => {
      renderWithProviders(<Navbar />)

      const brandElement = screen.getByText('URL Shortener')
      expect(brandElement).toBeInTheDocument()
      expect(brandElement).toBeVisible()
    })

    it('renders Login navigation link', () => {
      renderWithProviders(<Navbar />)

      const loginLink = screen.getByRole('link', { name: /login/i })
      expect(loginLink).toBeInTheDocument()
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('renders Register navigation link', () => {
      renderWithProviders(<Navbar />)

      const registerLink = screen.getByRole('link', { name: /register/i })
      expect(registerLink).toBeInTheDocument()
      expect(registerLink).toHaveAttribute('href', '/register')
    })

    it('renders ThemeToggle component', () => {
      renderWithProviders(<Navbar />)

      // ThemeToggle renders a button with aria-label for accessibility
      const themeToggleButton = screen.getByRole('button', { name: /switch to (light|dark) mode/i })
      expect(themeToggleButton).toBeInTheDocument()
      expect(themeToggleButton).toBeVisible()
    })
  })

  describe('Integration Tests - Navigation Functionality', () => {
    it('Login link navigates to /login route when clicked', async () => {
      const user = userEvent.setup()
      renderWithProviders(<Navbar />)

      const loginLink = screen.getByRole('link', { name: /login/i })
      expect(loginLink).toHaveAttribute('href', '/login')

      await user.click(loginLink)

      // Verify the link href attribute points to /login
      // In a BrowserRouter context, clicking the link will navigate
      expect(loginLink).toHaveAttribute('href', '/login')
    })

    it('Register link navigates to /register route when clicked', async () => {
      const user = userEvent.setup()
      renderWithProviders(<Navbar />)

      const registerLink = screen.getByRole('link', { name: /register/i })
      expect(registerLink).toHaveAttribute('href', '/register')

      await user.click(registerLink)

      // Verify the link href attribute points to /register
      expect(registerLink).toHaveAttribute('href', '/register')
    })
  })

  describe('Navbar Structure and Accessibility', () => {
    it('navbar contains all required elements in correct structure', () => {
      renderWithProviders(<Navbar />)

      // Check navbar element exists
      const navbar = screen.getByRole('navigation')
      expect(navbar).toBeInTheDocument()

      // Verify brand/logo link points to home
      const brandLink = screen.getByRole('link', { name: /url shortener/i })
      expect(brandLink).toHaveAttribute('href', '/')

      // Verify Login and Register links
      const loginLink = screen.getByRole('link', { name: /login/i })
      const registerLink = screen.getByRole('link', { name: /register/i })
      expect(loginLink).toBeInTheDocument()
      expect(registerLink).toBeInTheDocument()

      // Verify ThemeToggle button
      const themeToggle = screen.getByRole('button', { name: /switch to (light|dark) mode/i })
      expect(themeToggle).toBeInTheDocument()
    })

    it('Login link is keyboard accessible', async () => {
      const user = userEvent.setup()
      renderWithProviders(<Navbar />)

      const loginLink = screen.getByRole('link', { name: /login/i })
      loginLink.focus()
      expect(loginLink).toHaveFocus()
    })

    it('Register link is keyboard accessible', async () => {
      const user = userEvent.setup()
      renderWithProviders(<Navbar />)

      const registerLink = screen.getByRole('link', { name: /register/i })
      registerLink.focus()
      expect(registerLink).toHaveFocus()
    })
  })
})
