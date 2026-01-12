import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import Navbar from './Navbar'
import Home from '../pages/Home'
import Login from '../pages/Login'
import Register from '../pages/Register'

// Mock scrollIntoView
Element.prototype.scrollIntoView = vi.fn()

// Mock localStorage
const localStorageMock = {
  getItem: vi.fn(),
  setItem: vi.fn(),
  clear: vi.fn(),
}
Object.defineProperty(window, 'localStorage', { value: localStorageMock })

const renderWithRouter = (initialEntries = ['/']) => {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <Navbar />
      <div className="pt-16">
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/login" element={<Login />} />
          <Route path="/register" element={<Register />} />
        </Routes>
      </div>
    </MemoryRouter>
  )
}

describe('Navigation Header Display', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    localStorageMock.getItem.mockReturnValue(null)
  })

  // Test Case 1: Header element exists with logo, navigation links, and auth buttons
  describe('Test Case 1: Header Components', () => {
    it('renders navigation header with all required components', () => {
      renderWithRouter()

      // Check header exists
      const header = screen.getByTestId('navigation-header')
      expect(header).toBeInTheDocument()

      // Check logo exists
      const logo = screen.getByTestId('nav-logo')
      expect(logo).toBeInTheDocument()
      expect(logo).toHaveTextContent('ShortURL')

      // Check navigation links exist
      const featuresLink = screen.getByTestId('nav-features')
      expect(featuresLink).toBeInTheDocument()
      expect(featuresLink).toHaveTextContent('Features')

      const howItWorksLink = screen.getByTestId('nav-how-it-works')
      expect(howItWorksLink).toBeInTheDocument()
      expect(howItWorksLink).toHaveTextContent('How It Works')

      // Check auth buttons exist
      const loginButton = screen.getByTestId('nav-login')
      expect(loginButton).toBeInTheDocument()
      expect(loginButton).toHaveTextContent('Login')

      const registerButton = screen.getByTestId('nav-register')
      expect(registerButton).toBeInTheDocument()
      expect(registerButton).toHaveTextContent('Register')
    })

    it('renders theme toggle component', () => {
      renderWithRouter()

      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle).toBeInTheDocument()
    })
  })

  // Test Case 2: Click on Login button navigates to /login route
  describe('Test Case 2: Login Button Navigation', () => {
    it('navigates to /login when clicking Login button', async () => {
      renderWithRouter()

      const loginButton = screen.getByTestId('nav-login')
      fireEvent.click(loginButton)

      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })
    })
  })

  // Test Case 3: Click on Register button navigates to /register route
  describe('Test Case 3: Register Button Navigation', () => {
    it('navigates to /register when clicking Register button', async () => {
      renderWithRouter()

      const registerButton = screen.getByTestId('nav-register')
      fireEvent.click(registerButton)

      await waitFor(() => {
        expect(screen.getByTestId('register-page')).toBeInTheDocument()
      })
    })
  })

  // Test Case 4: Click on Features navigation link scrolls to Features section
  describe('Test Case 4: Features Link Scroll', () => {
    it('scrolls to Features section when clicking Features link', () => {
      renderWithRouter()

      const featuresLink = screen.getByTestId('nav-features')
      const featuresSection = screen.getByTestId('features-section')

      expect(featuresSection).toBeInTheDocument()

      fireEvent.click(featuresLink)
      expect(Element.prototype.scrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' })
    })
  })

  // Test Case 5: Click on How It Works navigation link scrolls to How It Works section
  describe('Test Case 5: How It Works Link Scroll', () => {
    it('scrolls to How It Works section when clicking How It Works link', () => {
      renderWithRouter()

      const howItWorksLink = screen.getByTestId('nav-how-it-works')
      const howItWorksSection = screen.getByTestId('how-it-works-section')

      expect(howItWorksSection).toBeInTheDocument()

      fireEvent.click(howItWorksLink)
      expect(Element.prototype.scrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' })
    })
  })

  // Additional test for theme toggle functionality
  describe('Theme Toggle Functionality', () => {
    it('toggles theme when clicking theme toggle button', () => {
      renderWithRouter()

      const themeToggle = screen.getByTestId('theme-toggle')

      // Click to toggle theme
      fireEvent.click(themeToggle)

      // Verify localStorage.setItem was called with theme
      expect(localStorageMock.setItem).toHaveBeenCalledWith('theme', expect.any(String))
    })
  })
})
