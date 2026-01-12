import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import Navbar from './Navbar'
import Home from '../pages/Home'
import Login from '../pages/Login'
import { AuthProvider } from '../contexts/AuthContext'

// Mock localStorage
const localStorageMock = {
  getItem: vi.fn(),
  setItem: vi.fn(),
  clear: vi.fn(),
  removeItem: vi.fn(),
}
Object.defineProperty(window, 'localStorage', { value: localStorageMock })

// Store original console.error
const originalConsoleError = console.error

const renderWithRouter = (initialEntries = ['/']) => {
  return render(
    <MemoryRouter initialEntries={initialEntries}>
      <AuthProvider>
        <Navbar />
        <div className="pt-16">
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/login" element={<Login />} />
          </Routes>
        </div>
      </AuthProvider>
    </MemoryRouter>
  )
}

describe('Error Handling - Navigation Links', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    localStorageMock.getItem.mockReturnValue(null)
  })

  afterEach(() => {
    // Restore console.error after each test
    console.error = originalConsoleError
  })

  // Test Case 1: Click internal scroll link when target section doesn't exist
  // Expected: No JavaScript error is thrown, graceful fallback
  describe('Test Case 1: Graceful fallback when target section does not exist', () => {
    it('does not throw JavaScript error when clicking Features link and section is missing', () => {
      // Render Navbar in isolation (without Home component that provides sections)
      render(
        <MemoryRouter initialEntries={['/']}>
          <AuthProvider>
            <Navbar />
          </AuthProvider>
        </MemoryRouter>
      )

      // Track any errors that might be thrown
      const errorSpy = vi.fn()
      console.error = errorSpy

      const featuresLink = screen.getByTestId('nav-features')

      // This should NOT throw an error
      expect(() => {
        fireEvent.click(featuresLink)
      }).not.toThrow()

      // Verify no console errors were logged
      expect(errorSpy).not.toHaveBeenCalled()
    })

    it('does not throw JavaScript error when clicking How It Works link and section is missing', () => {
      // Render Navbar in isolation (without sections)
      render(
        <MemoryRouter initialEntries={['/']}>
          <AuthProvider>
            <Navbar />
          </AuthProvider>
        </MemoryRouter>
      )

      const errorSpy = vi.fn()
      console.error = errorSpy

      const howItWorksLink = screen.getByTestId('nav-how-it-works')

      // This should NOT throw an error
      expect(() => {
        fireEvent.click(howItWorksLink)
      }).not.toThrow()

      // Verify no console errors were logged
      expect(errorSpy).not.toHaveBeenCalled()
    })

    it('gracefully handles missing section by doing nothing (no scroll attempted)', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <AuthProvider>
            <Navbar />
          </AuthProvider>
        </MemoryRouter>
      )

      // Mock scrollIntoView to track if it's called
      const scrollIntoViewMock = vi.fn()
      Element.prototype.scrollIntoView = scrollIntoViewMock

      const featuresLink = screen.getByTestId('nav-features')
      fireEvent.click(featuresLink)

      // scrollIntoView should NOT be called because section doesn't exist
      expect(scrollIntoViewMock).not.toHaveBeenCalled()
    })

    it('scrolls correctly when section exists (positive test)', () => {
      // Create a mock features section before rendering
      const featuresDiv = document.createElement('div')
      featuresDiv.id = 'features'
      document.body.appendChild(featuresDiv)

      // Mock scrollIntoView
      const scrollIntoViewMock = vi.fn()
      featuresDiv.scrollIntoView = scrollIntoViewMock

      render(
        <MemoryRouter initialEntries={['/']}>
          <AuthProvider>
            <Navbar />
          </AuthProvider>
        </MemoryRouter>
      )

      const featuresLink = screen.getByTestId('nav-features')
      fireEvent.click(featuresLink)

      // scrollIntoView SHOULD be called because section exists
      expect(scrollIntoViewMock).toHaveBeenCalledWith({ behavior: 'smooth' })

      // Cleanup
      document.body.removeChild(featuresDiv)
    })
  })

  // Test Case 2: Test Login link when auth service is unavailable
  // Expected: Navigation to login page still works (handled by login page)
  describe('Test Case 2: Login link navigation when auth service is unavailable', () => {
    it('navigates to login page regardless of auth service status', async () => {
      renderWithRouter()

      const loginButton = screen.getByTestId('nav-login')
      expect(loginButton).toBeInTheDocument()

      // Click login button
      fireEvent.click(loginButton)

      // Should navigate to login page
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })
    })

    it('Login link works with React Router (client-side navigation)', async () => {
      renderWithRouter()

      const loginButton = screen.getByTestId('nav-login')

      // Verify it's a Link component (has href attribute)
      expect(loginButton).toHaveAttribute('href', '/login')

      // Click and verify navigation works
      fireEvent.click(loginButton)

      await waitFor(() => {
        const loginPage = screen.getByTestId('login-page')
        expect(loginPage).toBeInTheDocument()
      })
    })

    it('Login page renders even when network requests might fail', async () => {
      // Simulate a scenario where API calls might fail
      // The navigation itself should still work since it's client-side routing

      // Mock fetch to simulate auth service unavailability
      const originalFetch = global.fetch
      global.fetch = vi.fn().mockRejectedValue(new Error('Network error'))

      renderWithRouter()

      const loginButton = screen.getByTestId('nav-login')
      fireEvent.click(loginButton)

      // Navigation should still work - login page should render
      await waitFor(() => {
        expect(screen.getByTestId('login-page')).toBeInTheDocument()
      })

      // Login form should be visible
      expect(screen.getByText('Welcome Back')).toBeInTheDocument()

      // Restore fetch
      global.fetch = originalFetch
    })

    it('Login link has correct href attribute for direct URL access', () => {
      renderWithRouter()

      const loginButton = screen.getByTestId('nav-login')

      // The Login link should have correct href so users can also navigate directly
      expect(loginButton).toHaveAttribute('href', '/login')
      expect(loginButton.tagName.toLowerCase()).toBe('a')
    })
  })
})
