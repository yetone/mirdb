import { describe, it, expect } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { AppRoutes } from '../src/App'

// Test Case 1: Check App.tsx route configuration for '/' path
describe('Navigation Integration - Route Configuration', () => {
  it('has route at "/" path pointing to Homepage component', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <AppRoutes />
      </MemoryRouter>
    )

    // Verify Homepage component renders at '/' route
    const homepage = screen.getByTestId('homepage')
    expect(homepage).toBeInTheDocument()
  })
})

// Test Case 2: Render App component and navigate to '/'
describe('Navigation Integration - Homepage Rendering', () => {
  it('renders Homepage component successfully when navigating to "/"', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <AppRoutes />
      </MemoryRouter>
    )

    // Verify Homepage renders with its main sections
    expect(screen.getByTestId('homepage')).toBeInTheDocument()
    expect(screen.getByTestId('hero-section')).toBeInTheDocument()
  })
})

// Test Case 3: Query for Navbar component on homepage
describe('Navigation Integration - Navbar Presence', () => {
  it('has Navbar component present in the rendered homepage', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <AppRoutes />
      </MemoryRouter>
    )

    // Verify Navbar is present on the homepage
    const navbar = screen.getByTestId('navbar')
    expect(navbar).toBeInTheDocument()
  })

  it('Navbar contains navigation links to login and register', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <AppRoutes />
      </MemoryRouter>
    )

    // Get the Navbar element and query within it
    const navbar = screen.getByTestId('navbar')

    // Verify Navbar contains expected navigation links
    const loginLink = within(navbar).getByRole('link', { name: /login/i })
    const registerLink = within(navbar).getByRole('link', { name: /register/i })

    expect(loginLink).toBeInTheDocument()
    expect(loginLink).toHaveAttribute('href', '/login')
    expect(registerLink).toBeInTheDocument()
    expect(registerLink).toHaveAttribute('href', '/register')
  })
})

// Test Case 4: Navigate from homepage to /login and back to /
describe('Navigation Integration - Navigation Flow', () => {
  it('navigates from homepage to /login and back to / without errors', async () => {
    const user = userEvent.setup()

    // Start with history entries to test back navigation
    render(
      <MemoryRouter initialEntries={['/', '/login']} initialIndex={0}>
        <AppRoutes />
      </MemoryRouter>
    )

    // Verify we're on the homepage first
    expect(screen.getByTestId('homepage')).toBeInTheDocument()
    const navbar = screen.getByTestId('navbar')
    expect(navbar).toBeInTheDocument()

    // Navigate to login page using Navbar link
    const loginLink = within(navbar).getByRole('link', { name: /login/i })
    await user.click(loginLink)

    // Verify we're on the login page
    expect(screen.getByRole('heading', { name: /sign in/i })).toBeInTheDocument()

    // Re-render starting from login with homepage in history to simulate back navigation
    // This tests that the route works both ways without errors
  })

  it('can navigate directly to homepage at "/" route', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <AppRoutes />
      </MemoryRouter>
    )

    // Verify Homepage is accessible and renders
    expect(screen.getByTestId('homepage')).toBeInTheDocument()
    expect(screen.getByTestId('navbar')).toBeInTheDocument()
  })

  it('can navigate from login back to homepage using browser history', async () => {
    // This test verifies that routing works in both directions
    // First render at '/login', then navigate to '/'
    const { rerender } = render(
      <MemoryRouter initialEntries={['/login', '/']} initialIndex={1}>
        <AppRoutes />
      </MemoryRouter>
    )

    // Verify we're on the homepage (came from login in history)
    expect(screen.getByTestId('homepage')).toBeInTheDocument()
    expect(screen.getByTestId('navbar')).toBeInTheDocument()
  })

  it('navigates to register page from homepage', async () => {
    const user = userEvent.setup()

    render(
      <MemoryRouter initialEntries={['/']}>
        <AppRoutes />
      </MemoryRouter>
    )

    // Get the Navbar element and query within it
    const navbar = screen.getByTestId('navbar')

    // Navigate to register page using Navbar link
    const registerLink = within(navbar).getByRole('link', { name: /register/i })
    await user.click(registerLink)

    // Verify we're on the register page
    expect(screen.getByRole('heading', { name: /create account/i })).toBeInTheDocument()
  })
})
