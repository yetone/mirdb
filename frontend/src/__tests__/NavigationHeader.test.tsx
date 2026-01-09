import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import NavigationHeader from '../components/NavigationHeader'
import ThemeToggle from '../components/ThemeToggle'
import Home from '../pages/Home'

// Helper to render with Router
const renderWithRouter = (ui: React.ReactElement, { route = '/' } = {}) => {
  return render(<MemoryRouter initialEntries={[route]}>{ui}</MemoryRouter>)
}

describe('NavigationHeader', () => {
  beforeEach(() => {
    // Reset document theme before each test
    document.documentElement.setAttribute('data-theme', 'light')
    localStorage.clear()
  })

  // Test Case 1: Header renders with logo, navigation links, theme toggle, and auth buttons
  describe('Test Case 1: Header renders with all required elements', () => {
    it('renders the navigation header component', () => {
      renderWithRouter(<NavigationHeader />)

      const header = screen.getByTestId('navigation-header')
      expect(header).toBeInTheDocument()
    })

    it('renders the logo/brand name', () => {
      renderWithRouter(<NavigationHeader />)

      const logo = screen.getByTestId('nav-logo')
      expect(logo).toBeInTheDocument()
      expect(logo).toHaveTextContent('URL Shortener')
    })

    it('renders Features navigation link', () => {
      renderWithRouter(<NavigationHeader />)

      const featuresLink = screen.getByTestId('nav-features')
      expect(featuresLink).toBeInTheDocument()
      expect(featuresLink).toHaveTextContent('Features')
    })

    it('renders How It Works navigation link', () => {
      renderWithRouter(<NavigationHeader />)

      const howItWorksLink = screen.getByTestId('nav-how-it-works')
      expect(howItWorksLink).toBeInTheDocument()
      expect(howItWorksLink).toHaveTextContent('How It Works')
    })

    it('renders theme toggle button', () => {
      renderWithRouter(<NavigationHeader />)

      // There may be multiple theme toggles (desktop and mobile), check at least one exists
      const themeToggles = screen.getAllByTestId('theme-toggle')
      expect(themeToggles.length).toBeGreaterThanOrEqual(1)
      expect(themeToggles[0]).toBeInTheDocument()
    })

    it('renders Log In button', () => {
      renderWithRouter(<NavigationHeader />)

      const loginButton = screen.getByTestId('nav-login')
      expect(loginButton).toBeInTheDocument()
      expect(loginButton).toHaveTextContent('Log In')
    })

    it('renders Sign Up button', () => {
      renderWithRouter(<NavigationHeader />)

      const signupButton = screen.getByTestId('nav-signup')
      expect(signupButton).toBeInTheDocument()
      expect(signupButton).toHaveTextContent('Sign Up')
    })

    it('has proper navigation role and aria-label', () => {
      renderWithRouter(<NavigationHeader />)

      const nav = screen.getByRole('navigation', { name: /main navigation/i })
      expect(nav).toBeInTheDocument()
    })
  })

  // Test Case 2: Click on logo navigates to homepage
  describe('Test Case 2: Logo navigation', () => {
    it('logo links to homepage (/)', () => {
      renderWithRouter(<NavigationHeader />)

      const logo = screen.getByTestId('nav-logo')
      expect(logo).toHaveAttribute('href', '/')
    })

    it('logo has proper aria-label for accessibility', () => {
      renderWithRouter(<NavigationHeader />)

      const logo = screen.getByTestId('nav-logo')
      expect(logo).toHaveAttribute('aria-label', 'URL Shortener - Go to homepage')
    })
  })

  // Test Case 3: Click Features navigation link scrolls to Features section
  describe('Test Case 3: Features navigation', () => {
    it('clicking Features calls scroll handler with features id', () => {
      const mockScrollHandler = vi.fn()
      renderWithRouter(<NavigationHeader onScrollToSection={mockScrollHandler} />)

      const featuresLink = screen.getByTestId('nav-features')
      fireEvent.click(featuresLink)

      expect(mockScrollHandler).toHaveBeenCalledWith('features')
    })

    it('clicking Features calls scrollIntoView when no handler provided', () => {
      const mockScrollIntoView = vi.fn()
      const featuresSection = document.createElement('div')
      featuresSection.id = 'features'
      featuresSection.scrollIntoView = mockScrollIntoView
      document.body.appendChild(featuresSection)

      renderWithRouter(<NavigationHeader />)

      const featuresLink = screen.getByTestId('nav-features')
      fireEvent.click(featuresLink)

      expect(mockScrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' })

      document.body.removeChild(featuresSection)
    })
  })

  // Test Case 4: Click How It Works navigation link scrolls to How It Works section
  describe('Test Case 4: How It Works navigation', () => {
    it('clicking How It Works calls scroll handler with how-it-works id', () => {
      const mockScrollHandler = vi.fn()
      renderWithRouter(<NavigationHeader onScrollToSection={mockScrollHandler} />)

      const howItWorksLink = screen.getByTestId('nav-how-it-works')
      fireEvent.click(howItWorksLink)

      expect(mockScrollHandler).toHaveBeenCalledWith('how-it-works')
    })

    it('clicking How It Works calls scrollIntoView when no handler provided', () => {
      const mockScrollIntoView = vi.fn()
      const howItWorksSection = document.createElement('div')
      howItWorksSection.id = 'how-it-works'
      howItWorksSection.scrollIntoView = mockScrollIntoView
      document.body.appendChild(howItWorksSection)

      renderWithRouter(<NavigationHeader />)

      const howItWorksLink = screen.getByTestId('nav-how-it-works')
      fireEvent.click(howItWorksLink)

      expect(mockScrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' })

      document.body.removeChild(howItWorksSection)
    })
  })

  // Test Case 5: Click Log In button navigates to /login
  describe('Test Case 5: Log In navigation', () => {
    it('Log In button links to /login page', () => {
      renderWithRouter(<NavigationHeader />)

      const loginButton = screen.getByTestId('nav-login')
      expect(loginButton).toHaveAttribute('href', '/login')
    })
  })

  // Test Case 6: Click Sign Up button navigates to /register
  describe('Test Case 6: Sign Up navigation', () => {
    it('Sign Up button links to /register page', () => {
      renderWithRouter(<NavigationHeader />)

      const signupButton = screen.getByTestId('nav-signup')
      expect(signupButton).toHaveAttribute('href', '/register')
    })

    it('Sign Up button has primary styling', () => {
      renderWithRouter(<NavigationHeader />)

      const signupButton = screen.getByTestId('nav-signup')
      expect(signupButton).toHaveClass('btn-primary')
    })
  })

  // Test Case 7: Theme toggle button is present and clickable
  describe('Test Case 7: Theme toggle functionality', () => {
    it('theme toggle button is present', () => {
      renderWithRouter(<NavigationHeader />)

      // There may be multiple theme toggles (desktop and mobile)
      const themeToggles = screen.getAllByTestId('theme-toggle')
      expect(themeToggles.length).toBeGreaterThanOrEqual(1)
      expect(themeToggles[0]).toBeInTheDocument()
    })

    it('theme toggle button is clickable', () => {
      renderWithRouter(<NavigationHeader />)

      const themeToggles = screen.getAllByTestId('theme-toggle')
      const themeToggle = themeToggles[0]
      expect(themeToggle).not.toBeDisabled()

      // Should not throw when clicked
      expect(() => fireEvent.click(themeToggle)).not.toThrow()
    })

    it('clicking theme toggle changes the theme', () => {
      renderWithRouter(<NavigationHeader />)

      const themeToggles = screen.getAllByTestId('theme-toggle')
      const themeToggle = themeToggles[0]

      // Initial theme is light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light')

      // Click to change theme
      fireEvent.click(themeToggle)

      // Theme should change to dark
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark')
    })

    it('theme toggle has proper aria-label for accessibility', () => {
      renderWithRouter(<NavigationHeader />)

      const themeToggles = screen.getAllByTestId('theme-toggle')
      const themeToggle = themeToggles[0]
      expect(themeToggle).toHaveAttribute('aria-label')
      expect(themeToggle.getAttribute('aria-label')).toContain('theme')
    })
  })

  // Additional integration tests
  describe('Integration with Home page', () => {
    it('NavigationHeader renders correctly within Home page', () => {
      render(
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      )

      const header = screen.getByTestId('navigation-header')
      expect(header).toBeInTheDocument()
    })

    it('navigation links work with Home page sections', () => {
      const scrollIntoViewMock = vi.fn()
      Element.prototype.scrollIntoView = scrollIntoViewMock

      render(
        <BrowserRouter>
          <Home />
        </BrowserRouter>
      )

      // Test Features link
      const featuresLink = screen.getByTestId('nav-features')
      fireEvent.click(featuresLink)
      expect(scrollIntoViewMock).toHaveBeenCalledWith({ behavior: 'smooth' })

      scrollIntoViewMock.mockClear()

      // Test How It Works link
      const howItWorksLink = screen.getByTestId('nav-how-it-works')
      fireEvent.click(howItWorksLink)
      expect(scrollIntoViewMock).toHaveBeenCalledWith({ behavior: 'smooth' })
    })
  })

  // Accessibility tests
  describe('Accessibility', () => {
    it('header is wrapped in header element', () => {
      renderWithRouter(<NavigationHeader />)

      const header = document.querySelector('header')
      expect(header).toBeInTheDocument()
    })

    it('navigation has proper role', () => {
      renderWithRouter(<NavigationHeader />)

      const nav = screen.getByRole('navigation')
      expect(nav).toBeInTheDocument()
    })

    it('all buttons and links are keyboard accessible', () => {
      renderWithRouter(<NavigationHeader />)

      const logo = screen.getByTestId('nav-logo')
      const featuresLink = screen.getByTestId('nav-features')
      const howItWorksLink = screen.getByTestId('nav-how-it-works')
      const themeToggles = screen.getAllByTestId('theme-toggle')
      const loginButton = screen.getByTestId('nav-login')
      const signupButton = screen.getByTestId('nav-signup')

      // All should be focusable
      expect(logo.tagName).toBe('A')
      expect(featuresLink.tagName).toBe('BUTTON')
      expect(howItWorksLink.tagName).toBe('BUTTON')
      expect(themeToggles[0].tagName).toBe('BUTTON')
      expect(loginButton.tagName).toBe('A')
      expect(signupButton.tagName).toBe('A')
    })
  })
})

// Separate tests for ThemeToggle component
describe('ThemeToggle Component', () => {
  beforeEach(() => {
    document.documentElement.setAttribute('data-theme', 'light')
    localStorage.clear()
  })

  it('renders the theme toggle button', () => {
    renderWithRouter(<ThemeToggle />)

    const button = screen.getByTestId('theme-toggle')
    expect(button).toBeInTheDocument()
  })

  it('has circular button styling', () => {
    renderWithRouter(<ThemeToggle />)

    const button = screen.getByTestId('theme-toggle')
    expect(button).toHaveClass('btn-circle')
  })

  it('cycles through themes on click', () => {
    renderWithRouter(<ThemeToggle />)

    const button = screen.getByTestId('theme-toggle')

    // Start with light
    expect(document.documentElement.getAttribute('data-theme')).toBe('light')

    // Click to dark
    fireEvent.click(button)
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark')

    // Click to cyberpunk
    fireEvent.click(button)
    expect(document.documentElement.getAttribute('data-theme')).toBe('cyberpunk')

    // Click to synthwave
    fireEvent.click(button)
    expect(document.documentElement.getAttribute('data-theme')).toBe('synthwave')

    // Click back to light
    fireEvent.click(button)
    expect(document.documentElement.getAttribute('data-theme')).toBe('light')
  })

  it('saves theme to localStorage', () => {
    renderWithRouter(<ThemeToggle />)

    const button = screen.getByTestId('theme-toggle')

    fireEvent.click(button)

    expect(localStorage.getItem('theme')).toBe('dark')
  })

  it('displays appropriate icon for each theme', () => {
    renderWithRouter(<ThemeToggle />)

    const button = screen.getByTestId('theme-toggle')

    // Should have an SVG icon
    let svg = button.querySelector('svg')
    expect(svg).toBeInTheDocument()

    // Click and check icon changes
    fireEvent.click(button)
    svg = button.querySelector('svg')
    expect(svg).toBeInTheDocument()
  })
})
