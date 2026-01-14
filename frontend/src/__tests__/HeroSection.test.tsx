import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter, MemoryRouter, Routes, Route } from 'react-router-dom'
import HeroSection from '../components/HeroSection'
import Home from '../pages/Home'

const renderWithRouter = (component: React.ReactElement) => {
  return render(<BrowserRouter>{component}</BrowserRouter>)
}

describe('HeroSection', () => {
  // Test Case 1: Hero section renders with h1 headline containing URL shortening related text
  it('renders hero section with h1 headline containing URL shortening related text', () => {
    renderWithRouter(<Home />)

    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection).toBeInTheDocument()

    const headline = screen.getByRole('heading', { level: 1 })
    expect(headline).toBeInTheDocument()
    expect(headline.textContent?.toLowerCase()).toMatch(/shorten|url|link/)
  })

  // Test Case 2: Primary CTA button with text 'Get Started' or 'Get Started Free' is present
  it('renders primary CTA button with Get Started text', () => {
    renderWithRouter(<HeroSection />)

    const getStartedButton = screen.getByTestId('cta-get-started')
    expect(getStartedButton).toBeInTheDocument()
    expect(getStartedButton.textContent).toMatch(/get started/i)

    // Verify it's styled as primary button (has btn-primary class)
    expect(getStartedButton).toHaveClass('btn-primary')
  })

  // Test Case 3: Secondary CTA button with text 'Login' is present and styled differently from primary
  it('renders secondary Login button styled differently from primary', () => {
    renderWithRouter(<HeroSection />)

    const loginButton = screen.getByTestId('cta-login')
    expect(loginButton).toBeInTheDocument()
    expect(loginButton.textContent).toMatch(/login/i)

    // Verify it's styled as outline button (different from primary)
    expect(loginButton).toHaveClass('btn-outline')
    expect(loginButton).not.toHaveClass('btn-primary')

    // Get the primary button to compare
    const getStartedButton = screen.getByTestId('cta-get-started')

    // Verify they have different styling classes
    expect(getStartedButton.className).not.toBe(loginButton.className)
  })

  // Test Case 4: Subheadline text explaining the benefit of the service is displayed below headline
  it('renders subheadline text explaining the benefit of the service', () => {
    renderWithRouter(<HeroSection />)

    const subheadline = screen.getByTestId('hero-subheadline')
    expect(subheadline).toBeInTheDocument()

    // Verify subheadline contains benefit-oriented text
    const subheadlineText = subheadline.textContent?.toLowerCase() || ''
    expect(
      subheadlineText.includes('link') ||
        subheadlineText.includes('analytics') ||
        subheadlineText.includes('insight') ||
        subheadlineText.includes('track') ||
        subheadlineText.includes('short')
    ).toBe(true)

    // Verify subheadline appears after headline in the DOM structure
    const headline = screen.getByRole('heading', { level: 1 })
    const heroSection = screen.getByTestId('hero-section')

    // Both should be within hero section
    expect(heroSection).toContainElement(headline)
    expect(heroSection).toContainElement(subheadline)
  })

  // Additional test: CTA buttons have correct navigation links
  it('CTA buttons have correct navigation links', () => {
    renderWithRouter(<HeroSection />)

    const getStartedButton = screen.getByTestId('cta-get-started')
    const loginButton = screen.getByTestId('cta-login')

    expect(getStartedButton).toHaveAttribute('href', '/register')
    expect(loginButton).toHaveAttribute('href', '/login')
  })
})

// Scenario Test Case 1: Integration test - Click 'Get Started' button navigates to /register
describe('Get Started CTA Navigation Integration', () => {
  it("navigates to /register when 'Get Started' button is clicked", async () => {
    const user = userEvent.setup()

    render(
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route path="/" element={<HeroSection />} />
          <Route
            path="/register"
            element={<div data-testid="register-page">Register Page</div>}
          />
        </Routes>
      </MemoryRouter>
    )

    const getStartedButton = screen.getByTestId('cta-get-started')
    await user.click(getStartedButton)

    // Verify navigation occurred
    expect(screen.getByTestId('register-page')).toBeInTheDocument()
  })

  // Scenario Test Case 3: Integration test - Keyboard accessibility (Enter key)
  it("navigates to /register when Enter key is pressed on focused 'Get Started' button", async () => {
    const user = userEvent.setup()

    render(
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route path="/" element={<HeroSection />} />
          <Route
            path="/register"
            element={<div data-testid="register-page">Register Page</div>}
          />
        </Routes>
      </MemoryRouter>
    )

    const getStartedButton = screen.getByTestId('cta-get-started')

    // Focus the button and press Enter
    getStartedButton.focus()
    expect(document.activeElement).toBe(getStartedButton)

    await user.keyboard('{Enter}')

    // Verify navigation occurred via keyboard
    expect(screen.getByTestId('register-page')).toBeInTheDocument()
  })

  it("allows Tab navigation to 'Get Started' button for accessibility", async () => {
    const user = userEvent.setup()

    render(
      <MemoryRouter initialEntries={['/']}>
        <HeroSection />
      </MemoryRouter>
    )

    // Tab to the Get Started button
    await user.tab()

    const getStartedButton = screen.getByTestId('cta-get-started')
    expect(document.activeElement).toBe(getStartedButton)
  })
})
