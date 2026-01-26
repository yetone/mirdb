/**
 * Home Page Unit Tests
 * Owner: Shared - Scenario 1 (hero tests), other scenarios for other components
 *
 * Tests for the Home page component to verify:
 * - Hero section integration
 * - CTA button navigation
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { render, screen, fireEvent, mockNavigate } from './setup'
import Home from '@/pages/Home'

describe('Home Page', () => {
  beforeEach(() => {
    mockNavigate.mockClear()
  })

  it('renders the Home component with hero section', () => {
    render(<Home />)

    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection).toBeInTheDocument()
  })

  it('contains heading element with product tagline text', () => {
    render(<Home />)

    const headline = screen.getByRole('heading', { level: 1 })
    expect(headline).toBeInTheDocument()
    expect(headline).toHaveTextContent('Shorten. Share. Track.')
  })

  it('displays both Get Started and Sign In buttons', () => {
    render(<Home />)

    const getStartedButton = screen.getByTestId('get-started-button')
    const signInButton = screen.getByTestId('sign-in-button')

    expect(getStartedButton).toBeInTheDocument()
    expect(signInButton).toBeInTheDocument()
  })

  it('navigates to /register when Get Started button is clicked', () => {
    render(<Home />)

    const getStartedButton = screen.getByTestId('get-started-button')
    fireEvent.click(getStartedButton)

    expect(mockNavigate).toHaveBeenCalledWith('/register')
  })

  it('navigates to /login when Sign In button is clicked', () => {
    render(<Home />)

    const signInButton = screen.getByTestId('sign-in-button')
    fireEvent.click(signInButton)

    expect(mockNavigate).toHaveBeenCalledWith('/login')
  })

  it('renders the navbar component', () => {
    render(<Home />)

    // Use getAllByRole since there are multiple navigation elements (navbar and footer)
    const navElements = screen.getAllByRole('navigation')
    expect(navElements.length).toBeGreaterThanOrEqual(1)
    // The first navigation element should be the navbar
    expect(navElements[0]).toHaveClass('navbar')
  })

  it('has the main content area', () => {
    render(<Home />)

    const main = screen.getByRole('main')
    expect(main).toBeInTheDocument()
  })
})

/**
 * Navigation Header Tests
 * Owner: Scenario 6 - Navigation Header
 *
 * Tests for the Navigation Header component to verify:
 * - Navigation header is present in the DOM
 * - Logo/brand element exists
 * - Navigation links exist (Features, How It Works)
 * - Auth buttons (Sign In, Get Started) are present
 */
describe('Navigation Header', () => {
  beforeEach(() => {
    mockNavigate.mockClear()
  })

  it('renders navigation header in the DOM', () => {
    render(<Home />)

    // Use getAllByRole since there are multiple navigation elements (navbar and footer)
    const navElements = screen.getAllByRole('navigation')
    expect(navElements.length).toBeGreaterThanOrEqual(1)
    // The first navigation element should be the navbar
    expect(navElements[0]).toHaveClass('navbar')
  })

  it('displays logo/brand element', () => {
    render(<Home />)

    const brandLink = screen.getByRole('link', { name: /urlshortener/i })
    expect(brandLink).toBeInTheDocument()
  })

  it('has Features anchor link with correct href', () => {
    render(<Home />)

    const featuresLink = screen.getByRole('link', { name: /features/i })
    expect(featuresLink).toBeInTheDocument()
    expect(featuresLink).toHaveAttribute('href', '#features')
  })

  it('has How It Works anchor link with correct href', () => {
    render(<Home />)

    const howItWorksLink = screen.getByRole('link', { name: /how it works/i })
    expect(howItWorksLink).toBeInTheDocument()
    expect(howItWorksLink).toHaveAttribute('href', '#how-it-works')
  })

  it('displays Sign In button in the header', () => {
    render(<Home />)

    const signInButton = screen.getByTestId('sign-in-nav')
    expect(signInButton).toBeInTheDocument()
    expect(signInButton).toHaveTextContent('Sign In')
  })

  it('displays Get Started button in the header', () => {
    render(<Home />)

    const getStartedButton = screen.getByTestId('get-started-nav')
    expect(getStartedButton).toBeInTheDocument()
    expect(getStartedButton).toHaveTextContent('Get Started')
  })

  it('Sign In nav button navigates to /login when clicked', () => {
    render(<Home />)

    const signInButton = screen.getByTestId('sign-in-nav')
    fireEvent.click(signInButton)

    expect(mockNavigate).toHaveBeenCalledWith('/login')
  })

  it('Get Started nav button navigates to /register when clicked', () => {
    render(<Home />)

    const getStartedButton = screen.getByTestId('get-started-nav')
    fireEvent.click(getStartedButton)

    expect(mockNavigate).toHaveBeenCalledWith('/register')
  })
})

/**
 * Accessibility - Screen Reader Compatibility Tests
 * Owner: Scenario 12 - Accessibility - Screen Reader Compatibility
 *
 * Tests for screen reader accessibility to verify:
 * - Exactly one h1 element (main headline)
 * - Headings follow logical order (h1 > h2 > h3, no skipping)
 * - All images have non-empty alt text or are marked decorative
 * - Navigation uses <nav> element or role='navigation'
 * - Main content uses <main> element or role='main'
 */
describe('Accessibility - Screen Reader Compatibility', () => {
  beforeEach(() => {
    mockNavigate.mockClear()
  })

  it('page has exactly one h1 element (main headline)', () => {
    render(<Home />)

    const h1Elements = screen.getAllByRole('heading', { level: 1 })
    expect(h1Elements).toHaveLength(1)
    expect(h1Elements[0]).toHaveTextContent('Shorten. Share. Track.')
  })

  it('headings follow logical order (h1 > h2 > h3, no skipping)', () => {
    const { container } = render(<Home />)

    // Get all heading elements in document order
    const allHeadings = container.querySelectorAll('h1, h2, h3, h4, h5, h6')
    const headingLevels = Array.from(allHeadings).map((h) =>
      parseInt(h.tagName[1], 10)
    )

    // Verify there's at least an h1
    expect(headingLevels[0]).toBe(1)

    // Verify no heading skips more than one level
    for (let i = 1; i < headingLevels.length; i++) {
      const currentLevel = headingLevels[i]
      const previousLevel = headingLevels[i - 1]

      // When going deeper, should not skip levels (e.g., h1 to h3 skips h2)
      if (currentLevel > previousLevel) {
        expect(currentLevel - previousLevel).toBeLessThanOrEqual(1)
      }
      // When going up (returning to higher level), any jump is acceptable
    }
  })

  it('all images have non-empty alt text or are marked decorative (aria-hidden)', () => {
    const { container } = render(<Home />)

    const images = container.querySelectorAll('img')
    const svgs = container.querySelectorAll('svg')

    // Check img elements have alt attributes
    images.forEach((img) => {
      const hasAlt = img.hasAttribute('alt')
      const isDecorative =
        img.getAttribute('alt') === '' ||
        img.getAttribute('aria-hidden') === 'true' ||
        img.getAttribute('role') === 'presentation'

      expect(hasAlt || isDecorative).toBe(true)
    })

    // Check SVG icons are properly marked as decorative or have accessible labels
    svgs.forEach((svg) => {
      const isDecorative = svg.getAttribute('aria-hidden') === 'true'
      const hasLabel =
        svg.hasAttribute('aria-label') ||
        svg.hasAttribute('aria-labelledby') ||
        svg.getAttribute('role') === 'img'

      // SVGs should either be decorative (aria-hidden) or have accessible labels
      expect(isDecorative || hasLabel).toBe(true)
    })
  })

  it('navigation uses <nav> element or role="navigation"', () => {
    const { container } = render(<Home />)

    // Check for nav elements
    const navElements = container.querySelectorAll('nav')
    const elementsWithNavRole = container.querySelectorAll('[role="navigation"]')

    // There should be at least one navigation landmark
    const totalNavLandmarks = navElements.length + elementsWithNavRole.length
    expect(totalNavLandmarks).toBeGreaterThanOrEqual(1)

    // Verify we can find navigation via role query
    const navigations = screen.getAllByRole('navigation')
    expect(navigations.length).toBeGreaterThanOrEqual(1)
  })

  it('main content uses <main> element or role="main"', () => {
    const { container } = render(<Home />)

    // Check for main element
    const mainElements = container.querySelectorAll('main')
    const elementsWithMainRole = container.querySelectorAll('[role="main"]')

    // There should be exactly one main landmark
    const totalMainLandmarks = mainElements.length + elementsWithMainRole.length
    expect(totalMainLandmarks).toBe(1)

    // Verify we can find main via role query
    const main = screen.getByRole('main')
    expect(main).toBeInTheDocument()
  })
})
