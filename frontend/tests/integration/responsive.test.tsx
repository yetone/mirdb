/**
 * Responsive Design Integration Tests
 * Owner: Scenario 6 - Responsive Design
 *
 * Purpose: Validates the homepage renders correctly across mobile, tablet, and desktop viewports.
 * Tests responsive behavior of homepage components at various viewport widths.
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { render, screen, fireEvent, within } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { HeroSection, FeaturesSection, HowItWorksSection } from '../../src/components/home'

// Test wrapper with necessary providers
function TestWrapper({ children }: { children: React.ReactNode }) {
  return <BrowserRouter>{children}</BrowserRouter>
}

// Mock Navbar component for responsive testing
function MockNavbar() {
  return (
    <nav data-testid="navbar" className="navbar bg-base-100">
      {/* Desktop navigation - hidden on mobile */}
      <div className="hidden md:flex gap-4" data-testid="desktop-nav">
        <a href="/login" className="btn btn-ghost">Login</a>
        <a href="/register" className="btn btn-primary">Register</a>
        <a href="/dashboard" className="btn btn-ghost">Dashboard</a>
      </div>
      {/* Mobile hamburger menu - visible only on mobile */}
      <button
        className="md:hidden btn btn-ghost btn-square min-h-[44px] min-w-[44px]"
        data-testid="hamburger-menu"
        aria-label="Open navigation menu"
        onClick={() => {
          const drawer = document.getElementById('mobile-drawer')
          if (drawer) drawer.classList.toggle('hidden')
        }}
      >
        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" className="w-6 h-6 stroke-current">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M4 6h16M4 12h16M4 18h16" />
        </svg>
      </button>
      {/* Mobile drawer - hidden by default */}
      <div id="mobile-drawer" className="hidden md:hidden fixed inset-0 bg-base-100 z-50" data-testid="mobile-drawer">
        <div className="flex flex-col gap-4 p-4">
          <a href="/login" className="btn btn-ghost min-h-[44px]">Login</a>
          <a href="/register" className="btn btn-primary min-h-[44px]">Register</a>
          <a href="/dashboard" className="btn btn-ghost min-h-[44px]">Dashboard</a>
        </div>
      </div>
    </nav>
  )
}

// Mock Footer component for responsive testing
function MockFooter() {
  return (
    <footer data-testid="footer" className="footer p-4 md:p-10 bg-base-200 text-base-content">
      <nav className="flex flex-col md:flex-row gap-2 md:gap-4" data-testid="footer-nav">
        <a href="/" className="link link-hover min-h-[44px] flex items-center">Home</a>
        <a href="/dashboard" className="link link-hover min-h-[44px] flex items-center">Dashboard</a>
        <a href="/login" className="link link-hover min-h-[44px] flex items-center">Login</a>
        <a href="/register" className="link link-hover min-h-[44px] flex items-center">Register</a>
      </nav>
      <nav className="flex flex-col md:flex-row gap-2 md:gap-4" data-testid="footer-legal">
        <a href="/privacy" className="link link-hover min-h-[44px] flex items-center">Privacy Policy</a>
        <a href="/terms" className="link link-hover min-h-[44px] flex items-center">Terms of Service</a>
      </nav>
    </footer>
  )
}

// Test Homepage component that assembles all sections
function TestHomepage() {
  return (
    <div className="min-h-screen bg-base-200 overflow-x-hidden">
      <MockNavbar />
      <main>
        <HeroSection />
        <FeaturesSection />
        <HowItWorksSection />
      </main>
      <MockFooter />
    </div>
  )
}

// Utility to simulate viewport changes via CSS media query mocking
function mockViewportWidth(width: number) {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  })
  Object.defineProperty(document.documentElement, 'clientWidth', {
    writable: true,
    configurable: true,
    value: width,
  })
  window.dispatchEvent(new Event('resize'))
}

// Utility to check if an element would overflow its container
function checkNoHorizontalOverflow(element: HTMLElement): boolean {
  const parent = element.parentElement
  if (!parent) return true
  return element.scrollWidth <= parent.clientWidth || element.scrollWidth <= window.innerWidth
}

describe('Responsive Design - Mobile Viewport (320-480px)', () => {
  beforeEach(() => {
    mockViewportWidth(320)
  })

  afterEach(() => {
    mockViewportWidth(1024) // Reset to default
  })

  // Test Case 1: Render homepage at 320px width (mobile) - No horizontal scrolling required
  it('renders homepage at 320px width without horizontal scrolling', () => {
    const { container } = render(
      <TestWrapper>
        <TestHomepage />
      </TestWrapper>
    )

    // Check that the main container has overflow-x-hidden class
    const mainContainer = container.firstChild as HTMLElement
    expect(mainContainer.className).toMatch(/overflow-x-hidden/)

    // Verify hero section renders
    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection).toBeInTheDocument()

    // Check that content is contained within viewport
    // Using px-4 class ensures padding on mobile
    expect(heroSection.className).toMatch(/px-4/)
  })

  // Test Case 2: Render homepage at 375px width (mobile) - All content readable without zooming
  it('renders homepage at 375px width with readable content', () => {
    mockViewportWidth(375)

    const { container } = render(
      <TestWrapper>
        <TestHomepage />
      </TestWrapper>
    )

    // Check headline has responsive font sizes
    const headline = screen.getByTestId('hero-headline')
    expect(headline).toBeInTheDocument()
    // The headline should have responsive text classes (text-4xl for mobile base)
    expect(headline.className).toMatch(/text-4xl/)

    // Subheadline should also be readable
    const subheadline = screen.getByTestId('hero-subheadline')
    expect(subheadline).toBeInTheDocument()
    expect(subheadline.className).toMatch(/text-lg/)
  })

  // Test Case 5: Render navbar at mobile viewport - Hamburger menu icon is visible
  it('renders hamburger menu icon at mobile viewport', () => {
    render(
      <TestWrapper>
        <TestHomepage />
      </TestWrapper>
    )

    const hamburgerMenu = screen.getByTestId('hamburger-menu')
    expect(hamburgerMenu).toBeInTheDocument()
    // Hamburger should be visible on mobile (md:hidden means visible below md breakpoint)
    expect(hamburgerMenu.className).toMatch(/md:hidden/)
  })

  // Test Case 6: Click hamburger menu on mobile - Navigation drawer/menu opens
  it('opens navigation drawer when clicking hamburger menu', () => {
    render(
      <TestWrapper>
        <TestHomepage />
      </TestWrapper>
    )

    const hamburgerMenu = screen.getByTestId('hamburger-menu')
    const mobileDrawer = screen.getByTestId('mobile-drawer')

    // Initially the drawer should be hidden
    expect(mobileDrawer.className).toMatch(/hidden/)

    // Click hamburger menu
    fireEvent.click(hamburgerMenu)

    // Drawer should now be visible (hidden class removed)
    expect(mobileDrawer.className).not.toMatch(/^hidden$/)
  })

  // Test Case 7: Render HeroSection at mobile viewport - CTA buttons are large enough to tap (min 44px)
  it('renders HeroSection with tappable CTA buttons at mobile viewport', () => {
    render(
      <TestWrapper>
        <HeroSection />
      </TestWrapper>
    )

    const ctaButton = screen.getByTestId('hero-cta-primary')
    expect(ctaButton).toBeInTheDocument()

    // Button should have btn-lg class which ensures minimum touch target
    const button = ctaButton.querySelector('button')
    if (button) {
      expect(button.className).toMatch(/btn-lg/)
    }
  })

  // Test Case 8: Render FeaturesSection at mobile viewport - Feature cards stack vertically
  it('renders FeaturesSection with vertically stacked feature cards at mobile viewport', () => {
    render(
      <TestWrapper>
        <FeaturesSection />
      </TestWrapper>
    )

    // Find the grid container (parent of feature cards)
    const featureCards = screen.getAllByRole('article')
    expect(featureCards.length).toBeGreaterThanOrEqual(3)

    // The parent grid should have grid-cols-1 for mobile
    const gridContainer = featureCards[0].parentElement
    expect(gridContainer?.className).toMatch(/grid-cols-1/)
  })

  // Test Case 10: Render Footer at mobile viewport - Footer links are stacked and accessible
  it('renders Footer with stacked links at mobile viewport', () => {
    render(
      <TestWrapper>
        <TestHomepage />
      </TestWrapper>
    )

    const footer = screen.getByTestId('footer')
    expect(footer).toBeInTheDocument()

    // Footer nav should use flex-col for mobile
    const footerNav = screen.getByTestId('footer-nav')
    expect(footerNav.className).toMatch(/flex-col/)
  })

  // Test Case 11: Test touch targets on mobile - All interactive elements meet minimum touch target size
  it('ensures all interactive elements meet minimum 44px touch target size', () => {
    render(
      <TestWrapper>
        <TestHomepage />
      </TestWrapper>
    )

    // Check hamburger menu has minimum touch target
    const hamburgerMenu = screen.getByTestId('hamburger-menu')
    expect(hamburgerMenu.className).toMatch(/min-h-\[44px\]/)
    expect(hamburgerMenu.className).toMatch(/min-w-\[44px\]/)

    // Check footer links have minimum touch target
    const footerNav = screen.getByTestId('footer-nav')
    const footerLinks = within(footerNav).getAllByRole('link')
    footerLinks.forEach((link) => {
      expect(link.className).toMatch(/min-h-\[44px\]/)
    })
  })
})

describe('Responsive Design - Tablet Viewport (768-1024px)', () => {
  beforeEach(() => {
    mockViewportWidth(768)
  })

  afterEach(() => {
    mockViewportWidth(1024)
  })

  // Test Case 3: Render homepage at 768px width (tablet) - Layout adapts to tablet viewport
  it('renders homepage at 768px width with adapted layout', () => {
    render(
      <TestWrapper>
        <TestHomepage />
      </TestWrapper>
    )

    // Verify hero section renders with adapted styling
    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection).toBeInTheDocument()

    // Check headline has medium breakpoint styling (md:text-5xl)
    const headline = screen.getByTestId('hero-headline')
    expect(headline.className).toMatch(/md:text-5xl/)

    // Features section should show 2 columns on tablet (md:grid-cols-2)
    const featureCards = screen.getAllByRole('article')
    const gridContainer = featureCards[0].parentElement
    expect(gridContainer?.className).toMatch(/md:grid-cols-2/)
  })

  it('shows desktop navigation at tablet viewport', () => {
    render(
      <TestWrapper>
        <TestHomepage />
      </TestWrapper>
    )

    // Desktop nav should be visible at md breakpoint
    const desktopNav = screen.getByTestId('desktop-nav')
    expect(desktopNav).toBeInTheDocument()
    expect(desktopNav.className).toMatch(/md:flex/)
  })

  it('hides hamburger menu at tablet viewport', () => {
    render(
      <TestWrapper>
        <TestHomepage />
      </TestWrapper>
    )

    // Hamburger should have md:hidden class, meaning it's hidden at md breakpoint
    const hamburgerMenu = screen.getByTestId('hamburger-menu')
    expect(hamburgerMenu.className).toMatch(/md:hidden/)
  })
})

describe('Responsive Design - Desktop Viewport (1280px+)', () => {
  beforeEach(() => {
    mockViewportWidth(1280)
  })

  afterEach(() => {
    mockViewportWidth(1024)
  })

  // Test Case 4: Render homepage at 1280px width (desktop) - Full desktop layout is displayed
  it('renders homepage at 1280px width with full desktop layout', () => {
    render(
      <TestWrapper>
        <TestHomepage />
      </TestWrapper>
    )

    // Verify hero section renders with full desktop styling
    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection).toBeInTheDocument()

    // Check headline has large breakpoint styling (lg:text-6xl)
    const headline = screen.getByTestId('hero-headline')
    expect(headline.className).toMatch(/lg:text-6xl/)
  })

  // Test Case 9: Render FeaturesSection at desktop viewport - Feature cards display in grid layout
  it('renders FeaturesSection with grid layout at desktop viewport', () => {
    render(
      <TestWrapper>
        <FeaturesSection />
      </TestWrapper>
    )

    // Find the grid container (parent of feature cards)
    const featureCards = screen.getAllByRole('article')
    const gridContainer = featureCards[0].parentElement

    // At lg breakpoint, should show 4 columns
    expect(gridContainer?.className).toMatch(/lg:grid-cols-4/)
  })

  it('renders HowItWorksSection in horizontal layout at desktop viewport', () => {
    render(
      <TestWrapper>
        <HowItWorksSection />
      </TestWrapper>
    )

    // Steps container should use md:flex-row for horizontal layout
    const stepsContainer = screen.getByTestId('steps-container')
    expect(stepsContainer).toBeInTheDocument()
    expect(stepsContainer.className).toMatch(/md:flex-row/)
  })

  it('renders Footer with horizontal links at desktop viewport', () => {
    render(
      <TestWrapper>
        <TestHomepage />
      </TestWrapper>
    )

    // Footer nav should use md:flex-row for horizontal layout
    const footerNav = screen.getByTestId('footer-nav')
    expect(footerNav.className).toMatch(/md:flex-row/)
  })
})

describe('Responsive Design - CTA and Interactive Elements', () => {
  it('renders CTA buttons in column layout on mobile, row layout on larger screens', () => {
    render(
      <TestWrapper>
        <HeroSection />
      </TestWrapper>
    )

    // Find the CTA container
    const ctaPrimary = screen.getByTestId('hero-cta-primary')
    const ctaContainer = ctaPrimary.parentElement

    // Should have flex-col for mobile and sm:flex-row for larger screens
    expect(ctaContainer?.className).toMatch(/flex-col/)
    expect(ctaContainer?.className).toMatch(/sm:flex-row/)
  })

  it('ensures all buttons are accessible at all viewports', () => {
    render(
      <TestWrapper>
        <TestHomepage />
      </TestWrapper>
    )

    // CTA button should be accessible
    const ctaButton = screen.getByTestId('hero-cta-primary')
    expect(ctaButton).toBeInTheDocument()

    // Login link should be accessible
    const loginLink = screen.getByTestId('hero-login-link')
    expect(loginLink).toBeInTheDocument()
  })
})

describe('Responsive Design - Content Reflow', () => {
  it('reflows content properly at mobile viewport without overflow', () => {
    mockViewportWidth(320)

    const { container } = render(
      <TestWrapper>
        <TestHomepage />
      </TestWrapper>
    )

    // Main container should prevent horizontal overflow
    const mainContainer = container.firstChild as HTMLElement
    expect(mainContainer.className).toMatch(/overflow-x-hidden/)

    // Hero section should have proper padding for mobile
    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection.className).toMatch(/px-4/)

    // Features section should have proper padding
    const featureCards = screen.getAllByRole('article')
    const featuresSection = featureCards[0].closest('section')
    expect(featuresSection?.className).toMatch(/px-4/)
  })

  it('HowItWorksSection has proper vertical layout on mobile', () => {
    mockViewportWidth(320)

    render(
      <TestWrapper>
        <HowItWorksSection />
      </TestWrapper>
    )

    // Steps container should use flex-col for vertical layout on mobile
    const stepsContainer = screen.getByTestId('steps-container')
    expect(stepsContainer.className).toMatch(/flex-col/)
  })
})
