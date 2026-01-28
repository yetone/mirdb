/**
 * Accessibility Unit Tests
 * Owner: Scenario 7 - Accessibility Compliance
 *
 * Tests for semantic HTML structure, heading hierarchy, and accessible labels
 * as specified in NFR-3 (WCAG 2.1 AA) and US-7 (Keyboard Navigation).
 *
 * Test Cases:
 * 5. Semantic HTML structure - nav, main, section, footer elements
 * 6. Heading hierarchy - h1 for main headline, h2 for sections
 * 7. Feature card icons - aria-label or aria-hidden with text alternatives
 */

import { describe, test, expect, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import React from 'react'
import { Home } from '@/pages/Home'
import { HeroSection } from '@/components/homepage/HeroSection'
import { FeaturesSection } from '@/components/homepage/FeaturesSection'
import { HowItWorksSection } from '@/components/homepage/HowItWorksSection'
import { Footer } from '@/components/homepage/Footer'
import { Navbar } from '@/components/Navbar'
import { AuthContext } from '@/contexts/AuthContext'

// Mock auth context
const mockAuthContext = {
  user: null,
  isAuthenticated: false,
  isLoading: false,
  login: vi.fn(),
  logout: vi.fn(),
  register: vi.fn(),
}

function renderWithProviders(ui: React.ReactElement) {
  return render(
    <AuthContext.Provider value={mockAuthContext}>
      <BrowserRouter>{ui}</BrowserRouter>
    </AuthContext.Provider>
  )
}

describe('TC5: Semantic HTML Structure', () => {
  test('Homepage contains nav element with role="navigation"', () => {
    renderWithProviders(<Home />)

    const navElement = screen.getByRole('navigation', { name: /main navigation/i })
    expect(navElement).toBeInTheDocument()
    expect(navElement.tagName.toLowerCase()).toBe('nav')
  })

  test('Homepage contains main element', () => {
    renderWithProviders(<Home />)

    const mainElement = screen.getByRole('main')
    expect(mainElement).toBeInTheDocument()
    expect(mainElement.tagName.toLowerCase()).toBe('main')
  })

  test('Homepage contains section elements with proper aria-labelledby', () => {
    renderWithProviders(<Home />)

    // Hero section
    const heroSection = document.querySelector('section[aria-labelledby="hero-title"]')
    expect(heroSection).toBeInTheDocument()

    // Features section
    const featuresSection = document.querySelector('section[aria-labelledby="features-title"]')
    expect(featuresSection).toBeInTheDocument()

    // How it works section
    const howItWorksSection = document.querySelector('section[aria-labelledby="how-it-works-title"]')
    expect(howItWorksSection).toBeInTheDocument()
  })

  test('Homepage contains footer element with role="contentinfo"', () => {
    renderWithProviders(<Home />)

    const footerElement = screen.getByRole('contentinfo')
    expect(footerElement).toBeInTheDocument()
    expect(footerElement.tagName.toLowerCase()).toBe('footer')
  })

  test('Navbar component uses nav element with aria-label', () => {
    renderWithProviders(<Navbar />)

    const nav = screen.getByRole('navigation', { name: /main navigation/i })
    expect(nav).toBeInTheDocument()
    expect(nav).toHaveAttribute('aria-label', 'Main navigation')
  })

  test('Footer component uses footer element with role attribute', () => {
    renderWithProviders(<Footer />)

    const footer = screen.getByRole('contentinfo')
    expect(footer).toBeInTheDocument()
    expect(footer).toHaveAttribute('role', 'contentinfo')
  })

  test('Footer contains navigation with aria-label', () => {
    renderWithProviders(<Footer />)

    const footerNav = screen.getByRole('navigation', { name: /footer navigation/i })
    expect(footerNav).toBeInTheDocument()
  })
})

describe('TC6: Heading Hierarchy', () => {
  test('Homepage has exactly one h1 element', () => {
    renderWithProviders(<Home />)

    const h1Elements = screen.getAllByRole('heading', { level: 1 })
    expect(h1Elements).toHaveLength(1)
  })

  test('h1 contains meaningful text about the service', () => {
    renderWithProviders(<Home />)

    const h1 = screen.getByRole('heading', { level: 1 })
    expect(h1).toHaveTextContent(/shorten urls|track performance/i)
  })

  test('HeroSection has h1 with proper id for aria-labelledby', () => {
    render(
      <BrowserRouter>
        <HeroSection
          isAuthenticated={false}
          onGetStarted={() => {}}
          onLogin={() => {}}
        />
      </BrowserRouter>
    )

    const h1 = screen.getByRole('heading', { level: 1 })
    expect(h1).toHaveAttribute('id', 'hero-title')
  })

  test('Section titles use h2 elements', () => {
    renderWithProviders(<Home />)

    const h2Elements = screen.getAllByRole('heading', { level: 2 })
    expect(h2Elements.length).toBeGreaterThanOrEqual(2)

    // Check that section titles are h2
    const featuresSectionTitle = screen.getByText('Powerful Features')
    expect(featuresSectionTitle.tagName.toLowerCase()).toBe('h2')

    const howItWorksSectionTitle = screen.getByText('How It Works')
    expect(howItWorksSectionTitle.tagName.toLowerCase()).toBe('h2')
  })

  test('FeaturesSection has h2 with proper id for aria-labelledby', () => {
    render(<FeaturesSection />)

    const h2 = screen.getByRole('heading', { level: 2 })
    expect(h2).toHaveAttribute('id', 'features-title')
    expect(h2).toHaveTextContent('Powerful Features')
  })

  test('HowItWorksSection has h2 with proper id for aria-labelledby', () => {
    render(<HowItWorksSection />)

    const h2 = screen.getByRole('heading', { level: 2 })
    expect(h2).toHaveAttribute('id', 'how-it-works-title')
    expect(h2).toHaveTextContent('How It Works')
  })

  test('Feature cards use h3 for titles (proper hierarchy under h2)', () => {
    render(<FeaturesSection />)

    const h3Elements = screen.getAllByRole('heading', { level: 3 })
    expect(h3Elements.length).toBeGreaterThanOrEqual(4)

    // Check feature card titles
    expect(screen.getByRole('heading', { level: 3, name: /url shortening/i })).toBeInTheDocument()
    expect(screen.getByRole('heading', { level: 3, name: /click analytics/i })).toBeInTheDocument()
    expect(screen.getByRole('heading', { level: 3, name: /geographic insights/i })).toBeInTheDocument()
    expect(screen.getByRole('heading', { level: 3, name: /share statistics/i })).toBeInTheDocument()
  })

  test('How It Works steps use h3 for titles', () => {
    render(<HowItWorksSection />)

    const h3Elements = screen.getAllByRole('heading', { level: 3 })
    expect(h3Elements.length).toBe(3)

    expect(screen.getByRole('heading', { level: 3, name: /paste your long url/i })).toBeInTheDocument()
    expect(screen.getByRole('heading', { level: 3, name: /get your short link/i })).toBeInTheDocument()
    expect(screen.getByRole('heading', { level: 3, name: /track performance/i })).toBeInTheDocument()
  })
})

describe('TC7: Feature Card Icons Accessibility', () => {
  test('Feature icons have visible text alternatives via titles', () => {
    render(<FeaturesSection />)

    // Each feature card has an icon and a visible title text
    const featureCards = screen.getAllByTestId(/feature-card-/)
    expect(featureCards).toHaveLength(4)

    featureCards.forEach((card) => {
      // Each card should have a visible title (h3)
      const title = within(card).getByRole('heading', { level: 3 })
      expect(title).toBeVisible()
      expect(title.textContent?.length).toBeGreaterThan(0)

      // Each card should have a description
      const description = within(card).getByText(/.{20,}/) // Text with at least 20 chars
      expect(description).toBeVisible()
    })
  })

  test('Feature icons are presentational (have visible text context)', () => {
    render(<FeaturesSection />)

    // Icons are emoji characters in divs with data-testid="feature-icon"
    const iconElements = screen.getAllByTestId('feature-icon')
    expect(iconElements).toHaveLength(4)

    iconElements.forEach((icon) => {
      // Icon should have content
      expect(icon.textContent?.length).toBeGreaterThan(0)

      // The icon's parent card should have a visible title
      const card = icon.closest('[data-testid^="feature-card-"]')
      expect(card).toBeInTheDocument()

      if (card) {
        const title = within(card as HTMLElement).getByRole('heading', { level: 3 })
        expect(title).toBeVisible()
      }
    })
  })

  test('How It Works step icons have visible text alternatives', () => {
    render(<HowItWorksSection />)

    // Each step has an icon and visible title/description
    const steps = document.querySelectorAll('.flex-1.text-center')
    expect(steps).toHaveLength(3)

    steps.forEach((step) => {
      // Each step should have a visible title (h3)
      const title = within(step as HTMLElement).getByRole('heading', { level: 3 })
      expect(title).toBeVisible()

      // Each step should have step badge
      const badge = within(step as HTMLElement).getByText(/Step \d/)
      expect(badge).toBeVisible()
    })
  })
})

describe('Accessibility - Button and Link Labels', () => {
  test('HeroSection buttons have accessible names', () => {
    render(
      <BrowserRouter>
        <HeroSection
          isAuthenticated={false}
          onGetStarted={() => {}}
          onLogin={() => {}}
        />
      </BrowserRouter>
    )

    // Get Started button
    const getStartedBtn = screen.getByRole('button', { name: /get started/i })
    expect(getStartedBtn).toBeInTheDocument()
    expect(getStartedBtn).toHaveAccessibleName()

    // Login button
    const loginBtn = screen.getByRole('button', { name: /log in/i })
    expect(loginBtn).toBeInTheDocument()
    expect(loginBtn).toHaveAccessibleName()
  })

  test('HeroSection shows Go to Dashboard for authenticated users', () => {
    render(
      <BrowserRouter>
        <HeroSection
          isAuthenticated={true}
          onGetStarted={() => {}}
          onLogin={() => {}}
        />
      </BrowserRouter>
    )

    // Dashboard button for authenticated users
    const dashboardBtn = screen.getByRole('button', { name: /go to dashboard/i })
    expect(dashboardBtn).toBeInTheDocument()
    expect(dashboardBtn).toHaveAccessibleName()
  })

  test('Footer links have accessible names', () => {
    renderWithProviders(<Footer />)

    const loginLink = screen.getByRole('link', { name: /login/i })
    expect(loginLink).toBeInTheDocument()
    expect(loginLink).toHaveAccessibleName()

    const registerLink = screen.getByRole('link', { name: /register/i })
    expect(registerLink).toBeInTheDocument()
    expect(registerLink).toHaveAccessibleName()
  })

  test('Navbar links have accessible names', () => {
    renderWithProviders(<Navbar />)

    // Brand/home link
    const homeLink = screen.getByRole('link', { name: /url shortener/i })
    expect(homeLink).toBeInTheDocument()

    // Login link
    const loginLink = screen.getByRole('link', { name: /log in/i })
    expect(loginLink).toBeInTheDocument()

    // Register/Sign up link
    const registerLink = screen.getByRole('link', { name: /sign up/i })
    expect(registerLink).toBeInTheDocument()
  })
})

describe('Accessibility - ARIA Attributes', () => {
  test('Interactive sections have proper aria-labelledby attributes', () => {
    renderWithProviders(<Home />)

    // Verify sections reference their headings via aria-labelledby
    const heroSection = document.querySelector('section[aria-labelledby="hero-title"]')
    const heroTitle = document.getElementById('hero-title')
    expect(heroSection).toBeInTheDocument()
    expect(heroTitle).toBeInTheDocument()
    expect(heroTitle?.tagName.toLowerCase()).toBe('h1')

    const featuresSection = document.querySelector('section[aria-labelledby="features-title"]')
    const featuresTitle = document.getElementById('features-title')
    expect(featuresSection).toBeInTheDocument()
    expect(featuresTitle).toBeInTheDocument()
    expect(featuresTitle?.tagName.toLowerCase()).toBe('h2')

    const howItWorksSection = document.querySelector('section[aria-labelledby="how-it-works-title"]')
    const howItWorksTitle = document.getElementById('how-it-works-title')
    expect(howItWorksSection).toBeInTheDocument()
    expect(howItWorksTitle).toBeInTheDocument()
    expect(howItWorksTitle?.tagName.toLowerCase()).toBe('h2')
  })
})
