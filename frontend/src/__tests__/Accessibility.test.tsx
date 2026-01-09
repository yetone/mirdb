import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../pages/Home'
import FeaturesSection from '../components/FeaturesSection'
import HeroSection from '../components/HeroSection'
import NavigationHeader from '../components/NavigationHeader'
import Footer from '../components/Footer'
import HowItWorksSection from '../components/HowItWorksSection'

const renderWithRouter = (ui: React.ReactElement) => {
  return render(<BrowserRouter>{ui}</BrowserRouter>)
}

describe('Accessibility Compliance Tests', () => {
  beforeEach(() => {
    document.documentElement.setAttribute('data-theme', 'light')
    localStorage.clear()
  })

  // Test Case 1: All images have descriptive alt text or are marked as decorative
  describe('Test Case 1: Image alt attributes', () => {
    it('all SVG icons in FeaturesSection are marked as decorative with aria-hidden', () => {
      render(<FeaturesSection />)

      const featureCards = screen.getAllByTestId(/^feature-card-/)
      expect(featureCards.length).toBeGreaterThan(0)

      // Check that all SVG icons have aria-hidden="true"
      const icons = document.querySelectorAll('[data-testid^="feature-icon-"] svg')
      icons.forEach((icon) => {
        expect(icon).toHaveAttribute('aria-hidden', 'true')
      })
    })

    it('all SVG icons in HowItWorksSection are marked as decorative with aria-hidden', () => {
      render(<HowItWorksSection />)

      const icons = document.querySelectorAll('[data-testid^="step-icon-"] svg')
      icons.forEach((icon) => {
        expect(icon).toHaveAttribute('aria-hidden', 'true')
      })
    })

    it('theme toggle SVG icons are marked as decorative', () => {
      renderWithRouter(<NavigationHeader />)

      const themeToggle = screen.getByTestId('theme-toggle')
      const svg = themeToggle.querySelector('svg')
      expect(svg).toHaveAttribute('aria-hidden', 'true')
    })

    it('no images without alt attributes exist on homepage', () => {
      renderWithRouter(<Home />)

      const allImages = document.querySelectorAll('img')
      allImages.forEach((img) => {
        expect(img).toHaveAttribute('alt')
      })
    })
  })

  // Test Case 2: Check color contrast ratios (structural test - visual validation requires manual testing)
  describe('Test Case 2: Color contrast structure', () => {
    it('text elements use proper semantic classes for readability', () => {
      renderWithRouter(<Home />)

      // Check that main headings exist with proper semantic structure
      const h1Elements = document.querySelectorAll('h1')
      expect(h1Elements.length).toBeGreaterThanOrEqual(1)

      const h2Elements = document.querySelectorAll('h2')
      expect(h2Elements.length).toBeGreaterThanOrEqual(1)
    })

    it('buttons have proper variant classes for visibility', () => {
      renderWithRouter(<NavigationHeader />)

      const signupButton = screen.getByTestId('nav-signup')
      expect(signupButton).toHaveClass('btn-primary')

      const loginButton = screen.getByTestId('nav-login')
      expect(loginButton).toHaveClass('btn-ghost')
    })

    it('feature cards use base-content color classes', () => {
      render(<FeaturesSection />)

      const descriptions = document.querySelectorAll('[data-testid^="feature-description-"]')
      descriptions.forEach((desc) => {
        expect(desc).toHaveClass('text-base-content/70')
      })
    })
  })

  // Test Case 5: Skip navigation link is present for keyboard users
  describe('Test Case 5: Skip navigation link', () => {
    it('skip to main content link exists on homepage', () => {
      renderWithRouter(<Home />)

      const skipLink = screen.getByTestId('skip-to-main')
      expect(skipLink).toBeInTheDocument()
    })

    it('skip link has correct href pointing to main content', () => {
      renderWithRouter(<Home />)

      const skipLink = screen.getByTestId('skip-to-main')
      expect(skipLink).toHaveAttribute('href', '#main-content')
    })

    it('skip link has accessible text', () => {
      renderWithRouter(<Home />)

      const skipLink = screen.getByTestId('skip-to-main')
      expect(skipLink).toHaveTextContent(/skip to main content/i)
    })

    it('main content element exists with correct id', () => {
      renderWithRouter(<Home />)

      const mainContent = document.getElementById('main-content')
      expect(mainContent).toBeInTheDocument()
      expect(mainContent?.tagName).toBe('MAIN')
    })

    it('skip link is visually hidden by default (uses sr-only class)', () => {
      renderWithRouter(<Home />)

      const skipLink = screen.getByTestId('skip-to-main')
      expect(skipLink).toHaveClass('sr-only')
    })

    it('skip link has focus styles to become visible when focused', () => {
      renderWithRouter(<Home />)

      const skipLink = screen.getByTestId('skip-to-main')
      expect(skipLink).toHaveClass('focus:not-sr-only')
    })
  })

  // Test Case 6: ARIA labels on interactive elements
  describe('Test Case 6: ARIA labels on interactive elements', () => {
    it('logo link has proper aria-label', () => {
      renderWithRouter(<NavigationHeader />)

      const logo = screen.getByTestId('nav-logo')
      expect(logo).toHaveAttribute('aria-label', 'URL Shortener - Go to homepage')
    })

    it('theme toggle has descriptive aria-label', () => {
      renderWithRouter(<NavigationHeader />)

      const themeToggle = screen.getByTestId('theme-toggle')
      expect(themeToggle).toHaveAttribute('aria-label')
      const ariaLabel = themeToggle.getAttribute('aria-label')
      expect(ariaLabel).toContain('theme')
    })

    it('navigation element has proper aria-label', () => {
      renderWithRouter(<NavigationHeader />)

      const nav = screen.getByRole('navigation', { name: /main navigation/i })
      expect(nav).toBeInTheDocument()
    })

    it('features section has aria-labelledby pointing to heading', () => {
      render(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      expect(section).toHaveAttribute('aria-labelledby', 'features-heading')

      const heading = document.getElementById('features-heading')
      expect(heading).toBeInTheDocument()
    })

    it('how it works section has aria-labelledby pointing to heading', () => {
      render(<HowItWorksSection />)

      const section = screen.getByTestId('how-it-works-section')
      expect(section).toHaveAttribute('aria-labelledby', 'how-it-works-heading')

      const heading = document.getElementById('how-it-works-heading')
      expect(heading).toBeInTheDocument()
    })

    it('footer has proper role and aria-label', () => {
      renderWithRouter(<Footer />)

      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()

      const footerNav = screen.getByRole('navigation', { name: /footer navigation/i })
      expect(footerNav).toBeInTheDocument()
    })

    it('step numbers in HowItWorksSection have aria-label', () => {
      render(<HowItWorksSection />)

      const stepNumbers = [
        screen.getByTestId('step-number-1'),
        screen.getByTestId('step-number-2'),
        screen.getByTestId('step-number-3'),
      ]

      stepNumbers.forEach((stepNumber, index) => {
        expect(stepNumber).toHaveAttribute('aria-label', `Step ${index + 1}`)
      })
    })
  })

  // Test for proper semantic HTML structure
  describe('Semantic HTML structure', () => {
    it('uses proper heading hierarchy', () => {
      renderWithRouter(<Home />)

      const h1 = screen.getByTestId('hero-headline')
      expect(h1.tagName).toBe('H1')

      const h2Elements = screen.getAllByRole('heading', { level: 2 })
      expect(h2Elements.length).toBeGreaterThanOrEqual(2) // Features and How It Works
    })

    it('uses proper landmark regions', () => {
      renderWithRouter(<Home />)

      // Header
      const header = document.querySelector('header')
      expect(header).toBeInTheDocument()

      // Navigation
      const nav = screen.getByRole('navigation', { name: /main navigation/i })
      expect(nav).toBeInTheDocument()

      // Main content
      const main = document.querySelector('main')
      expect(main).toBeInTheDocument()

      // Footer
      const footer = screen.getByRole('contentinfo')
      expect(footer).toBeInTheDocument()
    })

    it('steps in HowItWorksSection use list semantics', () => {
      render(<HowItWorksSection />)

      const list = screen.getByRole('list')
      expect(list).toBeInTheDocument()

      const listItems = screen.getAllByRole('listitem')
      expect(listItems).toHaveLength(3)
    })
  })

  // Test for keyboard accessibility
  describe('Keyboard accessibility structure', () => {
    it('all navigation links are focusable', () => {
      renderWithRouter(<NavigationHeader />)

      const logo = screen.getByTestId('nav-logo')
      const featuresLink = screen.getByTestId('nav-features')
      const howItWorksLink = screen.getByTestId('nav-how-it-works')
      const themeToggle = screen.getByTestId('theme-toggle')
      const loginButton = screen.getByTestId('nav-login')
      const signupButton = screen.getByTestId('nav-signup')

      // Links use <a> tag
      expect(logo.tagName).toBe('A')
      expect(loginButton.tagName).toBe('A')
      expect(signupButton.tagName).toBe('A')

      // Buttons use <button> tag
      expect(featuresLink.tagName).toBe('BUTTON')
      expect(howItWorksLink.tagName).toBe('BUTTON')
      expect(themeToggle.tagName).toBe('BUTTON')
    })

    it('CTA buttons in hero section are focusable', () => {
      renderWithRouter(<HeroSection />)

      const primaryCta = screen.getByTestId('hero-cta-primary')
      const secondaryCta = screen.getByTestId('hero-cta-secondary')

      expect(primaryCta.tagName).toBe('A')
      expect(secondaryCta.tagName).toBe('A')
    })

    it('footer links are focusable', () => {
      renderWithRouter(<Footer />)

      const aboutLink = screen.getByTestId('footer-link-about')
      const privacyLink = screen.getByTestId('footer-link-privacy')
      const termsLink = screen.getByTestId('footer-link-terms')

      expect(aboutLink.tagName).toBe('A')
      expect(privacyLink.tagName).toBe('A')
      expect(termsLink.tagName).toBe('A')
    })
  })
})
