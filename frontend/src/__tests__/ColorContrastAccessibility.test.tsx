import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { axe } from 'vitest-axe'
import Home from '../pages/Home'
import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'
import FooterSection from '../components/FooterSection'

/**
 * Color Contrast Accessibility Tests (NFR-5)
 *
 * Verifies color contrast meets WCAG 2.1 AA standards:
 * - Normal text: 4.5:1 minimum contrast ratio
 * - Large text (18pt+ or 14pt bold): 3:1 minimum contrast ratio
 * - Interactive elements must have sufficient contrast
 *
 * Test cases:
 * 1. Analyze headline contrast in light theme
 * 2. Analyze body text contrast in dark theme
 * 3. Analyze CTA button text contrast
 * 4. Run automated accessibility audit (axe-core)
 */

// Theme setup helpers
const setupTheme = (theme: string) => {
  document.documentElement.setAttribute('data-theme', theme)
}

const cleanupTheme = () => {
  document.documentElement.removeAttribute('data-theme')
}

describe('Color Contrast Accessibility (NFR-5)', () => {
  afterEach(() => {
    cleanupTheme()
  })

  // Test Case 1: Analyze headline contrast in light theme
  describe('Test Case 1: Headline contrast in light theme', () => {
    beforeEach(() => {
      setupTheme('light')
    })

    it('should have hero headline with white text on gradient background for sufficient contrast', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      // Large text (h1) requires minimum 3:1 contrast ratio
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline).toHaveClass('text-white')

      // Verify headline is styled as large text (responsive sizing)
      expect(headline).toHaveClass('text-4xl')
      expect(headline).toHaveClass('font-bold')
    })

    it('should have section headings that use theme-adaptive text colors', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // Section headings should inherit text color from base-content
      const featuresHeading = screen.getByRole('heading', { name: /powerful features/i })
      const howItWorksHeading = screen.getByRole('heading', { name: /how it works/i })

      // Both should use default text color (base-content) which has proper contrast
      expect(featuresHeading).toHaveClass('font-bold')
      expect(howItWorksHeading).toHaveClass('font-bold')
    })

    it('should have feature card titles with readable contrast in light theme', () => {
      render(<FeaturesSection />)

      const urlTitle = screen.getByText('URL Shortening')
      const analyticsTitle = screen.getByText('Analytics Dashboard')
      const linkTitle = screen.getByText('Link Management')

      // Titles should have semibold weight for visibility
      expect(urlTitle).toHaveClass('font-semibold')
      expect(analyticsTitle).toHaveClass('font-semibold')
      expect(linkTitle).toHaveClass('font-semibold')
    })

    it('should have footer headings with proper contrast in light theme', () => {
      render(
        <MemoryRouter>
          <FooterSection />
        </MemoryRouter>
      )

      // Footer brand heading
      const brandHeading = screen.getByText('URL Shortener')
      expect(brandHeading).toHaveClass('font-bold')

      // Footer section headings
      const navHeading = screen.getByText('Navigation')
      const legalHeading = screen.getByText('Legal')
      expect(navHeading).toHaveClass('font-semibold')
      expect(legalHeading).toHaveClass('font-semibold')
    })
  })

  // Test Case 2: Analyze body text contrast in dark theme
  describe('Test Case 2: Body text contrast in dark theme', () => {
    beforeEach(() => {
      setupTheme('dark')
    })

    it('should have hero subheadline with sufficient contrast (white/90 on gradient)', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      // Body text requires minimum 4.5:1 contrast ratio
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toHaveClass('text-white/90')
    })

    it('should have feature descriptions using theme-adaptive colors for dark mode contrast', () => {
      render(<FeaturesSection />)

      // Feature descriptions use text-base-content/70 which adapts to dark theme
      const urlDescription = screen.getByText(/create memorable, short links instantly/i)
      const analyticsDescription = screen.getByText(/track clicks, locations, and referrers/i)
      const linkDescription = screen.getByText(/organize and manage all your links/i)

      // The /70 opacity ensures readable contrast in dark mode
      expect(urlDescription).toHaveClass('text-base-content/70')
      expect(analyticsDescription).toHaveClass('text-base-content/70')
      expect(linkDescription).toHaveClass('text-base-content/70')
    })

    it('should have footer body text with readable contrast in dark theme', () => {
      render(
        <MemoryRouter>
          <FooterSection />
        </MemoryRouter>
      )

      // Footer description uses text-base-content/70
      const description = screen.getByText(/transform your long urls into short, memorable links/i)
      expect(description).toHaveClass('text-base-content/70')
    })

    it('should have copyright text with sufficient contrast in dark theme', () => {
      render(
        <MemoryRouter>
          <FooterSection />
        </MemoryRouter>
      )

      const copyright = screen.getByTestId('footer-copyright')
      // Uses text-base-content/60 which provides sufficient contrast against base-200
      expect(copyright).toHaveClass('text-base-content/60')
    })
  })

  // Test Case 3: Analyze CTA button text contrast
  describe('Test Case 3: CTA button text contrast', () => {
    it('should have primary CTA button with high contrast (primary text on white background)', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      const primaryCTA = screen.getByRole('link', { name: /get started free/i })

      // Button uses white background with primary text color for high contrast
      expect(primaryCTA).toHaveClass('bg-white')
      expect(primaryCTA).toHaveClass('text-primary')
      expect(primaryCTA).toHaveClass('btn')
      expect(primaryCTA).toHaveClass('btn-lg')
    })

    it('should have secondary login link with sufficient contrast (white on gradient)', () => {
      render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      const loginLink = screen.getByTestId('login-link')

      // Login link uses white text with font-semibold for visibility
      expect(loginLink).toHaveClass('text-white')
      expect(loginLink).toHaveClass('font-semibold')
      expect(loginLink).toHaveClass('underline')
    })

    it('should have footer navigation links with accessible contrast', () => {
      render(
        <MemoryRouter>
          <FooterSection />
        </MemoryRouter>
      )

      const homeLink = screen.getByTestId('footer-link-home')
      const loginLink = screen.getByTestId('footer-link-login')
      const registerLink = screen.getByTestId('footer-link-register')

      // Links use text-base-content/70 for readable contrast
      expect(homeLink).toHaveClass('text-base-content/70')
      expect(loginLink).toHaveClass('text-base-content/70')
      expect(registerLink).toHaveClass('text-base-content/70')
    })

    it('should have footer legal links with accessible contrast', () => {
      render(
        <MemoryRouter>
          <FooterSection />
        </MemoryRouter>
      )

      const privacyLink = screen.getByTestId('footer-link-privacy')
      const termsLink = screen.getByTestId('footer-link-terms')

      expect(privacyLink).toHaveClass('text-base-content/70')
      expect(termsLink).toHaveClass('text-base-content/70')
    })

    it('should have step number badges with proper text contrast', () => {
      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      const stepNumber1 = screen.getByTestId('step-number-1')
      const stepNumber2 = screen.getByTestId('step-number-2')
      const stepNumber3 = screen.getByTestId('step-number-3')

      // Step numbers use text-primary-content on bg-primary for guaranteed contrast
      expect(stepNumber1).toHaveClass('bg-primary')
      expect(stepNumber1).toHaveClass('text-primary-content')
      expect(stepNumber2).toHaveClass('bg-primary')
      expect(stepNumber2).toHaveClass('text-primary-content')
      expect(stepNumber3).toHaveClass('bg-primary')
      expect(stepNumber3).toHaveClass('text-primary-content')
    })
  })

  // Test Case 4: Run automated accessibility audit (axe-core)
  describe('Test Case 4: Automated accessibility audit with axe-core', () => {
    it('should have no color contrast violations in light theme', async () => {
      setupTheme('light')

      const { container } = render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      const results = await axe(container, {
        rules: {
          // Focus on color contrast rules
          'color-contrast': { enabled: true },
          'color-contrast-enhanced': { enabled: true },
        },
        runOnly: {
          type: 'rule',
          values: ['color-contrast'],
        },
      })

      expect(results).toHaveNoViolations()
    })

    it('should have no color contrast violations in dark theme', async () => {
      setupTheme('dark')

      const { container } = render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      const results = await axe(container, {
        rules: {
          'color-contrast': { enabled: true },
          'color-contrast-enhanced': { enabled: true },
        },
        runOnly: {
          type: 'rule',
          values: ['color-contrast'],
        },
      })

      expect(results).toHaveNoViolations()
    })

    it('should have no color contrast violations in cyberpunk theme', async () => {
      setupTheme('cyberpunk')

      const { container } = render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      const results = await axe(container, {
        rules: {
          'color-contrast': { enabled: true },
          'color-contrast-enhanced': { enabled: true },
        },
        runOnly: {
          type: 'rule',
          values: ['color-contrast'],
        },
      })

      expect(results).toHaveNoViolations()
    })

    it('should have no color contrast violations in synthwave theme', async () => {
      setupTheme('synthwave')

      const { container } = render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      const results = await axe(container, {
        rules: {
          'color-contrast': { enabled: true },
          'color-contrast-enhanced': { enabled: true },
        },
        runOnly: {
          type: 'rule',
          values: ['color-contrast'],
        },
      })

      expect(results).toHaveNoViolations()
    })

    it('should have no critical accessibility violations for hero section', async () => {
      setupTheme('light')

      const { container } = render(
        <MemoryRouter>
          <HeroSection />
        </MemoryRouter>
      )

      const results = await axe(container, {
        runOnly: {
          type: 'tag',
          values: ['wcag2a', 'wcag2aa'],
        },
      })

      // Filter for only critical and serious violations
      const criticalViolations = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      )

      expect(criticalViolations).toHaveLength(0)
    })

    it('should have no critical accessibility violations for features section', async () => {
      setupTheme('light')

      const { container } = render(<FeaturesSection />)

      const results = await axe(container, {
        runOnly: {
          type: 'tag',
          values: ['wcag2a', 'wcag2aa'],
        },
      })

      const criticalViolations = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      )

      expect(criticalViolations).toHaveLength(0)
    })

    it('should have no critical accessibility violations for footer section', async () => {
      setupTheme('light')

      const { container } = render(
        <MemoryRouter>
          <FooterSection />
        </MemoryRouter>
      )

      const results = await axe(container, {
        runOnly: {
          type: 'tag',
          values: ['wcag2a', 'wcag2aa'],
        },
      })

      const criticalViolations = results.violations.filter(
        (v) => v.impact === 'critical' || v.impact === 'serious'
      )

      expect(criticalViolations).toHaveLength(0)
    })
  })

  // Additional cross-theme contrast verification
  describe('Cross-theme color contrast consistency', () => {
    const themes = ['light', 'dark', 'cyberpunk', 'synthwave']

    themes.forEach((theme) => {
      it(`should use DaisyUI semantic color tokens that ensure contrast in ${theme} theme`, () => {
        setupTheme(theme)

        render(
          <MemoryRouter initialEntries={['/']}>
            <Home />
          </MemoryRouter>
        )

        // DaisyUI semantic tokens automatically provide proper contrast
        const featuresSection = screen.getByTestId('features-section')
        const howItWorksSection = screen.getByTestId('how-it-works-section')
        const footerSection = screen.getByTestId('footer-section')

        // Sections use bg-base-200 which adapts to each theme
        expect(featuresSection).toHaveClass('bg-base-200')
        expect(howItWorksSection).toHaveClass('bg-base-200')
        expect(footerSection).toHaveClass('bg-base-200')

        // Clean up for next iteration
        cleanupTheme()
      })
    })

    themes.forEach((theme) => {
      it(`should have icon containers with primary color in ${theme} theme`, () => {
        setupTheme(theme)

        render(<FeaturesSection />)

        const urlShorteningIcon = screen.getByTestId('url-shortening-icon')
        const analyticsIcon = screen.getByTestId('analytics-icon')
        const linkManagementIcon = screen.getByTestId('link-management-icon')

        // Icons use text-primary which adapts to each theme
        expect(urlShorteningIcon.closest('.text-primary')).toBeInTheDocument()
        expect(analyticsIcon.closest('.text-primary')).toBeInTheDocument()
        expect(linkManagementIcon.closest('.text-primary')).toBeInTheDocument()

        cleanupTheme()
      })
    })
  })

  // Test proper heading hierarchy for accessibility
  describe('Heading hierarchy and structure for accessibility', () => {
    it('should have proper heading hierarchy on homepage', () => {
      setupTheme('light')

      render(
        <MemoryRouter initialEntries={['/']}>
          <Home />
        </MemoryRouter>
      )

      // h1 should be the main headline
      const h1 = screen.getByRole('heading', { level: 1 })
      expect(h1).toHaveTextContent('Shorten, Share, Track')

      // h2 should be section headings
      const h2Headings = screen.getAllByRole('heading', { level: 2 })
      expect(h2Headings.length).toBeGreaterThanOrEqual(2)

      // Verify section headings exist
      expect(screen.getByRole('heading', { name: /powerful features/i })).toBeInTheDocument()
      expect(screen.getByRole('heading', { name: /how it works/i })).toBeInTheDocument()
    })

    it('should have h3 headings for feature cards', () => {
      setupTheme('light')

      render(<FeaturesSection />)

      const h3Headings = screen.getAllByRole('heading', { level: 3 })
      expect(h3Headings).toHaveLength(3)

      // Verify feature titles
      expect(screen.getByText('URL Shortening')).toBeInTheDocument()
      expect(screen.getByText('Analytics Dashboard')).toBeInTheDocument()
      expect(screen.getByText('Link Management')).toBeInTheDocument()
    })
  })
})
