/**
 * DaisyUI and Tailwind Styling Consistency Tests
 *
 * This test file verifies NFR-4: Homepage maintains consistent styling
 * with existing DaisyUI/Tailwind design system.
 *
 * Test Cases:
 * 1. Verify GlassMorphismCard usage - Feature cards use existing GlassMorphismCard component
 * 2. Verify FuturisticButton usage - CTA buttons use existing FuturisticButton component
 * 3. Check DaisyUI class usage - Homepage uses DaisyUI utility classes (btn, card, etc.)
 * 4. Verify color variable usage - Homepage uses DaisyUI color variables (primary, secondary, etc.)
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import FeaturesSection from './FeaturesSection'
import HeroSection from './HeroSection'
import FooterCTA from './FooterCTA'
import HowItWorksSection from './HowItWorksSection'
import Navbar from './Navbar'
import { ThemeProvider } from '../contexts/ThemeContext'
import { AuthProvider } from '../contexts/AuthContext'

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
      <div {...props}>{children}</div>
    ),
    h1: ({ children, ...props }: React.HTMLAttributes<HTMLHeadingElement>) => (
      <h1 {...props}>{children}</h1>
    ),
    h2: ({ children, ...props }: React.HTMLAttributes<HTMLHeadingElement>) => (
      <h2 {...props}>{children}</h2>
    ),
    p: ({ children, ...props }: React.HTMLAttributes<HTMLParagraphElement>) => (
      <p {...props}>{children}</p>
    ),
    nav: ({ children, ...props }: React.HTMLAttributes<HTMLElement>) => (
      <nav {...props}>{children}</nav>
    ),
    button: ({ children, ...props }: React.ButtonHTMLAttributes<HTMLButtonElement>) => (
      <button {...props}>{children}</button>
    ),
    header: ({ children, ...props }: React.HTMLAttributes<HTMLElement>) => (
      <header {...props}>{children}</header>
    ),
    section: ({ children, ...props }: React.HTMLAttributes<HTMLElement>) => (
      <section {...props}>{children}</section>
    ),
    svg: ({ children, ...props }: React.SVGProps<SVGSVGElement>) => (
      <svg {...props}>{children}</svg>
    ),
  },
  AnimatePresence: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}))

// Helper to render with all required providers
const renderWithProviders = (component: React.ReactNode) => {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <ThemeProvider>
        <AuthProvider>
          {component}
        </AuthProvider>
      </ThemeProvider>
    </MemoryRouter>
  )
}

describe('DaisyUI and Tailwind Styling Consistency (NFR-4)', () => {
  beforeEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  /**
   * Test Case 1: Verify GlassMorphismCard usage
   * Input: Verify GlassMorphismCard usage
   * Expected: Feature cards use existing GlassMorphismCard component
   */
  describe('Test Case 1: Verify GlassMorphismCard usage - Feature cards use existing GlassMorphismCard component', () => {
    it('FeaturesSection renders feature cards using GlassMorphismCard', () => {
      renderWithProviders(<FeaturesSection />)

      // All 4 feature cards should use GlassMorphismCard component
      const glassMorphismCards = screen.getAllByTestId('glassmorphism-card')
      expect(glassMorphismCards).toHaveLength(4)
    })

    it('GlassMorphismCard applies consistent glassmorphism styling classes', () => {
      renderWithProviders(<FeaturesSection />)

      const cards = screen.getAllByTestId('glassmorphism-card')

      // Each card should have the glassmorphism styling
      cards.forEach((card) => {
        // Backdrop blur for glass effect
        expect(card).toHaveClass('backdrop-blur-md')
        // DaisyUI-compatible semi-transparent background
        expect(card).toHaveClass('bg-base-200/30')
        // Themed border
        expect(card.className).toContain('border')
        expect(card.className).toContain('border-base-content/10')
        // Rounded corners for modern look
        expect(card).toHaveClass('rounded-2xl')
        // Shadow for elevation
        expect(card).toHaveClass('shadow-xl')
        // Consistent padding
        expect(card).toHaveClass('p-6')
      })
    })

    it('GlassMorphismCard uses DaisyUI theme-aware color variables', () => {
      renderWithProviders(<FeaturesSection />)

      const cards = screen.getAllByTestId('glassmorphism-card')

      cards.forEach((card) => {
        // Should use DaisyUI base-200 color (theme-aware)
        expect(card.className).toContain('base-200')
        // Should use DaisyUI base-content color for border (theme-aware)
        expect(card.className).toContain('base-content')
      })
    })

    it('feature card content inherits DaisyUI theme colors', () => {
      renderWithProviders(<FeaturesSection />)

      // Check that feature titles use base-content (DaisyUI theme variable)
      const titles = [
        screen.getByTestId('feature-title-1'),
        screen.getByTestId('feature-title-2'),
        screen.getByTestId('feature-title-3'),
        screen.getByTestId('feature-title-4'),
      ]

      titles.forEach((title) => {
        expect(title).toHaveClass('text-base-content')
      })
    })
  })

  /**
   * Test Case 2: Verify FuturisticButton usage
   * Input: Verify FuturisticButton usage
   * Expected: CTA buttons use existing FuturisticButton component
   */
  describe('Test Case 2: Verify FuturisticButton usage - CTA buttons use existing FuturisticButton component', () => {
    it('HeroSection uses FuturisticButton for primary CTA (Get Started)', () => {
      renderWithProviders(<HeroSection />)

      const getStartedButton = screen.getByTestId('cta-get-started')
      expect(getStartedButton).toBeInTheDocument()
      // FuturisticButton primary variant should use bg-primary
      expect(getStartedButton).toHaveClass('bg-primary')
      expect(getStartedButton).toHaveClass('text-primary-content')
    })

    it('HeroSection uses FuturisticButton for secondary CTA (Login)', () => {
      renderWithProviders(<HeroSection />)

      const loginButton = screen.getByTestId('cta-login')
      expect(loginButton).toBeInTheDocument()
      // FuturisticButton secondary variant uses bg-secondary
      expect(loginButton).toHaveClass('bg-secondary')
      expect(loginButton).toHaveClass('text-secondary-content')
    })

    it('FooterCTA uses FuturisticButton for Get Started CTA', () => {
      renderWithProviders(<FooterCTA />)

      const footerCTAButton = screen.getByTestId('footer-cta-get-started')
      expect(footerCTAButton).toBeInTheDocument()
      // Should use primary variant
      expect(footerCTAButton).toHaveClass('bg-primary')
      expect(footerCTAButton).toHaveClass('text-primary-content')
    })

    it('FuturisticButton applies consistent base styling across all instances', () => {
      renderWithProviders(<HeroSection />)

      const getStartedBtn = screen.getByTestId('cta-get-started')
      const loginBtn = screen.getByTestId('cta-login')

      // Common styling from FuturisticButton
      ;[getStartedBtn, loginBtn].forEach((button) => {
        expect(button).toHaveClass('inline-flex')
        expect(button).toHaveClass('items-center')
        expect(button).toHaveClass('justify-center')
        expect(button).toHaveClass('font-semibold')
        expect(button).toHaveClass('rounded-lg')
        // Focus accessibility
        expect(button).toHaveClass('focus:ring-2')
        expect(button).toHaveClass('focus:ring-primary')
      })
    })

    it('FuturisticButton is used consistently across homepage sections', () => {
      // Test HeroSection
      const { unmount: unmountHero } = renderWithProviders(<HeroSection />)
      expect(screen.getByTestId('cta-get-started')).toHaveClass('bg-primary')
      unmountHero()

      // Test FooterCTA
      renderWithProviders(<FooterCTA />)
      expect(screen.getByTestId('footer-cta-get-started')).toHaveClass('bg-primary')
    })
  })

  /**
   * Test Case 3: Check DaisyUI class usage
   * Input: Check DaisyUI class usage
   * Expected: Homepage uses DaisyUI utility classes (btn, card, etc.)
   */
  describe('Test Case 3: Check DaisyUI class usage - Homepage uses DaisyUI utility classes', () => {
    it('FeaturesSection uses DaisyUI bg-base-100 for section background', () => {
      renderWithProviders(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      expect(section).toHaveClass('bg-base-100')
    })

    it('FooterCTA uses DaisyUI bg-base-200 for section background', () => {
      renderWithProviders(<FooterCTA />)

      const footer = screen.getByTestId('footer-cta')
      expect(footer).toHaveClass('bg-base-200')
    })

    it('Navbar uses DaisyUI bg-base-100 with transparency for glass effect', () => {
      renderWithProviders(<Navbar />)

      const navbar = screen.getByTestId('navbar')
      // Uses bg-base-100/80 for semi-transparent background
      expect(navbar).toHaveClass('bg-base-100/80')
      expect(navbar).toHaveClass('backdrop-blur-md')
    })

    it('Navbar uses DaisyUI border-base-300 for bottom border', () => {
      renderWithProviders(<Navbar />)

      const navbar = screen.getByTestId('navbar')
      expect(navbar).toHaveClass('border-b')
      expect(navbar).toHaveClass('border-base-300')
    })

    it('HowItWorksSection uses DaisyUI primary color styling for step numbers', () => {
      renderWithProviders(<HowItWorksSection />)

      const stepNumbers = [
        screen.getByTestId('step-number-1'),
        screen.getByTestId('step-number-2'),
        screen.getByTestId('step-number-3'),
      ]

      stepNumbers.forEach((stepNumber) => {
        // Uses DaisyUI primary colors
        expect(stepNumber).toHaveClass('bg-primary')
        expect(stepNumber).toHaveClass('text-primary-content')
        expect(stepNumber).toHaveClass('rounded-full')
      })
    })

    it('HowItWorksSection connector lines use DaisyUI primary color with opacity', () => {
      renderWithProviders(<HowItWorksSection />)

      // Desktop connectors
      const connector1 = screen.getByTestId('connector-1')
      const connector2 = screen.getByTestId('connector-2')

      expect(connector1.className).toContain('bg-primary/30')
      expect(connector2.className).toContain('bg-primary/30')
    })

    it('FooterCTA border uses DaisyUI border-base-300', () => {
      renderWithProviders(<FooterCTA />)

      const footer = screen.getByTestId('footer-cta')
      // Find the element with the border
      const borderElement = footer.querySelector('.border-t.border-base-300')
      expect(borderElement).toBeInTheDocument()
    })

    it('homepage components use consistent Tailwind responsive classes', () => {
      renderWithProviders(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      // Responsive grid following Tailwind conventions
      expect(grid).toHaveClass('grid')
      expect(grid).toHaveClass('grid-cols-1')
      expect(grid).toHaveClass('sm:grid-cols-2')
      expect(grid).toHaveClass('lg:grid-cols-4')
    })
  })

  /**
   * Test Case 4: Verify color variable usage
   * Input: Verify color variable usage
   * Expected: Homepage uses DaisyUI color variables (primary, secondary, etc.)
   */
  describe('Test Case 4: Verify color variable usage - Homepage uses DaisyUI color variables', () => {
    it('HeroSection headline uses DaisyUI text-base-content for primary text', () => {
      renderWithProviders(<HeroSection />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toHaveClass('text-base-content')
    })

    it('HeroSection subheadline uses DaisyUI text-base-content with opacity', () => {
      renderWithProviders(<HeroSection />)

      const subheadline = screen.getByTestId('hero-subheadline')
      // Uses base-content with opacity (text-base-content/80)
      expect(subheadline.className).toContain('text-base-content')
    })

    it('FeaturesSection uses text-base-content/70 for descriptions (muted text)', () => {
      renderWithProviders(<FeaturesSection />)

      const descriptions = [
        screen.getByTestId('feature-description-1'),
        screen.getByTestId('feature-description-2'),
        screen.getByTestId('feature-description-3'),
        screen.getByTestId('feature-description-4'),
      ]

      descriptions.forEach((desc) => {
        expect(desc.className).toContain('text-base-content/70')
      })
    })

    it('feature icons use DaisyUI text-primary color', () => {
      renderWithProviders(<FeaturesSection />)

      const iconContainers = [
        screen.getByTestId('feature-icon-1'),
        screen.getByTestId('feature-icon-2'),
        screen.getByTestId('feature-icon-3'),
        screen.getByTestId('feature-icon-4'),
      ]

      iconContainers.forEach((iconContainer) => {
        expect(iconContainer).toHaveClass('text-primary')
      })
    })

    it('HeroSection navigation links use DaisyUI hover:text-primary', () => {
      renderWithProviders(<HeroSection />)

      const navLinks = [
        screen.getByTestId('nav-features'),
        screen.getByTestId('nav-demo'),
      ]

      navLinks.forEach((link) => {
        expect(link).toHaveClass('hover:text-primary')
      })
    })

    it('FooterCTA uses DaisyUI color variables for text hierarchy', () => {
      renderWithProviders(<FooterCTA />)

      // Primary text (headline) uses base-content
      const headline = screen.getByTestId('footer-headline')
      expect(headline).toHaveClass('text-base-content')

      // Secondary text (subheadline) uses base-content with opacity
      const subheadline = screen.getByTestId('footer-subheadline')
      expect(subheadline.className).toContain('text-base-content/70')

      // Legal links use even more muted text
      const copyright = screen.getByTestId('footer-copyright')
      expect(copyright.className).toContain('text-base-content/50')
    })

    it('Navbar logo uses DaisyUI hover:text-primary for interactive state', () => {
      renderWithProviders(<Navbar />)

      const logo = screen.getByTestId('navbar-logo')
      expect(logo).toHaveClass('text-base-content')
      expect(logo).toHaveClass('hover:text-primary')
    })

    it('Navbar desktop links use DaisyUI text-base-content/70 with hover:text-primary', () => {
      renderWithProviders(<Navbar />)

      const featuresLink = screen.getByTestId('nav-features-desktop')
      expect(featuresLink.className).toContain('text-base-content/70')
      expect(featuresLink).toHaveClass('hover:text-primary')
    })

    it('FuturisticButton variants use DaisyUI semantic colors', () => {
      renderWithProviders(<HeroSection />)

      // Primary button uses bg-primary and text-primary-content
      const primaryBtn = screen.getByTestId('cta-get-started')
      expect(primaryBtn).toHaveClass('bg-primary')
      expect(primaryBtn).toHaveClass('text-primary-content')

      // Secondary button uses bg-secondary and text-secondary-content
      const secondaryBtn = screen.getByTestId('cta-login')
      expect(secondaryBtn).toHaveClass('bg-secondary')
      expect(secondaryBtn).toHaveClass('text-secondary-content')
    })

    it('HowItWorksSection step descriptions use DaisyUI text-base-content/70', () => {
      renderWithProviders(<HowItWorksSection />)

      const descriptions = [
        screen.getByTestId('step-description-1'),
        screen.getByTestId('step-description-2'),
        screen.getByTestId('step-description-3'),
      ]

      descriptions.forEach((desc) => {
        expect(desc.className).toContain('text-base-content/70')
      })
    })
  })

  describe('Comprehensive styling consistency checks', () => {
    it('all homepage sections follow consistent spacing patterns', () => {
      renderWithProviders(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      // Consistent vertical padding
      expect(section).toHaveClass('py-20')
      // Consistent horizontal padding
      expect(section).toHaveClass('px-4')
    })

    it('Navbar has proper styling for layering and visibility', () => {
      renderWithProviders(<Navbar />)

      const navbar = screen.getByTestId('navbar')
      expect(navbar).toHaveClass('z-50')
      expect(navbar).toHaveClass('fixed')
      expect(navbar).toHaveClass('top-0')
    })

    it('FooterCTA navigation links maintain DaisyUI styling consistency', () => {
      renderWithProviders(<FooterCTA />)

      const navLinks = within(screen.getByTestId('footer-navigation')).getAllByRole('link')

      navLinks.forEach((link) => {
        // Should use theme-aware colors
        expect(link.className).toContain('text-base-content')
        expect(link).toHaveClass('hover:text-primary')
        expect(link).toHaveClass('transition-colors')
      })
    })
  })
})
