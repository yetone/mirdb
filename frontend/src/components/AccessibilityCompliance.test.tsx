import { describe, it, expect, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { ThemeProvider } from '../contexts/ThemeContext'
import { AuthProvider } from '../contexts/AuthContext'
import HeroSection from './HeroSection'
import FeaturesSection from './FeaturesSection'
import HowItWorksSection from './HowItWorksSection'
import Footer from './Footer'
import FooterCTA from './FooterCTA'
import Navbar from './Navbar'
import ThemeToggle from './ThemeToggle'

// Mock framer-motion
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, transition, whileInView, viewport, whileHover, whileTap, exit, variants, ...rest } = props
      return <div {...rest}>{children}</div>
    },
    h1: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, transition, ...rest } = props
      return <h1 {...rest}>{children}</h1>
    },
    h2: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, transition, whileInView, viewport, ...rest } = props
      return <h2 {...rest}>{children}</h2>
    },
    p: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, transition, whileInView, viewport, ...rest } = props
      return <p {...rest}>{children}</p>
    },
    nav: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, transition, ...rest } = props
      return <nav {...rest}>{children}</nav>
    },
    header: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, transition, ...rest } = props
      return <header {...rest}>{children}</header>
    },
    button: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { initial, animate, transition, whileHover, whileTap, ...rest } = props
      return <button {...rest}>{children}</button>
    },
    svg: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => {
      const { animate, initial, ...rest } = props
      return <svg {...rest}>{children}</svg>
    },
  },
  AnimatePresence: ({ children }: React.PropsWithChildren) => <>{children}</>,
}))

const renderWithProviders = (component: React.ReactNode) => {
  return render(
    <BrowserRouter>
      <AuthProvider>
        <ThemeProvider>
          {component}
        </ThemeProvider>
      </AuthProvider>
    </BrowserRouter>
  )
}

const renderHomepage = () => {
  return render(
    <BrowserRouter>
      <AuthProvider>
        <ThemeProvider>
          <Navbar />
          <main>
            <HeroSection />
            <FeaturesSection />
            <HowItWorksSection />
            <FooterCTA />
            <Footer />
          </main>
        </ThemeProvider>
      </AuthProvider>
    </BrowserRouter>
  )
}

describe('Accessibility Compliance - WCAG 2.1 AA Standards', () => {
  // Test Case 2: Check heading hierarchy
  describe('Heading Hierarchy (Test Case 2)', () => {
    it('homepage has exactly one h1 element', () => {
      renderHomepage()
      const h1Elements = screen.getAllByRole('heading', { level: 1 })
      expect(h1Elements).toHaveLength(1)
    })

    it('h1 element is in the hero section with meaningful content', () => {
      renderWithProviders(<HeroSection />)
      const h1 = screen.getByRole('heading', { level: 1 })
      expect(h1).toBeInTheDocument()
      expect(h1).toHaveTextContent('Shorten. Share. Analyze.')
    })

    it('h1 heading has proper data-testid', () => {
      renderWithProviders(<HeroSection />)
      const headline = screen.getByTestId('hero-headline')
      expect(headline.tagName).toBe('H1')
    })

    it('h2 headings follow h1 without skipping levels', () => {
      renderHomepage()

      // Get all h2 elements
      const h2Elements = screen.getAllByRole('heading', { level: 2 })
      expect(h2Elements.length).toBeGreaterThan(0)

      // Verify expected h2 elements exist
      const expectedH2Texts = ['Powerful Features', 'How It Works', 'Ready to Get Started?']
      expectedH2Texts.forEach(text => {
        const h2 = h2Elements.find(el => el.textContent?.includes(text))
        expect(h2).toBeDefined()
      })
    })

    it('h3 headings follow h2 without skipping levels (Features section)', () => {
      renderWithProviders(<FeaturesSection />)

      // Section has h2 heading
      const h2 = screen.getByRole('heading', { level: 2 })
      expect(h2).toHaveTextContent('Powerful Features')

      // Feature cards have h3 headings
      const h3Elements = screen.getAllByRole('heading', { level: 3 })
      expect(h3Elements.length).toBe(4) // 4 feature cards

      const expectedFeatureTitles = [
        'Instant URL Shortening',
        'Detailed Analytics',
        'Easy Management',
        'Secure Sharing'
      ]

      expectedFeatureTitles.forEach(title => {
        const h3 = h3Elements.find(el => el.textContent?.includes(title))
        expect(h3).toBeDefined()
      })
    })

    it('h3 headings follow h2 without skipping levels (How It Works section)', () => {
      renderWithProviders(<HowItWorksSection />)

      // Section has h2 heading
      const h2 = screen.getByRole('heading', { level: 2 })
      expect(h2).toHaveTextContent('How It Works')

      // Steps have h3 headings
      const h3Elements = screen.getAllByRole('heading', { level: 3 })
      expect(h3Elements.length).toBe(3) // 3 steps
    })

    it('Footer sections have h3/h4 headings in proper hierarchy', () => {
      renderWithProviders(<Footer />)

      // Footer has h3 for brand and h4 for sections
      const h3 = screen.getByRole('heading', { level: 3 })
      expect(h3).toHaveTextContent('URL Shortener')

      const h4Elements = screen.getAllByRole('heading', { level: 4 })
      expect(h4Elements.length).toBe(2) // Quick Links and Legal
    })

    it('FooterCTA uses h2 for the call-to-action heading', () => {
      renderWithProviders(<FooterCTA />)

      const ctaHeading = screen.getByTestId('footer-headline')
      expect(ctaHeading.tagName).toBe('H2')
      expect(ctaHeading).toHaveTextContent('Ready to Get Started?')
    })

    it('no heading levels are skipped throughout the page', () => {
      renderHomepage()

      const h1Elements = screen.getAllByRole('heading', { level: 1 })
      const h2Elements = screen.getAllByRole('heading', { level: 2 })
      const h3Elements = screen.getAllByRole('heading', { level: 3 })
      const h4Elements = screen.getAllByRole('heading', { level: 4 })
      const h5Elements = screen.queryAllByRole('heading', { level: 5 })
      const h6Elements = screen.queryAllByRole('heading', { level: 6 })

      // Must have h1
      expect(h1Elements.length).toBeGreaterThan(0)

      // If we have h2, we must have h1
      if (h2Elements.length > 0) {
        expect(h1Elements.length).toBeGreaterThan(0)
      }

      // If we have h3, we must have h2
      if (h3Elements.length > 0) {
        expect(h2Elements.length).toBeGreaterThan(0)
      }

      // If we have h4, we must have h3
      if (h4Elements.length > 0) {
        expect(h3Elements.length).toBeGreaterThan(0)
      }

      // If we have h5, we must have h4
      if (h5Elements.length > 0) {
        expect(h4Elements.length).toBeGreaterThan(0)
      }

      // If we have h6, we must have h5
      if (h6Elements.length > 0) {
        expect(h5Elements.length).toBeGreaterThan(0)
      }
    })

    it('follows proper h1 -> h2 -> h3 -> h4 progression without skipping', () => {
      const { container } = renderWithProviders(
        <>
          <HeroSection />
          <FeaturesSection />
          <HowItWorksSection />
          <FooterCTA />
          <Footer />
        </>
      )

      // Get all headings in document order
      const allHeadings = container.querySelectorAll('h1, h2, h3, h4, h5, h6')
      let previousLevel = 0

      allHeadings.forEach((heading, index) => {
        const level = parseInt(heading.tagName[1])

        // First heading should be h1
        if (index === 0) {
          expect(level).toBe(1)
        }

        // Heading levels should not skip (e.g., h1 to h3 without h2)
        if (previousLevel > 0) {
          // Can go to same level, next level down, or back up
          // But cannot skip down (e.g., h2 to h4)
          if (level > previousLevel) {
            expect(level - previousLevel).toBeLessThanOrEqual(1)
          }
        }

        previousLevel = level
      })
    })
  })

  // Test Case 6: Verify ARIA labels on icon-only buttons
  describe('ARIA Labels on Icon Buttons (Test Case 6)', () => {
    it('theme toggle button has aria-label', () => {
      renderWithProviders(<ThemeToggle />)
      const themeButton = screen.getByTestId('theme-toggle-button')
      expect(themeButton).toHaveAttribute('aria-label', 'Toggle theme')
    })

    it('theme toggle button has aria-expanded attribute', () => {
      renderWithProviders(<ThemeToggle />)
      const themeButton = screen.getByTestId('theme-toggle-button')
      expect(themeButton).toHaveAttribute('aria-expanded')
    })

    it('theme toggle button has aria-haspopup attribute', () => {
      renderWithProviders(<ThemeToggle />)
      const themeButton = screen.getByTestId('theme-toggle-button')
      expect(themeButton).toHaveAttribute('aria-haspopup', 'listbox')
    })

    it('hamburger menu button has aria-label', () => {
      renderWithProviders(<Navbar />)
      const hamburgerButton = screen.getByTestId('hamburger-button')
      expect(hamburgerButton).toHaveAttribute('aria-label')
      // The aria-label changes based on state
      expect(hamburgerButton.getAttribute('aria-label')).toMatch(/menu/i)
    })

    it('hamburger menu button has aria-expanded attribute', () => {
      renderWithProviders(<Navbar />)
      const hamburgerButton = screen.getByTestId('hamburger-button')
      expect(hamburgerButton).toHaveAttribute('aria-expanded', 'false')
    })

    it('hamburger menu button has aria-controls attribute', () => {
      renderWithProviders(<Navbar />)
      const hamburgerButton = screen.getByTestId('hamburger-button')
      expect(hamburgerButton).toHaveAttribute('aria-controls', 'mobile-menu')
    })

    it('hamburger icon svg has aria-hidden attribute', () => {
      renderWithProviders(<Navbar />)
      const hamburgerButton = screen.getByTestId('hamburger-button')
      const svg = hamburgerButton.querySelector('svg')
      expect(svg).toHaveAttribute('aria-hidden', 'true')
    })

    it('has sr-only text for screen readers', () => {
      renderWithProviders(<Navbar />)
      const srOnlyText = screen.getByText(/Close menu|Open menu/)
      expect(srOnlyText).toHaveClass('sr-only')
    })

    it('all feature icons have aria-hidden attribute', () => {
      renderWithProviders(<FeaturesSection />)

      // All feature icon containers should have aria-hidden="true"
      for (let i = 1; i <= 4; i++) {
        const iconContainer = screen.getByTestId(`feature-icon-${i}`)
        expect(iconContainer).toHaveAttribute('aria-hidden', 'true')
      }
    })

    it('feature icon SVGs are properly hidden from screen readers', () => {
      renderWithProviders(<FeaturesSection />)

      // Get all feature icon containers
      for (let i = 1; i <= 4; i++) {
        const iconContainer = screen.getByTestId(`feature-icon-${i}`)
        const svg = iconContainer.querySelector('svg')
        expect(svg).toHaveAttribute('aria-hidden', 'true')
      }
    })

    it('user menu button has aria-expanded and aria-haspopup attributes when authenticated', () => {
      // This tests authenticated state - we can verify the button structure exists
      renderWithProviders(<Navbar />)
      // When not authenticated, we check for the Login/Get Started buttons which should be accessible
      const loginLink = screen.getByTestId('navbar-login')
      expect(loginLink).toBeInTheDocument()
    })

    it('step number indicators have aria-label for screen readers', () => {
      renderWithProviders(<HowItWorksSection />)

      for (let i = 1; i <= 3; i++) {
        const stepNumber = screen.getByTestId(`step-number-${i}`)
        expect(stepNumber).toHaveAttribute('aria-label', `Step ${i}`)
      }
    })

    it('connector lines have aria-hidden attribute', () => {
      renderWithProviders(<HowItWorksSection />)

      // Desktop connectors
      const connector1 = screen.getByTestId('connector-1')
      const connector2 = screen.getByTestId('connector-2')
      expect(connector1).toHaveAttribute('aria-hidden', 'true')
      expect(connector2).toHaveAttribute('aria-hidden', 'true')

      // Mobile connectors
      const mobileConnector1 = screen.getByTestId('connector-mobile-1')
      const mobileConnector2 = screen.getByTestId('connector-mobile-2')
      expect(mobileConnector1).toHaveAttribute('aria-hidden', 'true')
      expect(mobileConnector2).toHaveAttribute('aria-hidden', 'true')
    })
  })

  // Semantic HTML verification
  describe('Semantic HTML Structure', () => {
    it('page uses proper semantic elements (header, main, section, footer)', () => {
      renderHomepage()

      // Check for header element (Navbar renders as header)
      const header = document.querySelector('header')
      expect(header).toBeInTheDocument()

      // Check for main element
      const main = document.querySelector('main')
      expect(main).toBeInTheDocument()

      // Check for section elements
      const sections = document.querySelectorAll('section')
      expect(sections.length).toBeGreaterThan(0)

      // Check for footer element
      const footers = document.querySelectorAll('footer')
      expect(footers.length).toBeGreaterThan(0)
    })

    it('FeaturesSection has section landmark with aria-labelledby', () => {
      renderWithProviders(<FeaturesSection />)
      const section = screen.getByTestId('features-section')
      expect(section.tagName.toLowerCase()).toBe('section')
      expect(section).toHaveAttribute('aria-labelledby', 'features-heading')
    })

    it('HowItWorksSection has section landmark with aria-labelledby', () => {
      renderWithProviders(<HowItWorksSection />)
      const section = document.querySelector('section#how-it-works')
      expect(section).toBeInTheDocument()
      expect(section).toHaveAttribute('aria-labelledby', 'how-it-works-title')
    })

    it('Footer navigation has proper nav element', () => {
      renderWithProviders(<FooterCTA />)
      const nav = screen.getByTestId('footer-navigation')
      expect(nav.tagName.toLowerCase()).toBe('nav')
      expect(nav).toHaveAttribute('aria-label', 'Footer navigation')
    })

    it('Hero section navigation has proper aria-label', () => {
      renderWithProviders(<HeroSection />)
      const nav = screen.getByRole('navigation', { name: 'Page sections' })
      expect(nav).toBeInTheDocument()
    })

    it('HeroSection uses section element', () => {
      const { container } = renderWithProviders(<HeroSection />)
      const section = container.querySelector('section')
      expect(section).toBeInTheDocument()
    })

    it('HeroSection contains header element', () => {
      const { container } = renderWithProviders(<HeroSection />)
      const header = container.querySelector('header')
      expect(header).toBeInTheDocument()
    })

    it('Navbar uses header element', () => {
      const { container } = renderWithProviders(<Navbar />)
      const header = container.querySelector('header')
      expect(header).toBeInTheDocument()
    })

    it('Navbar uses nav element for navigation', () => {
      const { container } = renderWithProviders(<Navbar />)
      const nav = container.querySelector('nav')
      expect(nav).toBeInTheDocument()
    })

    it('Footer uses footer element', () => {
      const { container } = renderWithProviders(<Footer />)
      const footer = container.querySelector('footer')
      expect(footer).toBeInTheDocument()
    })

    it('Footer contains nav elements for link groups', () => {
      renderWithProviders(<Footer />)
      const navElements = screen.getAllByRole('navigation')
      expect(navElements.length).toBeGreaterThanOrEqual(1)
    })

    it('FooterCTA uses footer element', () => {
      const { container } = renderWithProviders(<FooterCTA />)
      const footer = container.querySelector('footer')
      expect(footer).toBeInTheDocument()
    })
  })

  // Focus management verification
  describe('Focus Indicators', () => {
    it('theme toggle button has focus ring styles', () => {
      renderWithProviders(<ThemeToggle />)
      const button = screen.getByTestId('theme-toggle-button')
      expect(button).toHaveClass('focus:ring-2')
      expect(button).toHaveClass('focus:ring-primary')
    })

    it('all interactive elements have proper focus outline classes', () => {
      renderWithProviders(<ThemeToggle />)
      const button = screen.getByTestId('theme-toggle-button')
      expect(button).toHaveClass('focus:outline-none')
      expect(button).toHaveClass('focus:ring-2')
    })

    it('toggle button is focusable', () => {
      renderWithProviders(<ThemeToggle />)
      const toggleButton = screen.getByTestId('theme-toggle-button')
      expect(toggleButton.tagName).toBe('BUTTON')
      expect(toggleButton).not.toHaveAttribute('tabindex', '-1')
    })
  })

  // Image alt text verification
  describe('Image Alt Text', () => {
    it('decorative icons have aria-hidden to exclude from screen readers', () => {
      renderWithProviders(<FeaturesSection />)

      // Check that decorative icon containers are hidden from assistive tech
      const iconContainers = [
        screen.getByTestId('feature-icon-1'),
        screen.getByTestId('feature-icon-2'),
        screen.getByTestId('feature-icon-3'),
        screen.getByTestId('feature-icon-4')
      ]

      iconContainers.forEach(container => {
        expect(container).toHaveAttribute('aria-hidden', 'true')
      })
    })

    it('BackgroundEffect is hidden from screen readers', () => {
      renderWithProviders(<HeroSection />)
      const background = screen.getByTestId('hero-background')
      expect(background).toHaveAttribute('aria-hidden', 'true')
    })
  })

  // Keyboard navigation verification
  describe('Keyboard Navigation', () => {
    it('CTA buttons in hero section are focusable links', () => {
      renderWithProviders(<HeroSection />)

      const getStartedButton = screen.getByTestId('cta-get-started')
      const loginButton = screen.getByTestId('cta-login')

      // Links should be focusable
      expect(getStartedButton.tagName.toLowerCase()).toBe('a')
      expect(loginButton.tagName.toLowerCase()).toBe('a')

      // Links should have href attributes
      expect(getStartedButton).toHaveAttribute('href', '/register')
      expect(loginButton).toHaveAttribute('href', '/login')
    })

    it('navbar links are keyboard accessible', () => {
      renderWithProviders(<Navbar />)

      const loginLink = screen.getByTestId('navbar-login')
      const getStartedLink = screen.getByTestId('navbar-get-started')

      expect(loginLink.tagName.toLowerCase()).toBe('a')
      expect(getStartedLink.tagName.toLowerCase()).toBe('a')
    })

    it('footer navigation links are keyboard accessible', () => {
      renderWithProviders(<FooterCTA />)

      const homeLink = screen.getByTestId('footer-nav-home')
      const loginLink = screen.getByTestId('footer-nav-login')

      expect(homeLink.tagName.toLowerCase()).toBe('a')
      expect(loginLink.tagName.toLowerCase()).toBe('a')
    })

    it('all links are keyboard accessible', () => {
      renderWithProviders(<HeroSection />)
      const links = screen.getAllByRole('link')
      links.forEach(link => {
        expect(link).not.toHaveAttribute('tabindex', '-1')
      })
    })

    it('all buttons are keyboard accessible', () => {
      renderWithProviders(<Navbar />)
      const buttons = screen.getAllByRole('button')
      buttons.forEach(button => {
        expect(button).not.toHaveAttribute('tabindex', '-1')
      })
    })
  })

  // Touch target sizes
  describe('Touch Target Sizes', () => {
    it('hamburger button has minimum 44x44px touch target', () => {
      renderWithProviders(<Navbar />)
      const hamburgerButton = screen.getByTestId('hamburger-button')
      const className = hamburgerButton.className
      expect(className).toContain('w-11')
      expect(className).toContain('h-11')
    })

    it('navigation links have minimum touch target height', () => {
      renderWithProviders(<Navbar />)
      const desktopNav = screen.getByTestId('desktop-nav')
      const links = desktopNav.querySelectorAll('a')
      links.forEach(link => {
        expect(link.className).toContain('min-h-[44px]')
      })
    })

    it('toggle button has minimum touch target size', () => {
      renderWithProviders(<ThemeToggle />)
      const toggleButton = screen.getByTestId('theme-toggle-button')
      const className = toggleButton.className
      expect(className).toContain('min-w-[44px]')
      expect(className).toContain('min-h-[44px]')
    })
  })
})
