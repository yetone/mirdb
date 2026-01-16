import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
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

describe('TC2: Heading Hierarchy Compliance', () => {
  describe('HeroSection', () => {
    it('has a single h1 element as the main headline', () => {
      renderWithProviders(<HeroSection />)

      const h1Elements = screen.getAllByRole('heading', { level: 1 })
      expect(h1Elements).toHaveLength(1)
      expect(h1Elements[0]).toHaveTextContent('Shorten. Share. Analyze.')
    })

    it('h1 heading has proper data-testid', () => {
      renderWithProviders(<HeroSection />)

      const headline = screen.getByTestId('hero-headline')
      expect(headline.tagName).toBe('H1')
    })
  })

  describe('FeaturesSection', () => {
    it('uses h2 for section heading', () => {
      renderWithProviders(<FeaturesSection />)

      const h2Heading = screen.getByRole('heading', { level: 2 })
      expect(h2Heading).toBeInTheDocument()
      expect(h2Heading).toHaveTextContent('Powerful Features')
    })

    it('uses h3 for feature card headings (proper hierarchy after h2)', () => {
      renderWithProviders(<FeaturesSection />)

      const h3Headings = screen.getAllByRole('heading', { level: 3 })
      expect(h3Headings.length).toBeGreaterThanOrEqual(4)

      // Verify feature titles
      expect(h3Headings[0]).toHaveTextContent('Instant URL Shortening')
      expect(h3Headings[1]).toHaveTextContent('Detailed Analytics')
      expect(h3Headings[2]).toHaveTextContent('Easy Management')
      expect(h3Headings[3]).toHaveTextContent('Secure Sharing')
    })

    it('does not skip heading levels (h2 -> h3, not h2 -> h4)', () => {
      renderWithProviders(<FeaturesSection />)

      const h2Count = screen.getAllByRole('heading', { level: 2 }).length
      const h3Count = screen.getAllByRole('heading', { level: 3 }).length
      const h4Headings = screen.queryAllByRole('heading', { level: 4 })

      // There should be h2 and h3 headings
      expect(h2Count).toBeGreaterThan(0)
      expect(h3Count).toBeGreaterThan(0)

      // h4 should only exist if there are h3 headings (no skipping)
      if (h4Headings.length > 0) {
        expect(h3Count).toBeGreaterThan(0)
      }
    })
  })

  describe('HowItWorksSection', () => {
    it('uses h2 for section heading', () => {
      renderWithProviders(<HowItWorksSection />)

      const sectionHeading = screen.getByText('How It Works')
      expect(sectionHeading.tagName).toBe('H2')
    })

    it('uses h3 for step headings', () => {
      renderWithProviders(<HowItWorksSection />)

      const stepTitles = [
        'Paste your long URL',
        'Get your short link instantly',
        'Track performance with analytics'
      ]

      stepTitles.forEach(title => {
        const heading = screen.getByText(title)
        expect(heading.tagName).toBe('H3')
      })
    })
  })

  describe('Footer', () => {
    it('uses h3 for brand title and h4 for section titles', () => {
      renderWithProviders(<Footer />)

      const brandTitle = screen.getByTestId('footer-brand-title')
      expect(brandTitle.tagName).toBe('H3')

      const navTitle = screen.getByTestId('footer-nav-title')
      expect(navTitle.tagName).toBe('H4')

      const legalTitle = screen.getByTestId('footer-legal-title')
      expect(legalTitle.tagName).toBe('H4')
    })
  })

  describe('FooterCTA', () => {
    it('uses h2 for the call-to-action heading', () => {
      renderWithProviders(<FooterCTA />)

      const ctaHeading = screen.getByTestId('footer-headline')
      expect(ctaHeading.tagName).toBe('H2')
      expect(ctaHeading).toHaveTextContent('Ready to Get Started?')
    })
  })

  describe('Full Page Heading Hierarchy', () => {
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
})

describe('TC6: ARIA Labels on Icon-Only Buttons', () => {
  describe('ThemeToggle', () => {
    it('has aria-label on the toggle button', () => {
      renderWithProviders(<ThemeToggle />)

      const toggleButton = screen.getByTestId('theme-toggle-button')
      expect(toggleButton).toHaveAttribute('aria-label', 'Toggle theme')
    })

    it('has aria-haspopup on toggle button', () => {
      renderWithProviders(<ThemeToggle />)

      const toggleButton = screen.getByTestId('theme-toggle-button')
      expect(toggleButton).toHaveAttribute('aria-haspopup', 'listbox')
    })

    it('has aria-expanded on toggle button', () => {
      renderWithProviders(<ThemeToggle />)

      const toggleButton = screen.getByTestId('theme-toggle-button')
      expect(toggleButton).toHaveAttribute('aria-expanded')
    })
  })

  describe('Navbar', () => {
    it('hamburger button has aria-label', () => {
      renderWithProviders(<Navbar />)

      const hamburgerButton = screen.getByTestId('hamburger-button')
      expect(hamburgerButton).toHaveAttribute('aria-label')
    })

    it('hamburger button has aria-controls for mobile menu', () => {
      renderWithProviders(<Navbar />)

      const hamburgerButton = screen.getByTestId('hamburger-button')
      expect(hamburgerButton).toHaveAttribute('aria-controls', 'mobile-menu')
    })

    it('hamburger button has aria-expanded', () => {
      renderWithProviders(<Navbar />)

      const hamburgerButton = screen.getByTestId('hamburger-button')
      expect(hamburgerButton).toHaveAttribute('aria-expanded')
    })

    it('hamburger button icon is hidden from accessibility tree', () => {
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
  })

  describe('FeaturesSection', () => {
    it('feature icons are marked as decorative with aria-hidden', () => {
      renderWithProviders(<FeaturesSection />)

      // Check icon containers are hidden
      const iconContainers = screen.getAllByTestId(/feature-icon-\d/)
      iconContainers.forEach(container => {
        expect(container).toHaveAttribute('aria-hidden', 'true')
      })
    })

    it('all feature SVG icons have aria-hidden="true"', () => {
      const { container } = renderWithProviders(<FeaturesSection />)

      const featureIcons = container.querySelectorAll('[data-testid^="feature-icon-"] svg')
      featureIcons.forEach(icon => {
        expect(icon).toHaveAttribute('aria-hidden', 'true')
      })
    })
  })

  describe('HowItWorksSection', () => {
    it('step numbers have aria-label for accessibility', () => {
      renderWithProviders(<HowItWorksSection />)

      const stepNumbers = screen.getAllByTestId(/step-number-\d/)
      stepNumbers.forEach((step, index) => {
        expect(step).toHaveAttribute('aria-label', `Step ${index + 1}`)
      })
    })

    it('connector lines are hidden from accessibility tree', () => {
      const { container } = renderWithProviders(<HowItWorksSection />)

      const connectors = container.querySelectorAll('[data-testid^="connector-"]')
      connectors.forEach(connector => {
        expect(connector).toHaveAttribute('aria-hidden', 'true')
      })
    })
  })

  describe('HeroSection', () => {
    it('navigation section has aria-label', () => {
      renderWithProviders(<HeroSection />)

      const nav = screen.getByRole('navigation', { name: 'Page sections' })
      expect(nav).toBeInTheDocument()
    })
  })

  describe('Section Landmarks', () => {
    it('FeaturesSection has proper aria-labelledby', () => {
      renderWithProviders(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      expect(section).toHaveAttribute('aria-labelledby', 'features-heading')
    })

    it('HowItWorksSection has proper aria-labelledby', () => {
      const { container } = renderWithProviders(<HowItWorksSection />)

      const section = container.querySelector('#how-it-works')
      expect(section).toHaveAttribute('aria-labelledby', 'how-it-works-title')
    })
  })
})

describe('Semantic HTML Structure', () => {
  describe('HeroSection', () => {
    it('uses section element', () => {
      const { container } = renderWithProviders(<HeroSection />)

      const section = container.querySelector('section')
      expect(section).toBeInTheDocument()
    })

    it('contains header element', () => {
      const { container } = renderWithProviders(<HeroSection />)

      const header = container.querySelector('header')
      expect(header).toBeInTheDocument()
    })

    it('contains nav element', () => {
      const { container } = renderWithProviders(<HeroSection />)

      const nav = container.querySelector('nav')
      expect(nav).toBeInTheDocument()
    })
  })

  describe('FeaturesSection', () => {
    it('uses section element with id', () => {
      const { container } = renderWithProviders(<FeaturesSection />)

      const section = container.querySelector('section#features')
      expect(section).toBeInTheDocument()
    })
  })

  describe('HowItWorksSection', () => {
    it('uses section element with id', () => {
      const { container } = renderWithProviders(<HowItWorksSection />)

      const section = container.querySelector('section#how-it-works')
      expect(section).toBeInTheDocument()
    })
  })

  describe('Footer', () => {
    it('uses footer element', () => {
      const { container } = renderWithProviders(<Footer />)

      const footer = container.querySelector('footer')
      expect(footer).toBeInTheDocument()
    })

    it('contains nav elements for link groups', () => {
      renderWithProviders(<Footer />)

      const navElements = screen.getAllByRole('navigation')
      expect(navElements.length).toBeGreaterThanOrEqual(1)
    })
  })

  describe('FooterCTA', () => {
    it('uses footer element', () => {
      const { container } = renderWithProviders(<FooterCTA />)

      const footer = container.querySelector('footer')
      expect(footer).toBeInTheDocument()
    })
  })

  describe('Navbar', () => {
    it('uses header element', () => {
      const { container } = renderWithProviders(<Navbar />)

      const header = container.querySelector('header')
      expect(header).toBeInTheDocument()
    })

    it('uses nav element for navigation', () => {
      const { container } = renderWithProviders(<Navbar />)

      const nav = container.querySelector('nav')
      expect(nav).toBeInTheDocument()
    })
  })
})

describe('Keyboard Accessibility', () => {
  describe('ThemeToggle', () => {
    it('toggle button is focusable', () => {
      renderWithProviders(<ThemeToggle />)

      const toggleButton = screen.getByTestId('theme-toggle-button')
      expect(toggleButton.tagName).toBe('BUTTON')
      expect(toggleButton).not.toHaveAttribute('tabindex', '-1')
    })

    it('has visible focus ring styles', () => {
      renderWithProviders(<ThemeToggle />)

      const toggleButton = screen.getByTestId('theme-toggle-button')
      const className = toggleButton.className
      expect(className).toContain('focus:ring-2')
      expect(className).toContain('focus:ring-primary')
    })
  })

  describe('Interactive Elements', () => {
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
})

describe('Touch Target Sizes', () => {
  describe('Navbar', () => {
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
  })

  describe('ThemeToggle', () => {
    it('toggle button has minimum touch target size', () => {
      renderWithProviders(<ThemeToggle />)

      const toggleButton = screen.getByTestId('theme-toggle-button')
      const className = toggleButton.className
      expect(className).toContain('min-w-[44px]')
      expect(className).toContain('min-h-[44px]')
    })
  })
})
