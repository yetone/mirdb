import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from './Home'
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
    header: ({ children, ...props }: React.HTMLAttributes<HTMLElement>) => (
      <header {...props}>{children}</header>
    ),
    button: ({ children, ...props }: React.ButtonHTMLAttributes<HTMLButtonElement>) => (
      <button {...props}>{children}</button>
    ),
    svg: ({ children, ...props }: React.SVGProps<SVGSVGElement>) => (
      <svg {...props}>{children}</svg>
    ),
    section: ({ children, ...props }: React.HTMLAttributes<HTMLElement>) => (
      <section {...props}>{children}</section>
    ),
    footer: ({ children, ...props }: React.HTMLAttributes<HTMLElement>) => (
      <footer {...props}>{children}</footer>
    ),
  },
  AnimatePresence: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}))

// Helper to render with all required providers
const renderWithProviders = (component: React.ReactNode) => {
  return render(
    <BrowserRouter>
      <ThemeProvider>
        <AuthProvider>
          {component}
        </AuthProvider>
      </ThemeProvider>
    </BrowserRouter>
  )
}

describe('Home Component - Homepage Component Structure', () => {
  beforeEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  afterEach(() => {
    localStorage.clear()
    document.documentElement.removeAttribute('data-theme')
  })

  // Test Case 1: Home component renders without errors
  describe('Test Case 1: Render Home component', () => {
    it('Home component renders without errors', () => {
      renderWithProviders(<Home />)
      expect(screen.getByTestId('home-page')).toBeInTheDocument()
    })

    it('accepts custom data-testid prop', () => {
      renderWithProviders(<Home data-testid="custom-home" />)
      expect(screen.getByTestId('custom-home')).toBeInTheDocument()
    })

    it('renders main content wrapper', () => {
      renderWithProviders(<Home />)
      expect(screen.getByTestId('home-main-content')).toBeInTheDocument()
    })

    it('main content has correct styling classes', () => {
      renderWithProviders(<Home />)
      const mainContent = screen.getByTestId('home-main-content')
      expect(mainContent).toHaveClass('min-h-screen')
      expect(mainContent).toHaveClass('bg-base-100')
      expect(mainContent).toHaveClass('pt-16')
    })
  })

  // Test Case 2: Home contains Hero, Features, HowItWorks, and Footer sections
  describe('Test Case 2: Check Home component sections', () => {
    it('Home contains Hero section', () => {
      renderWithProviders(<Home />)
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })

    it('Home contains Features section', () => {
      renderWithProviders(<Home />)
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
    })

    it('Home contains HowItWorks section', () => {
      renderWithProviders(<Home />)
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
    })

    it('Home contains SocialProof section', () => {
      renderWithProviders(<Home />)
      expect(screen.getByTestId('social-proof-section')).toBeInTheDocument()
    })

    it('Home contains FooterCTA section', () => {
      renderWithProviders(<Home />)
      expect(screen.getByTestId('footer-cta')).toBeInTheDocument()
    })

    it('Home contains Footer section', () => {
      renderWithProviders(<Home />)
      expect(screen.getByTestId('footer')).toBeInTheDocument()
    })

    it('Home contains Navbar', () => {
      renderWithProviders(<Home />)
      expect(screen.getByTestId('navbar')).toBeInTheDocument()
    })

    it('all required sections are present', () => {
      renderWithProviders(<Home />)

      // Verify all required sections exist
      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      const footerSection = screen.getByTestId('footer')

      expect(heroSection).toBeInTheDocument()
      expect(featuresSection).toBeInTheDocument()
      expect(howItWorksSection).toBeInTheDocument()
      expect(footerSection).toBeInTheDocument()
    })
  })

  // Test Case 3: Verify TypeScript types - All props and state are properly typed
  describe('Test Case 3: Verify TypeScript types', () => {
    it('Home component accepts typed props correctly', () => {
      // TypeScript compilation ensures props are typed
      // This test verifies the component renders with valid prop types
      const { container } = renderWithProviders(<Home data-testid="typed-home" />)
      expect(container).toBeInTheDocument()
    })

    it('section components render with their default test IDs', () => {
      renderWithProviders(<Home />)

      // All sections should render with default data-testid values
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
      expect(screen.getByTestId('features-section')).toBeInTheDocument()
      expect(screen.getByTestId('how-it-works-section')).toBeInTheDocument()
      expect(screen.getByTestId('social-proof-section')).toBeInTheDocument()
    })

    it('Home component renders with default props', () => {
      // Verify component renders correctly without any props (using defaults)
      renderWithProviders(<Home />)
      expect(screen.getByTestId('home-page')).toBeInTheDocument()
    })
  })

  // Section Order Verification
  describe('Section Order Verification', () => {
    it('sections are rendered in correct order within main content', () => {
      renderWithProviders(<Home />)

      const mainContent = screen.getByTestId('home-main-content')
      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')
      const socialProofSection = screen.getByTestId('social-proof-section')
      const howItWorksSection = screen.getByTestId('how-it-works-section')
      const footerCtaSection = screen.getByTestId('footer-cta')
      const footerSection = screen.getByTestId('footer')

      // Verify all sections are within main content
      expect(mainContent).toContainElement(heroSection)
      expect(mainContent).toContainElement(featuresSection)
      expect(mainContent).toContainElement(socialProofSection)
      expect(mainContent).toContainElement(howItWorksSection)
      expect(mainContent).toContainElement(footerCtaSection)
      expect(mainContent).toContainElement(footerSection)
    })

    it('Hero section appears before Features section in DOM', () => {
      renderWithProviders(<Home />)

      const heroSection = screen.getByTestId('hero-section')
      const featuresSection = screen.getByTestId('features-section')

      // Check DOM order using compareDocumentPosition
      const position = heroSection.compareDocumentPosition(featuresSection)
      // Node.DOCUMENT_POSITION_FOLLOWING = 4
      expect(position & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    })

    it('Footer section appears last in DOM', () => {
      renderWithProviders(<Home />)

      const footerSection = screen.getByTestId('footer')
      const heroSection = screen.getByTestId('hero-section')

      // Check DOM order - footer should come after hero
      const position = heroSection.compareDocumentPosition(footerSection)
      expect(position & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    })
  })

  // Component Composition
  describe('Component Composition', () => {
    it('Home component is composed of modular section components', () => {
      renderWithProviders(<Home />)

      // Each section should be a distinct, testable element
      const sections = [
        'hero-section',
        'features-section',
        'social-proof-section',
        'how-it-works-section',
        'footer-cta',
        'footer'
      ]

      sections.forEach(sectionTestId => {
        const section = screen.getByTestId(sectionTestId)
        expect(section).toBeInTheDocument()
      })
    })

    it('Navbar is separate from main content', () => {
      renderWithProviders(<Home />)

      const homePage = screen.getByTestId('home-page')
      const navbar = screen.getByTestId('navbar')
      const mainContent = screen.getByTestId('home-main-content')

      // Both navbar and main content should be direct children of home-page
      expect(homePage).toContainElement(navbar)
      expect(homePage).toContainElement(mainContent)

      // Navbar should not be inside main content
      expect(mainContent).not.toContainElement(navbar)
    })
  })

  // Semantic HTML Structure
  describe('Semantic HTML Structure', () => {
    it('renders main element for main content', () => {
      renderWithProviders(<Home />)
      const mainElement = screen.getByRole('main')
      expect(mainElement).toBeInTheDocument()
    })

    it('sections use semantic section elements', () => {
      renderWithProviders(<Home />)

      // Features and HowItWorks use semantic <section> elements
      const featuresSection = screen.getByTestId('features-section')
      const howItWorksSection = screen.getByTestId('how-it-works-section')

      expect(featuresSection.tagName.toLowerCase()).toBe('section')
      expect(howItWorksSection.tagName.toLowerCase()).toBe('section')
    })

    it('footer uses semantic footer element', () => {
      renderWithProviders(<Home />)

      const footerElements = screen.getAllByRole('contentinfo')
      expect(footerElements.length).toBeGreaterThan(0)
    })
  })
})
