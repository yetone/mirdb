import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Home from './Home'
import App from '../App'
import { AuthProvider } from '../contexts/AuthContext'

// Mock scrollIntoView
Element.prototype.scrollIntoView = vi.fn()

// Mock localStorage
const localStorageMock = {
  getItem: vi.fn(),
  setItem: vi.fn(),
  clear: vi.fn(),
  removeItem: vi.fn(),
}
Object.defineProperty(window, 'localStorage', { value: localStorageMock })

const renderWithRouter = () => {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <AuthProvider>
        <Home />
      </AuthProvider>
    </MemoryRouter>
  )
}

// Render App (includes Navbar) for navigation tests - App already has AuthProvider
const renderAppWithRouter = () => {
  return render(
    <MemoryRouter initialEntries={['/']}>
      <App />
    </MemoryRouter>
  )
}

// Helper to set viewport width for tests
const setViewportWidth = (width: number) => {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  })
  window.dispatchEvent(new Event('resize'))
}

describe('Responsive Design - Mobile (375px viewport)', () => {
  const MOBILE_WIDTH = 375

  beforeEach(() => {
    vi.clearAllMocks()
    localStorageMock.getItem.mockReturnValue(null)
    setViewportWidth(MOBILE_WIDTH)
  })

  afterEach(() => {
    // Reset to default
    setViewportWidth(1024)
  })

  // Test Case 1: Navigation collapses to mobile menu or hamburger icon
  describe('Test Case 1: Navigation at mobile viewport', () => {
    it('renders navigation that adapts to mobile viewport', () => {
      renderWithRouter()

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()

      // The hero section has responsive classes for mobile layout
      // At mobile, the container should use flex-col (column layout)
      const container = heroSection.querySelector('.container')
      expect(container).toBeInTheDocument()
      expect(container).toHaveClass('flex-col')
    })

    it('CTA buttons stack vertically on small screens', () => {
      renderWithRouter()

      const primaryCTA = screen.getByTestId('hero-cta-primary')
      const secondaryCTA = screen.getByTestId('hero-cta-secondary')

      expect(primaryCTA).toBeInTheDocument()
      expect(secondaryCTA).toBeInTheDocument()

      // Buttons should be in a flex-col container at mobile
      const buttonContainer = primaryCTA.parentElement
      expect(buttonContainer).toHaveClass('flex-col')
    })
  })

  // Test Case 2: Feature cards stack in single column
  describe('Test Case 2: Feature cards at mobile viewport', () => {
    it('renders feature cards in a responsive grid that stacks on mobile', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Find the grid container with feature cards
      const gridContainer = featuresSection.querySelector('.grid')
      expect(gridContainer).toBeInTheDocument()

      // Grid should have grid-cols-1 for mobile (single column)
      expect(gridContainer).toHaveClass('grid-cols-1')
    })

    it('renders all 4 feature cards', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      const featureCards = featuresSection.querySelectorAll('.card')

      expect(featureCards.length).toBe(4)
    })
  })

  // Test Case 3: Hero content is readable and properly sized for mobile
  describe('Test Case 3: Hero section at mobile viewport', () => {
    it('renders hero headline with responsive text sizing', () => {
      renderWithRouter()

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()

      // Headline should have text-4xl as base (mobile) size
      expect(headline).toHaveClass('text-4xl')
    })

    it('renders hero subheadline with responsive text sizing', () => {
      renderWithRouter()

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toBeInTheDocument()

      // Subheadline should have text-lg as base (mobile) size
      expect(subheadline).toHaveClass('text-lg')
    })

    it('hero text is centered on mobile viewport', () => {
      renderWithRouter()

      const headline = screen.getByTestId('hero-headline')
      const textContainer = headline.parentElement

      // On mobile, text should be centered (text-center class)
      expect(textContainer).toHaveClass('text-center')
    })

    it('hero visual element is displayed and properly sized', () => {
      renderWithRouter()

      const visual = screen.getByTestId('hero-visual')
      expect(visual).toBeInTheDocument()

      // Visual should have max-w-lg class for proper sizing
      expect(visual).toHaveClass('max-w-lg')
      expect(visual).toHaveClass('w-full')
    })
  })

  // Test Case 4: CTA buttons are full-width or appropriately sized for touch
  describe('Test Case 4: CTA buttons at mobile viewport', () => {
    it('renders primary CTA button with large touch-friendly size', () => {
      renderWithRouter()

      const primaryCTA = screen.getByTestId('hero-cta-primary')
      expect(primaryCTA).toBeInTheDocument()

      // Button should have btn-lg class for touch-friendly sizing
      expect(primaryCTA).toHaveClass('btn-lg')
      expect(primaryCTA).toHaveClass('btn')
      expect(primaryCTA).toHaveClass('btn-primary')
    })

    it('renders secondary CTA button with large touch-friendly size', () => {
      renderWithRouter()

      const secondaryCTA = screen.getByTestId('hero-cta-secondary')
      expect(secondaryCTA).toBeInTheDocument()

      // Button should have btn-lg class for touch-friendly sizing
      expect(secondaryCTA).toHaveClass('btn-lg')
      expect(secondaryCTA).toHaveClass('btn')
      expect(secondaryCTA).toHaveClass('btn-outline')
    })

    it('buttons are contained in a flex column container for mobile stacking', () => {
      renderWithRouter()

      const primaryCTA = screen.getByTestId('hero-cta-primary')
      const buttonContainer = primaryCTA.parentElement

      // Container should flex-col on mobile and sm:flex-row on larger screens
      expect(buttonContainer).toHaveClass('flex')
      expect(buttonContainer).toHaveClass('flex-col')
      expect(buttonContainer).toHaveClass('sm:flex-row')
    })
  })

  // Additional mobile layout tests
  describe('Additional mobile layout tests', () => {
    it('hero section has proper mobile padding', () => {
      renderWithRouter()

      const heroSection = screen.getByTestId('hero-section')

      // Section should have px-4 for horizontal padding on mobile
      expect(heroSection).toHaveClass('px-4')
    })

    it('features section has proper mobile padding', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')

      // Section should have px-4 for horizontal padding on mobile
      expect(featuresSection).toHaveClass('px-4')
    })

    it('hero container uses column layout on mobile and row on large screens', () => {
      renderWithRouter()

      const heroSection = screen.getByTestId('hero-section')
      const container = heroSection.querySelector('.container')

      // Container should be flex-col on mobile, lg:flex-row on large screens
      expect(container).toHaveClass('flex-col')
      expect(container).toHaveClass('lg:flex-row')
    })
  })
})

describe('Responsive Design - Desktop (1280px viewport)', () => {
  const DESKTOP_WIDTH = 1280

  beforeEach(() => {
    vi.clearAllMocks()
    localStorageMock.getItem.mockReturnValue(null)
    setViewportWidth(DESKTOP_WIDTH)
  })

  afterEach(() => {
    // Reset to default
    setViewportWidth(1024)
  })

  // Test Case 1: Full navigation with all links visible in header
  describe('Test Case 1: Navigation at desktop viewport', () => {
    it('renders navigation header with all navigation links visible', () => {
      renderAppWithRouter()

      // Navigation header should be present
      const navHeader = screen.getByTestId('navigation-header')
      expect(navHeader).toBeInTheDocument()

      // Navigation links should be visible (md:flex class shows them on desktop)
      const navContainer = navHeader.querySelector('.hidden.md\\:flex')
      expect(navContainer).toBeInTheDocument()

      // Features link should be visible
      const featuresLink = screen.getByTestId('nav-features')
      expect(featuresLink).toBeInTheDocument()

      // How It Works link should be visible
      const howItWorksLink = screen.getByTestId('nav-how-it-works')
      expect(howItWorksLink).toBeInTheDocument()
    })

    it('renders logo in navigation', () => {
      renderAppWithRouter()

      const logo = screen.getByTestId('nav-logo')
      expect(logo).toBeInTheDocument()
      expect(logo).toHaveTextContent('ShortURL')
    })

    it('renders Login and Register buttons in navigation', () => {
      renderAppWithRouter()

      const loginBtn = screen.getByTestId('nav-login')
      const registerBtn = screen.getByTestId('nav-register')

      expect(loginBtn).toBeInTheDocument()
      expect(registerBtn).toBeInTheDocument()
      expect(registerBtn).toHaveClass('btn-primary')
    })

    it('navigation is fixed at top with proper z-index', () => {
      renderAppWithRouter()

      const navHeader = screen.getByTestId('navigation-header')
      expect(navHeader).toHaveClass('fixed')
      expect(navHeader).toHaveClass('top-0')
      expect(navHeader).toHaveClass('z-50')
    })
  })

  // Test Case 2: Feature cards display in row or multi-column grid
  describe('Test Case 2: Feature cards at desktop viewport', () => {
    it('renders feature cards in a multi-column grid layout', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toBeInTheDocument()

      // Find the grid container
      const gridContainer = featuresSection.querySelector('.grid')
      expect(gridContainer).toBeInTheDocument()

      // Grid should have lg:grid-cols-4 for desktop (4 column layout)
      expect(gridContainer).toHaveClass('lg:grid-cols-4')
    })

    it('renders all 4 feature cards with proper layout', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      const featureCards = featuresSection.querySelectorAll('.card')

      expect(featureCards.length).toBe(4)

      // Each card should have shadow and hover effects
      featureCards.forEach((card) => {
        expect(card).toHaveClass('shadow-xl')
        expect(card).toHaveClass('hover:shadow-2xl')
      })
    })

    it('features section has container with centered content', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      const container = featuresSection.querySelector('.container')

      expect(container).toBeInTheDocument()
      expect(container).toHaveClass('mx-auto')
    })

    it('feature cards grid has responsive breakpoints', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      const gridContainer = featuresSection.querySelector('.grid')

      // Grid should have all responsive breakpoints
      expect(gridContainer).toHaveClass('grid-cols-1')
      expect(gridContainer).toHaveClass('md:grid-cols-2')
      expect(gridContainer).toHaveClass('lg:grid-cols-4')
    })
  })

  // Test Case 3: Hero section uses side-by-side layout for content and visual
  describe('Test Case 3: Hero section at desktop viewport', () => {
    it('hero container uses row layout on desktop (lg:flex-row)', () => {
      renderWithRouter()

      const heroSection = screen.getByTestId('hero-section')
      const container = heroSection.querySelector('.container')

      expect(container).toBeInTheDocument()
      // Container should have lg:flex-row for side-by-side layout
      expect(container).toHaveClass('lg:flex-row')
      expect(container).toHaveClass('items-center')
      expect(container).toHaveClass('justify-between')
    })

    it('hero headline has desktop text sizing (lg:text-6xl)', () => {
      renderWithRouter()

      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()

      // Headline should have lg:text-6xl for desktop
      expect(headline).toHaveClass('lg:text-6xl')
      expect(headline).toHaveClass('md:text-5xl')
      expect(headline).toHaveClass('text-4xl')
    })

    it('hero text is left-aligned on desktop (lg:text-left)', () => {
      renderWithRouter()

      const headline = screen.getByTestId('hero-headline')
      const textContainer = headline.parentElement

      // On desktop, text should be left-aligned
      expect(textContainer).toHaveClass('lg:text-left')
    })

    it('hero subheadline has desktop text sizing (md:text-xl)', () => {
      renderWithRouter()

      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toBeInTheDocument()

      expect(subheadline).toHaveClass('md:text-xl')
      expect(subheadline).toHaveClass('text-lg')
    })

    it('hero visual element is displayed alongside text content', () => {
      renderWithRouter()

      const visual = screen.getByTestId('hero-visual')
      expect(visual).toBeInTheDocument()

      // Visual should be in a flex-1 container for equal sizing
      const visualContainer = visual.parentElement
      expect(visualContainer).toHaveClass('flex-1')
    })

    it('hero content container is flex-1 for equal distribution', () => {
      renderWithRouter()

      const headline = screen.getByTestId('hero-headline')
      // Get the motion.div content container (parent of text elements)
      const contentContainer = headline.parentElement

      expect(contentContainer).toHaveClass('flex-1')
    })

    it('CTA buttons are side-by-side on desktop (sm:flex-row)', () => {
      renderWithRouter()

      const primaryCTA = screen.getByTestId('hero-cta-primary')
      const buttonContainer = primaryCTA.parentElement

      // On desktop/larger screens, buttons should be in a row
      expect(buttonContainer).toHaveClass('sm:flex-row')
      expect(buttonContainer).toHaveClass('gap-4')
    })

    it('CTA buttons justify left on desktop (lg:justify-start)', () => {
      renderWithRouter()

      const primaryCTA = screen.getByTestId('hero-cta-primary')
      const buttonContainer = primaryCTA.parentElement

      expect(buttonContainer).toHaveClass('lg:justify-start')
    })
  })

  // Additional desktop layout tests
  describe('Additional desktop layout tests', () => {
    it('hero section uses full viewport height', () => {
      renderWithRouter()

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('min-h-screen')
    })

    it('hero section has gradient background', () => {
      renderWithRouter()

      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toHaveClass('bg-gradient-to-br')
    })

    it('features section has proper vertical padding', () => {
      renderWithRouter()

      const featuresSection = screen.getByTestId('features-section')
      expect(featuresSection).toHaveClass('py-20')
    })

    it('container uses mx-auto for centering', () => {
      renderWithRouter()

      const heroSection = screen.getByTestId('hero-section')
      const container = heroSection.querySelector('.container')

      expect(container).toHaveClass('mx-auto')
    })

    it('hero container has gap for spacing between content and visual', () => {
      renderWithRouter()

      const heroSection = screen.getByTestId('hero-section')
      const container = heroSection.querySelector('.container')

      expect(container).toHaveClass('gap-12')
    })
  })
})
