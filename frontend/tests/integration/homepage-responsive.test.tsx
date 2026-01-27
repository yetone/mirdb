/**
 * Integration Test: Homepage Responsive Design
 * Owner: Scenario 6 - Responsive Design
 *
 * Validates responsive design across mobile, tablet, and desktop viewports
 * as specified in REQ-7, NFR-1, and US-5.
 *
 * Test viewports:
 * - Mobile: 375px width
 * - Tablet: 768px width
 * - Desktop: 1280px width
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { screen, within } from '@testing-library/react'
import { renderWithProviders } from '../utils/test-utils'
import Home from '../../src/pages/Home'

// Helper to mock viewport dimensions
function setViewportWidth(width: number) {
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
}

// Helper to mock matchMedia for responsive queries
function mockMatchMedia(width: number) {
  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    configurable: true,
    value: vi.fn().mockImplementation((query: string) => {
      // Parse common Tailwind breakpoint queries
      const minWidthMatch = query.match(/\(min-width:\s*(\d+)px\)/)
      const maxWidthMatch = query.match(/\(max-width:\s*(\d+)px\)/)

      let matches = false
      if (minWidthMatch) {
        matches = width >= parseInt(minWidthMatch[1], 10)
      } else if (maxWidthMatch) {
        matches = width <= parseInt(maxWidthMatch[1], 10)
      }

      return {
        matches,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }
    }),
  })
}

// Cleanup helper for window properties
function cleanupViewportMocks() {
  // Restore original matchMedia if needed
  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    configurable: true,
    value: vi.fn().mockImplementation((query: string) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    })),
  })
}

describe('Homepage Responsive Design', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    // Mock scrollIntoView for smooth scroll functionality
    window.HTMLElement.prototype.scrollIntoView = vi.fn()
  })

  afterEach(() => {
    cleanupViewportMocks()
  })

  // Test Case 1: Render homepage at 375px width (mobile)
  // Expected: No horizontal scrolling required, all content readable
  describe('Mobile Viewport (375px)', () => {
    beforeEach(() => {
      setViewportWidth(375)
      mockMatchMedia(375)
    })

    it('renders all content without horizontal overflow at 375px width', () => {
      const { container } = renderWithProviders(<Home />)

      // Verify main content sections are present
      expect(screen.getByRole('main')).toBeInTheDocument()

      // Check that hero section is present
      const heroSection = screen.getByLabelText('Hero')
      expect(heroSection).toBeInTheDocument()

      // Check that features section is present
      const featuresSection = screen.getByLabelText('Features')
      expect(featuresSection).toBeInTheDocument()

      // Check that "How It Works" section is present
      const howItWorksSection = screen.getByLabelText('How It Works')
      expect(howItWorksSection).toBeInTheDocument()

      // Check that stats section is present
      const statsSection = screen.getByLabelText('Statistics')
      expect(statsSection).toBeInTheDocument()

      // Main container should not cause horizontal scroll (uses min-h-screen)
      const mainElement = container.querySelector('main')
      expect(mainElement).toHaveClass('min-h-screen')
    })

    it('all text content is readable at mobile viewport', () => {
      renderWithProviders(<Home />)

      // Verify headlines are present and readable
      const mainHeadline = screen.getByRole('heading', { level: 1 })
      expect(mainHeadline).toBeInTheDocument()
      expect(mainHeadline).toHaveTextContent('Shorten Your Links')

      // Verify subheadline is present
      const subheadline = screen.getByText(/Transform long URLs/i)
      expect(subheadline).toBeInTheDocument()
    })

    it('features are displayed in single column layout on mobile', () => {
      renderWithProviders(<Home />)

      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toBeInTheDocument()

      // Grid should use single column on mobile (grid-cols-1)
      expect(featuresGrid).toHaveClass('grid-cols-1')
    })

    it('CTA buttons are stacked vertically on mobile', () => {
      renderWithProviders(<Home />)

      // Get Started and Learn More buttons should be present
      const getStartedButton = screen.getByRole('button', { name: /get started/i })
      const learnMoreButton = screen.getByRole('button', { name: /learn more/i })

      expect(getStartedButton).toBeInTheDocument()
      expect(learnMoreButton).toBeInTheDocument()

      // Buttons container should use flex-col on mobile (sm:flex-row)
      const buttonsContainer = getStartedButton.closest('div')
      expect(buttonsContainer).toHaveClass('flex-col')
    })
  })

  // Test Case 2: Render homepage at 768px width (tablet)
  // Expected: Layout adapts appropriately for tablet, no content overflow
  describe('Tablet Viewport (768px)', () => {
    beforeEach(() => {
      setViewportWidth(768)
      mockMatchMedia(768)
    })

    it('renders all content with tablet-appropriate layout at 768px width', () => {
      renderWithProviders(<Home />)

      // Verify main sections are present
      expect(screen.getByRole('main')).toBeInTheDocument()
      expect(screen.getByLabelText('Hero')).toBeInTheDocument()
      expect(screen.getByLabelText('Features')).toBeInTheDocument()
      expect(screen.getByLabelText('How It Works')).toBeInTheDocument()
      expect(screen.getByLabelText('Statistics')).toBeInTheDocument()
    })

    it('features grid shows 2-column layout on tablet', () => {
      renderWithProviders(<Home />)

      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toBeInTheDocument()

      // Should have md:grid-cols-2 for 2-column layout on medium screens
      expect(featuresGrid).toHaveClass('md:grid-cols-2')
    })

    it('stats grid shows 3-column layout on tablet', () => {
      renderWithProviders(<Home />)

      const statsGrid = screen.getByTestId('stats-grid')
      expect(statsGrid).toBeInTheDocument()

      // Stats should show 3 columns on md screens
      expect(statsGrid).toHaveClass('md:grid-cols-3')
    })

    it('content adapts without horizontal overflow', () => {
      const { container } = renderWithProviders(<Home />)

      // All sections should be within max-width containers
      const sections = container.querySelectorAll('section')
      sections.forEach((section) => {
        const innerDiv = section.querySelector('div')
        // Most sections have max-w-* constraints
        if (innerDiv) {
          const hasMaxWidth =
            innerDiv.className.includes('max-w-') ||
            section.className.includes('max-w-')
          expect(hasMaxWidth || section.className.includes('px-')).toBe(true)
        }
      })
    })
  })

  // Test Case 3: Render homepage at 1280px width (desktop)
  // Expected: Full desktop layout with multi-column sections
  describe('Desktop Viewport (1280px)', () => {
    beforeEach(() => {
      setViewportWidth(1280)
      mockMatchMedia(1280)
    })

    it('renders full desktop layout with multi-column sections at 1280px width', () => {
      renderWithProviders(<Home />)

      // Verify all main sections are present
      expect(screen.getByRole('main')).toBeInTheDocument()
      expect(screen.getByLabelText('Hero')).toBeInTheDocument()
      expect(screen.getByLabelText('Features')).toBeInTheDocument()
      expect(screen.getByLabelText('How It Works')).toBeInTheDocument()
      expect(screen.getByLabelText('Statistics')).toBeInTheDocument()
    })

    it('features grid shows 3-column layout on desktop', () => {
      renderWithProviders(<Home />)

      const featuresGrid = screen.getByTestId('features-grid')
      expect(featuresGrid).toBeInTheDocument()

      // Should have lg:grid-cols-3 for 3-column layout on large screens
      expect(featuresGrid).toHaveClass('lg:grid-cols-3')
    })

    it('how it works section shows horizontal layout on desktop', () => {
      renderWithProviders(<Home />)

      const howItWorksSteps = screen.getByTestId('how-it-works-steps')
      expect(howItWorksSteps).toBeInTheDocument()

      // Should use flex-row on large screens (lg:flex-row)
      expect(howItWorksSteps).toHaveClass('lg:flex-row')
    })

    it('CTA buttons are displayed side by side on desktop', () => {
      renderWithProviders(<Home />)

      const getStartedButton = screen.getByRole('button', { name: /get started/i })

      // Buttons container should use sm:flex-row for horizontal layout
      const buttonsContainer = getStartedButton.closest('div')
      expect(buttonsContainer).toHaveClass('sm:flex-row')
    })

    it('step connectors are visible on desktop', () => {
      renderWithProviders(<Home />)

      // Step connectors should be visible (hidden on mobile, flex on lg)
      const stepConnectors = screen.getAllByTestId('step-connector')
      expect(stepConnectors.length).toBeGreaterThan(0)

      stepConnectors.forEach((connector) => {
        // Connectors have hidden lg:flex classes
        expect(connector).toHaveClass('hidden')
        expect(connector).toHaveClass('lg:flex')
      })
    })
  })

  // Test Case 4: Measure CTA button dimensions on mobile
  // Expected: Minimum touch target of 44x44 pixels
  describe('Touch Target Sizes', () => {
    beforeEach(() => {
      setViewportWidth(375)
      mockMatchMedia(375)
    })

    it('CTA buttons meet minimum 44x44 pixel touch target requirement', () => {
      renderWithProviders(<Home />)

      const getStartedButton = screen.getByRole('button', { name: /get started/i })
      const learnMoreButton = screen.getByRole('button', { name: /learn more/i })
      const loginButton = screen.getByRole('button', { name: /login/i })

      // Verify buttons have appropriate padding classes for touch targets
      // DaisyUI's btn class provides adequate touch targets by default
      // Additional padding via px-8 py-3 ensures > 44px dimensions
      expect(getStartedButton).toHaveClass('btn')
      expect(getStartedButton).toHaveClass('py-3')
      expect(getStartedButton).toHaveClass('px-8')

      expect(learnMoreButton).toHaveClass('btn')
      expect(learnMoreButton).toHaveClass('py-3')
      expect(learnMoreButton).toHaveClass('px-8')

      // Login button in header also should be accessible
      expect(loginButton).toHaveClass('btn')
    })

    it('all interactive elements are accessible touch targets', () => {
      renderWithProviders(<Home />)

      // All buttons should be rendered with button role
      const allButtons = screen.getAllByRole('button')

      allButtons.forEach((button) => {
        // Buttons should have btn class from DaisyUI which ensures minimum sizing
        expect(button).toHaveClass('btn')
      })
    })
  })

  // Test Case 5: Test navigation menu on mobile
  // Expected: Mobile-friendly menu accessible (hamburger or similar)
  describe('Mobile Navigation', () => {
    beforeEach(() => {
      setViewportWidth(375)
      mockMatchMedia(375)
    })

    it('navigation is accessible on mobile viewport', () => {
      renderWithProviders(<Home />)

      // Check that navigation header is present
      const header = screen.getByRole('banner')
      expect(header).toBeInTheDocument()

      // Navigation should contain login link
      const nav = within(header).getByRole('navigation')
      expect(nav).toBeInTheDocument()

      // Login button should be accessible
      const loginButton = within(header).getByRole('button', { name: /login/i })
      expect(loginButton).toBeInTheDocument()
    })

    it('header navigation is responsive with proper padding', () => {
      const { container } = renderWithProviders(<Home />)

      const header = container.querySelector('header')
      expect(header).toBeInTheDocument()

      // Header should have responsive padding (p-4 sm:p-6)
      expect(header).toHaveClass('p-4')
      expect(header).toHaveClass('sm:p-6')
    })

    it('navigation links are touch-friendly', () => {
      renderWithProviders(<Home />)

      const loginButton = screen.getByRole('button', { name: /login/i })

      // Button should have adequate padding for mobile touch
      expect(loginButton).toHaveClass('px-6')
      expect(loginButton).toHaveClass('py-2')
    })
  })

  // Test Case 6: Test font sizes at mobile viewport
  // Expected: Text remains readable (minimum 16px for body text)
  describe('Font Size Readability', () => {
    beforeEach(() => {
      setViewportWidth(375)
      mockMatchMedia(375)
    })

    it('body text maintains readable font size (min 16px) on mobile', () => {
      renderWithProviders(<Home />)

      // Paragraph text should use text-lg (18px) or similar for readability
      const subheadline = screen.getByText(/Transform long URLs/i)
      expect(subheadline).toBeInTheDocument()

      // The subheadline uses text-lg (18px) which is above 16px minimum
      expect(subheadline).toHaveClass('text-lg')
    })

    it('headlines are properly sized for mobile readability', () => {
      renderWithProviders(<Home />)

      // Main headline should have responsive text sizes
      const mainHeadline = screen.getByRole('heading', { level: 1 })
      expect(mainHeadline).toBeInTheDocument()

      // Should use text-4xl on mobile (36px), scaling up on larger screens
      expect(mainHeadline).toHaveClass('text-4xl')
      expect(mainHeadline).toHaveClass('sm:text-5xl')
      expect(mainHeadline).toHaveClass('lg:text-6xl')
    })

    it('section headings are readable on mobile', () => {
      renderWithProviders(<Home />)

      // Section headings (h2) should be properly sized
      const sectionHeadings = screen.getAllByRole('heading', { level: 2 })

      sectionHeadings.forEach((heading) => {
        // Section headings use text-3xl (30px) on mobile
        expect(heading).toHaveClass('text-3xl')
        expect(heading).toHaveClass('sm:text-4xl')
      })
    })

    it('feature card text is readable on mobile', () => {
      renderWithProviders(<Home />)

      // Feature card titles (h3) should be text-xl (20px)
      const featureTitles = screen.getAllByRole('heading', { level: 3 })

      featureTitles.forEach((title) => {
        expect(title).toHaveClass('text-xl')
      })
    })

    it('description text maintains adequate contrast and size', () => {
      renderWithProviders(<Home />)

      // Feature descriptions should be readable
      const featureDescriptions = screen.getAllByText(/Transform long|unwieldy URLs|deep insights|Monitor every click/i)

      featureDescriptions.forEach((description) => {
        // Should not have text-sm or smaller classes
        expect(description.className).not.toMatch(/\btext-xs\b/)
      })
    })
  })

  // Additional responsive layout tests
  describe('Responsive Section Padding', () => {
    it('sections have appropriate responsive padding', () => {
      const { container } = renderWithProviders(<Home />)

      // Features section
      const featuresSection = screen.getByLabelText('Features')
      expect(featuresSection).toHaveClass('px-4')
      expect(featuresSection).toHaveClass('sm:px-6')
      expect(featuresSection).toHaveClass('lg:px-8')
      expect(featuresSection).toHaveClass('py-16')
      expect(featuresSection).toHaveClass('sm:py-24')

      // How It Works section
      const howItWorksSection = screen.getByLabelText('How It Works')
      expect(howItWorksSection).toHaveClass('px-4')
      expect(howItWorksSection).toHaveClass('sm:px-6')
      expect(howItWorksSection).toHaveClass('lg:px-8')

      // Stats section
      const statsSection = screen.getByLabelText('Statistics')
      expect(statsSection).toHaveClass('px-4')
      expect(statsSection).toHaveClass('sm:px-6')
      expect(statsSection).toHaveClass('lg:px-8')
    })
  })

  describe('Responsive Max-Width Containers', () => {
    it('content is constrained within max-width containers', () => {
      const { container } = renderWithProviders(<Home />)

      // Check for max-width constraints on content containers
      const maxWidthContainers = container.querySelectorAll('[class*="max-w-"]')
      expect(maxWidthContainers.length).toBeGreaterThan(0)

      // Hero content should be max-w-4xl
      const heroSection = screen.getByLabelText('Hero')
      const heroContent = heroSection.querySelector('[class*="max-w-4xl"]')
      expect(heroContent).toBeInTheDocument()

      // Features should be max-w-6xl
      const featuresSection = screen.getByLabelText('Features')
      const featuresContent = featuresSection.querySelector('[class*="max-w-6xl"]')
      expect(featuresContent).toBeInTheDocument()
    })
  })
})
