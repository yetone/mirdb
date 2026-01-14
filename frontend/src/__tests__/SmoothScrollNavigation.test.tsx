import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import { render, screen, fireEvent, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BrowserRouter } from 'react-router-dom'
import Home from '../pages/Home'
import HeroSection from '../components/HeroSection'
import FeaturesSection from '../components/FeaturesSection'
import DemoSection from '../components/DemoSection'

// Test wrapper with router
const renderWithRouter = (ui: React.ReactElement) => {
  return render(<BrowserRouter>{ui}</BrowserRouter>)
}

describe('Smooth Scroll Navigation (REQ-10)', () => {
  describe('Test Case 1: Click anchor link to #features section', () => {
    let scrollIntoViewMock: ReturnType<typeof vi.fn>

    beforeEach(() => {
      // Mock scrollIntoView to capture calls
      scrollIntoViewMock = vi.fn()
      Element.prototype.scrollIntoView = scrollIntoViewMock
    })

    afterEach(() => {
      vi.restoreAllMocks()
    })

    it('should have navigation links that target homepage sections', () => {
      renderWithRouter(<Home />)

      // Check for section navigation links in hero
      const navFeatures = screen.getByTestId('nav-features')
      const navDemo = screen.getByTestId('nav-demo')

      expect(navFeatures).toBeInTheDocument()
      expect(navDemo).toBeInTheDocument()
    })

    it('should scroll to features section when anchor link is clicked', () => {
      renderWithRouter(<Home />)

      // Find the features anchor link
      const featuresLink = screen.getByTestId('nav-features')

      fireEvent.click(featuresLink)

      // Check that scrollIntoView was called
      expect(scrollIntoViewMock).toHaveBeenCalled()

      // Verify features section exists and has the correct id
      const featuresSection = document.getElementById('features')
      expect(featuresSection).toBeInTheDocument()
    })

    it('should have href="#features" link that targets the features section', () => {
      renderWithRouter(<Home />)

      // Check for anchor link targeting features
      const navFeatures = screen.getByTestId('nav-features')
      expect(navFeatures).toHaveAttribute('href', '#features')
    })

    it('should have href="#demo" link that targets the demo section', () => {
      renderWithRouter(<Home />)

      // Check for anchor link targeting demo
      const navDemo = screen.getByTestId('nav-demo')
      expect(navDemo).toHaveAttribute('href', '#demo')
    })

    it('should have navigation links within a nav element with proper aria-label', () => {
      renderWithRouter(<HeroSection />)

      const nav = screen.getByRole('navigation', { name: /page sections/i })
      expect(nav).toBeInTheDocument()

      const navFeatures = within(nav).getByTestId('nav-features')
      const navDemo = within(nav).getByTestId('nav-demo')

      expect(navFeatures).toBeInTheDocument()
      expect(navDemo).toBeInTheDocument()
    })

    it('should allow keyboard navigation to section links', async () => {
      const user = userEvent.setup()
      renderWithRouter(<HeroSection />)

      // Tab through to reach the section navigation links
      // Order: Get Started -> Login -> Features -> Try It
      await user.tab() // Get Started
      await user.tab() // Login
      await user.tab() // Features

      const navFeatures = screen.getByTestId('nav-features')
      expect(document.activeElement).toBe(navFeatures)

      await user.tab() // Try It
      const navDemo = screen.getByTestId('nav-demo')
      expect(document.activeElement).toBe(navDemo)
    })
  })

  describe('Test Case 2: CSS scroll-behavior property on homepage', () => {
    it('should have scroll-behavior: smooth set on html element via CSS', async () => {
      // Read the CSS file content to verify scroll-behavior is set
      // Note: jsdom doesn't load external CSS, so we verify the CSS file content
      const fs = await import('fs')
      const path = await import('path')

      const cssPath = path.resolve(__dirname, '../index.css')
      const cssContent = fs.readFileSync(cssPath, 'utf8')

      // Verify the CSS file contains scroll-behavior: smooth for html
      const hasScrollBehavior = cssContent.includes('scroll-behavior: smooth') ||
                                 cssContent.includes('scroll-behavior:smooth')

      expect(hasScrollBehavior).toBe(true)

      // Also verify it applies to html element
      const htmlRule = /html\s*\{[^}]*scroll-behavior:\s*smooth[^}]*\}/
      expect(htmlRule.test(cssContent)).toBe(true)
    })

    it('should have smooth scrolling enabled via CSS file', async () => {
      const fs = await import('fs')
      const path = await import('path')

      const cssPath = path.resolve(__dirname, '../index.css')
      const cssContent = fs.readFileSync(cssPath, 'utf8')

      // Verify CSS contains scroll-behavior rule
      expect(cssContent).toContain('scroll-behavior')

      // Render the app to ensure it loads without errors
      renderWithRouter(<Home />)

      // Verify html element exists
      const html = document.documentElement
      expect(html).toBeTruthy()
    })

    it('should render the homepage with proper structure for smooth scrolling', () => {
      renderWithRouter(<Home />)

      // Verify the main container exists
      const main = document.querySelector('main')
      expect(main).toBeInTheDocument()

      // Verify sections are present and in order
      const heroSection = screen.getByTestId('hero-section')
      const demoSection = screen.getByTestId('demo-section')
      const footerSection = screen.getByTestId('footer-section')

      expect(heroSection).toBeInTheDocument()
      expect(demoSection).toBeInTheDocument()
      expect(footerSection).toBeInTheDocument()
    })
  })

  describe('Test Case 3: Section elements have proper id attributes', () => {
    it('should have FeaturesSection with id="features"', () => {
      renderWithRouter(<FeaturesSection />)

      const section = document.getElementById('features')
      expect(section).toBeInTheDocument()
      expect(section?.tagName.toLowerCase()).toBe('section')
    })

    it('should have DemoSection with id="demo"', () => {
      renderWithRouter(<DemoSection />)

      const section = document.getElementById('demo')
      expect(section).toBeInTheDocument()
      expect(section?.tagName.toLowerCase()).toBe('section')
    })

    it('should have HeroSection with id="hero"', () => {
      renderWithRouter(<HeroSection />)

      const section = document.getElementById('hero')
      expect(section).toBeInTheDocument()
      expect(section?.tagName.toLowerCase()).toBe('section')
    })

    it('should have all sections with matching navigation anchor targets', () => {
      renderWithRouter(<Home />)

      // Get all section ids
      const heroSection = document.getElementById('hero')
      const featuresSection = document.getElementById('features')
      const demoSection = document.getElementById('demo')

      expect(heroSection).toBeInTheDocument()
      expect(featuresSection).toBeInTheDocument()
      expect(demoSection).toBeInTheDocument()

      // Verify anchors match section ids
      const navFeatures = screen.getByTestId('nav-features')
      const navDemo = screen.getByTestId('nav-demo')

      expect(navFeatures).toHaveAttribute('href', '#features')
      expect(navDemo).toHaveAttribute('href', '#demo')
    })

    it('should have sections in correct DOM order for logical navigation', () => {
      renderWithRouter(<Home />)

      const heroSection = document.getElementById('hero')
      const featuresSection = document.getElementById('features')
      const demoSection = document.getElementById('demo')

      expect(heroSection).toBeInTheDocument()
      expect(featuresSection).toBeInTheDocument()
      expect(demoSection).toBeInTheDocument()

      // Verify DOM order using compareDocumentPosition
      if (heroSection && featuresSection) {
        const heroBeforeFeatures = heroSection.compareDocumentPosition(featuresSection) &
          Node.DOCUMENT_POSITION_FOLLOWING
        expect(heroBeforeFeatures).toBeTruthy()
      }

      if (featuresSection && demoSection) {
        const featuresBeforeDemo = featuresSection.compareDocumentPosition(demoSection) &
          Node.DOCUMENT_POSITION_FOLLOWING
        expect(featuresBeforeDemo).toBeTruthy()
      }
    })
  })

  describe('Smooth scroll anchor click behavior', () => {
    let scrollIntoViewMock: ReturnType<typeof vi.fn>

    beforeEach(() => {
      scrollIntoViewMock = vi.fn()
      Element.prototype.scrollIntoView = scrollIntoViewMock
    })

    afterEach(() => {
      vi.restoreAllMocks()
    })

    it('should call scrollIntoView with smooth behavior when clicking section anchor', () => {
      renderWithRouter(<Home />)

      // Find anchor link to features
      const featuresLink = screen.getByTestId('nav-features')

      fireEvent.click(featuresLink)

      // Verify scrollIntoView was called with smooth behavior
      expect(scrollIntoViewMock).toHaveBeenCalledWith(
        expect.objectContaining({ behavior: 'smooth' })
      )
    })

    it('should render anchor links as standard <a> tags for native scroll behavior', () => {
      renderWithRouter(<HeroSection />)

      const navFeatures = screen.getByTestId('nav-features')
      const navDemo = screen.getByTestId('nav-demo')

      // Verify they are anchor tags (not buttons or other elements)
      expect(navFeatures.tagName).toBe('A')
      expect(navDemo.tagName).toBe('A')

      // Verify href attributes start with #
      expect(navFeatures.getAttribute('href')?.startsWith('#')).toBe(true)
      expect(navDemo.getAttribute('href')?.startsWith('#')).toBe(true)
    })
  })

  describe('Accessibility for smooth scroll navigation', () => {
    it('should have hover and focus styles on navigation links', () => {
      renderWithRouter(<HeroSection />)

      const navFeatures = screen.getByTestId('nav-features')
      const navDemo = screen.getByTestId('nav-demo')

      // Check for transition-colors class which enables smooth hover effects
      expect(navFeatures.classList.contains('transition-colors')).toBe(true)
      expect(navDemo.classList.contains('transition-colors')).toBe(true)

      // Check for hover styles
      expect(navFeatures.classList.contains('hover:text-primary')).toBe(true)
      expect(navDemo.classList.contains('hover:text-primary')).toBe(true)
    })

    it('should have link-hover class for consistent styling', () => {
      renderWithRouter(<HeroSection />)

      const navFeatures = screen.getByTestId('nav-features')
      const navDemo = screen.getByTestId('nav-demo')

      expect(navFeatures.classList.contains('link')).toBe(true)
      expect(navFeatures.classList.contains('link-hover')).toBe(true)
      expect(navDemo.classList.contains('link')).toBe(true)
      expect(navDemo.classList.contains('link-hover')).toBe(true)
    })
  })
})
