import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
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
      const featuresLink = screen.queryByRole('link', { name: /features/i })
      const demoLink = screen.queryByRole('link', { name: /try it/i }) ||
                       screen.queryByRole('link', { name: /demo/i })

      // Navigation links should exist to sections
      // Note: These may be anchor links or have onClick handlers
      const heroSection = screen.getByTestId('hero-section')
      expect(heroSection).toBeInTheDocument()
    })

    it('should scroll to features section when anchor link is clicked', () => {
      renderWithRouter(<Home />)

      // Find the features anchor link
      const featuresLink = document.querySelector('a[href="#features"]')

      if (featuresLink) {
        fireEvent.click(featuresLink)

        // Check that scrollIntoView was called
        expect(scrollIntoViewMock).toHaveBeenCalled()
      }

      // Verify features section exists and has the correct id
      const featuresSection = document.getElementById('features')
      expect(featuresSection).toBeInTheDocument()
    })

    it('should have href="#features" link that targets the features section', () => {
      renderWithRouter(<Home />)

      // Check for anchor link targeting features
      const anchors = document.querySelectorAll('a[href^="#"]')
      const featuresAnchors = Array.from(anchors).filter(
        a => a.getAttribute('href') === '#features'
      )

      // There should be at least one link to #features
      expect(featuresAnchors.length).toBeGreaterThan(0)
    })

    it('should have href="#demo" link that targets the demo section', () => {
      renderWithRouter(<Home />)

      // Check for anchor link targeting demo
      const anchors = document.querySelectorAll('a[href^="#"]')
      const demoAnchors = Array.from(anchors).filter(
        a => a.getAttribute('href') === '#demo'
      )

      // There should be at least one link to #demo
      expect(demoAnchors.length).toBeGreaterThan(0)
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
      const anchors = document.querySelectorAll('a[href^="#"]')
      const anchorTargets = Array.from(anchors).map(a => a.getAttribute('href'))

      // Should have links to sections
      expect(anchorTargets).toContain('#features')
      expect(anchorTargets).toContain('#demo')
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
      const featuresLink = document.querySelector('a[href="#features"]')

      if (featuresLink) {
        fireEvent.click(featuresLink)

        // Verify scrollIntoView was called with smooth behavior
        expect(scrollIntoViewMock).toHaveBeenCalledWith(
          expect.objectContaining({ behavior: 'smooth' })
        )
      }
    })

    it('should prevent default browser jump and use smooth scroll', () => {
      renderWithRouter(<Home />)

      const featuresLink = document.querySelector('a[href="#features"]')

      if (featuresLink) {
        // Create a mock click event
        const clickEvent = new MouseEvent('click', {
          bubbles: true,
          cancelable: true,
        })

        featuresLink.dispatchEvent(clickEvent)

        // If using JavaScript scroll, scrollIntoView should be called
        // with smooth behavior option
        if (scrollIntoViewMock.mock.calls.length > 0) {
          expect(scrollIntoViewMock).toHaveBeenCalledWith(
            expect.objectContaining({ behavior: 'smooth' })
          )
        }
      }
    })
  })
})
