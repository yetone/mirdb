/**
 * URL Preview Component Unit Tests
 * Owner: Scenario 9 - URL Shortening Preview/Demo
 *
 * Tests for the UrlPreview component that demonstrates URL shortening.
 *
 * Test coverage:
 * - URL preview section is present
 * - Preview shows example of long URL being shortened
 * - Preview section is visually distinct and noticeable
 */

import { describe, it, expect } from 'vitest'
import { render, screen, within } from '../../utils/render'
import { UrlPreview } from '../../../src/components/homepage/UrlPreview'
import { Home } from '../../../src/pages/Home'

describe('UrlPreview Component', () => {
  describe('Test Case 1: URL preview/demo section is present on page', () => {
    it('should render the URL preview section', () => {
      render(<UrlPreview />)

      // Verify the URL preview section is present
      const previewSection = screen.getByTestId('url-preview')
      expect(previewSection).toBeInTheDocument()
    })

    it('should be present in the Home page', () => {
      render(<Home />)

      // Verify the URL preview section is present in Home page
      const previewSection = screen.getByTestId('url-preview')
      expect(previewSection).toBeInTheDocument()
    })

    it('should have proper accessibility label', () => {
      render(<UrlPreview />)

      // Verify the section has aria-label for accessibility
      const previewSection = screen.getByRole('region', { name: /url shortening preview/i })
      expect(previewSection).toBeInTheDocument()
    })

    it('should render inside a glass morphism card', () => {
      render(<UrlPreview />)

      // Verify it uses GlassMorphismCard component
      const glassCard = screen.getByTestId('glass-card')
      expect(glassCard).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Preview shows example of long URL being shortened', () => {
    it('should display a long URL example', () => {
      render(<UrlPreview />)

      // Verify long URL is displayed
      const longUrlElement = screen.getByTestId('long-url')
      expect(longUrlElement).toBeInTheDocument()
      expect(longUrlElement.textContent).toMatch(/https:\/\/example\.com/)
      expect(longUrlElement.textContent).toMatch(/articles/)
    })

    it('should display a short URL result', () => {
      render(<UrlPreview />)

      // Verify short URL is displayed
      const shortUrlElement = screen.getByTestId('short-url')
      expect(shortUrlElement).toBeInTheDocument()
      expect(shortUrlElement.textContent).toMatch(/short\.ly/)
    })

    it('should show the URL transformation with visual indicator', () => {
      render(<UrlPreview />)

      // Verify transformation container exists
      const transformation = screen.getByTestId('url-transformation')
      expect(transformation).toBeInTheDocument()

      // Verify arrow indicator between long and short URLs
      const arrow = screen.getByTestId('transformation-arrow')
      expect(arrow).toBeInTheDocument()
    })

    it('should display both long URL container and short URL container', () => {
      render(<UrlPreview />)

      // Verify both containers exist
      const longUrlContainer = screen.getByTestId('long-url-container')
      expect(longUrlContainer).toBeInTheDocument()

      const shortUrlContainer = screen.getByTestId('short-url-container')
      expect(shortUrlContainer).toBeInTheDocument()
    })

    it('should demonstrate URL length reduction in benefits section', () => {
      render(<UrlPreview />)

      // Verify benefits callout mentions character reduction
      const benefits = screen.getByTestId('preview-benefits')
      expect(benefits).toBeInTheDocument()
      expect(benefits.textContent).toMatch(/characters/i)
      expect(benefits.textContent).toMatch(/reduced/i)
    })
  })

  describe('Test Case 3: Preview section is visually distinct and noticeable', () => {
    it('should have a prominent title', () => {
      render(<UrlPreview />)

      // Verify title is present and visible
      const title = screen.getByTestId('preview-title')
      expect(title).toBeInTheDocument()
      expect(title.textContent).toMatch(/how it works/i)
    })

    it('should have a descriptive subtitle', () => {
      render(<UrlPreview />)

      // Verify description is present
      const description = screen.getByTestId('preview-description')
      expect(description).toBeInTheDocument()
      expect(description.textContent).toMatch(/transform/i)
    })

    it('should apply distinct styling to the short URL', () => {
      render(<UrlPreview />)

      // Verify short URL has distinct styling (primary color border)
      const shortUrl = screen.getByTestId('short-url')
      expect(shortUrl).toHaveClass('short-url')
      expect(shortUrl).toHaveClass('border-primary')
    })

    it('should have proper visual hierarchy with labeled sections', () => {
      render(<UrlPreview />)

      // Verify labels are present for both URL sections
      expect(screen.getByText(/your long url/i)).toBeInTheDocument()
      expect(screen.getByText(/your short link/i)).toBeInTheDocument()
    })

    it('should use the url-preview CSS class for styling', () => {
      render(<UrlPreview />)

      const previewSection = screen.getByTestId('url-preview')
      expect(previewSection).toHaveClass('url-preview')
    })

    it('should display an icon for visual appeal', () => {
      render(<UrlPreview />)

      // The component uses Zap icon in the header which should be rendered
      const previewSection = screen.getByTestId('url-preview')
      // Check for SVG elements (icons from lucide-react)
      const svgElements = previewSection.querySelectorAll('svg')
      expect(svgElements.length).toBeGreaterThan(0)
    })
  })

  describe('Integration with Home page', () => {
    it('should render URL preview inside the url-preview-section on Home page', () => {
      render(<Home />)

      // Find the url-preview-section wrapper
      const sectionWrapper = screen.getByTestId('url-preview-section')
      expect(sectionWrapper).toBeInTheDocument()

      // The UrlPreview component should be inside it
      const urlPreview = within(sectionWrapper).getByTestId('url-preview')
      expect(urlPreview).toBeInTheDocument()
    })

    it('should display complete transformation demo on Home page', () => {
      render(<Home />)

      // Verify all key elements are present on the home page
      expect(screen.getByTestId('long-url')).toBeInTheDocument()
      expect(screen.getByTestId('short-url')).toBeInTheDocument()
      expect(screen.getByTestId('transformation-arrow')).toBeInTheDocument()
    })
  })
})
