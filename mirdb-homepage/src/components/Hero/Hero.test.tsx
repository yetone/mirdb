import { describe, it, expect } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { Hero } from './Hero'
import { GITHUB_REPO_URL } from '../../utils/constants'

describe('Hero Component', () => {
  // ==============================================
  // Scenario 17: Component Isolation and Props
  // Test Case 1: Render Hero with custom tagline prop
  // Test Case 5: Test component with missing optional props
  // ==============================================

  describe('Component Isolation - Custom Props (Test Case 1)', () => {
    it('displays the provided custom tagline text', () => {
      const customTagline = 'A custom tagline for testing isolation'
      render(<Hero tagline={customTagline} />)

      const subtitle = screen.getByRole('heading', { level: 2 })
      expect(subtitle).toHaveTextContent(customTagline)
    })

    it('renders custom tagline with special characters', () => {
      const specialTagline = 'Store <key, value> pairs efficiently!'
      render(<Hero tagline={specialTagline} />)

      const subtitle = screen.getByRole('heading', { level: 2 })
      expect(subtitle).toHaveTextContent(specialTagline)
    })

    it('renders with all custom props simultaneously', () => {
      render(
        <Hero
          logoSrc="/custom-logo.png"
          tagline="Custom Tagline"
          ctaText="Custom CTA"
          ctaHref="https://custom.example.com"
        />
      )

      const logo = screen.getByAltText('MirDB')
      const subtitle = screen.getByRole('heading', { level: 2 })
      const cta = screen.getByRole('link', { name: /custom cta/i })

      expect(logo).toHaveAttribute('src', '/custom-logo.png')
      expect(subtitle).toHaveTextContent('Custom Tagline')
      expect(cta).toHaveAttribute('href', 'https://custom.example.com')
    })
  })

  describe('Component Isolation - Default Props (Test Case 5)', () => {
    it('renders without errors when no props are provided', () => {
      const { container } = render(<Hero />)
      expect(container).toBeInTheDocument()
    })

    it('uses default logo source when logoSrc prop is missing', () => {
      render(<Hero />)
      const logo = screen.getByAltText('MirDB')
      expect(logo).toHaveAttribute('src', '/logo.svg')
    })

    it('uses default tagline when tagline prop is missing', () => {
      render(<Hero />)
      const subtitle = screen.getByRole('heading', { level: 2 })
      expect(subtitle).toHaveTextContent('A Persistent Key-Value Store with Memcached Protocol')
    })

    it('uses default CTA text when ctaText prop is missing', () => {
      render(<Hero />)
      const cta = screen.getByRole('link', { name: /get started/i })
      expect(cta).toBeInTheDocument()
    })

    it('uses default CTA href (GitHub URL) when ctaHref prop is missing', () => {
      render(<Hero />)
      const cta = screen.getByRole('link', { name: /get started/i })
      expect(cta).toHaveAttribute('href', GITHUB_REPO_URL)
    })

    it('handles partial props - only logoSrc provided', () => {
      render(<Hero logoSrc="/partial-test.svg" />)

      const logo = screen.getByAltText('MirDB')
      expect(logo).toHaveAttribute('src', '/partial-test.svg')

      // Other defaults should still work
      const subtitle = screen.getByRole('heading', { level: 2 })
      expect(subtitle.textContent?.toLowerCase()).toContain('persistent')
      expect(screen.getByRole('link', { name: /get started/i })).toBeInTheDocument()
    })

    it('handles partial props - only tagline provided', () => {
      render(<Hero tagline="Partial Test Tagline" />)

      const subtitle = screen.getByRole('heading', { level: 2 })
      expect(subtitle).toHaveTextContent('Partial Test Tagline')

      // Other defaults should still work
      const logo = screen.getByAltText('MirDB')
      expect(logo).toHaveAttribute('src', '/logo.svg')
    })
  })

  describe('Test Case 1: Logo presence and dimensions', () => {
    it('renders logo image with alt text "MirDB" and minimum 120x120px dimensions', () => {
      render(<Hero />)
      const logo = screen.getByAltText('MirDB')
      expect(logo).toBeInTheDocument()
      expect(logo).toHaveAttribute('width', '120')
      expect(logo).toHaveAttribute('height', '120')
      expect(logo).toHaveStyle({ minWidth: '120px', minHeight: '120px' })
    })

    it('renders logo with correct src attribute', () => {
      render(<Hero />)
      const logo = screen.getByAltText('MirDB')
      expect(logo).toHaveAttribute('src', '/logo.svg')
    })

    it('accepts custom logo source', () => {
      render(<Hero logoSrc="/custom-logo.svg" />)
      const logo = screen.getByAltText('MirDB')
      expect(logo).toHaveAttribute('src', '/custom-logo.svg')
    })
  })

  describe('Test Case 2: Product name in H1', () => {
    it('displays "MirDB" as H1 heading', () => {
      render(<Hero />)
      const heading = screen.getByRole('heading', { level: 1 })
      expect(heading).toBeInTheDocument()
      expect(heading).toHaveTextContent('MirDB')
    })
  })

  describe('Test Case 3: Value proposition tagline', () => {
    it('displays tagline about persistent key-value store and memcached protocol', () => {
      render(<Hero />)
      const subtitle = screen.getByRole('heading', { level: 2 })
      expect(subtitle).toBeInTheDocument()
      expect(subtitle.textContent?.toLowerCase()).toContain('persistent')
      expect(subtitle.textContent?.toLowerCase()).toContain('key-value')
      expect(subtitle.textContent?.toLowerCase()).toContain('memcached')
      expect(subtitle.textContent?.toLowerCase()).toContain('protocol')
    })

    it('accepts custom tagline', () => {
      const customTagline = 'Custom tagline text'
      render(<Hero tagline={customTagline} />)
      const subtitle = screen.getByRole('heading', { level: 2 })
      expect(subtitle).toHaveTextContent(customTagline)
    })
  })

  describe('Test Case 4: Get Started CTA button', () => {
    it('renders "Get Started" button with link to GitHub repository', () => {
      render(<Hero />)
      const ctaButton = screen.getByRole('link', { name: /get started/i })
      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton).toHaveAttribute('href', GITHUB_REPO_URL)
    })

    it('opens GitHub link in new tab with proper security attributes', () => {
      render(<Hero />)
      const ctaButton = screen.getByRole('link', { name: /get started/i })
      expect(ctaButton).toHaveAttribute('target', '_blank')
      expect(ctaButton).toHaveAttribute('rel', 'noopener noreferrer')
    })

    it('accepts custom CTA text and href', () => {
      render(<Hero ctaText="View Docs" ctaHref="https://docs.example.com" />)
      const ctaButton = screen.getByRole('link', { name: /view docs/i })
      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton).toHaveAttribute('href', 'https://docs.example.com')
    })
  })

  describe('Component rendering', () => {
    it('renders without crashing', () => {
      const { container } = render(<Hero />)
      expect(container).toBeInTheDocument()
    })

    it('renders as a section element', () => {
      render(<Hero />)
      const section = document.querySelector('section')
      expect(section).toBeInTheDocument()
    })

    it('has proper styling classes for centered layout', () => {
      render(<Hero />)
      const section = document.querySelector('section')
      expect(section).toHaveClass('flex', 'flex-col', 'items-center', 'justify-center')
    })
  })

  // Error Handling Tests (Scenario 16)
  describe('Error Handling - Logo Load Failure', () => {
    it('shows fallback when logo image fails to load', () => {
      render(<Hero />)

      const logoImage = screen.getByTestId('hero-logo')

      // Simulate image load error
      fireEvent.error(logoImage)

      // Fallback should now be visible
      const fallback = screen.getByTestId('logo-fallback')
      expect(fallback).toBeInTheDocument()
    })

    it('fallback displays M letter as placeholder', () => {
      render(<Hero />)

      const logoImage = screen.getByTestId('hero-logo')
      fireEvent.error(logoImage)

      const fallback = screen.getByTestId('logo-fallback')
      expect(fallback).toHaveTextContent('M')
    })

    it('fallback has proper aria-label for accessibility', () => {
      render(<Hero />)

      const logoImage = screen.getByTestId('hero-logo')
      fireEvent.error(logoImage)

      const fallback = screen.getByTestId('logo-fallback')
      expect(fallback).toHaveAttribute('aria-label', 'MirDB Logo')
      expect(fallback).toHaveAttribute('role', 'img')
    })

    it('fallback maintains minimum dimensions', () => {
      render(<Hero />)

      const logoImage = screen.getByTestId('hero-logo')
      fireEvent.error(logoImage)

      const fallback = screen.getByTestId('logo-fallback')
      expect(fallback).toHaveStyle({ minWidth: '120px', minHeight: '120px' })
    })

    it('page content remains visible when logo fails', () => {
      render(<Hero />)

      const logoImage = screen.getByTestId('hero-logo')
      fireEvent.error(logoImage)

      // Heading should still be visible
      const heading = screen.getByRole('heading', { level: 1 })
      expect(heading).toHaveTextContent('MirDB')

      // CTA should still be visible
      const cta = screen.getByRole('link', { name: /get started/i })
      expect(cta).toBeInTheDocument()
    })

    it('hides original image when error occurs', () => {
      render(<Hero />)

      const logoImage = screen.getByTestId('hero-logo')
      fireEvent.error(logoImage)

      // Original image should no longer be in DOM
      expect(screen.queryByTestId('hero-logo')).not.toBeInTheDocument()
    })
  })
})
