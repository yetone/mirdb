import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Hero } from './Hero'
import { GITHUB_REPO_URL } from '../../utils/constants'

describe('Hero Component', () => {
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
})
