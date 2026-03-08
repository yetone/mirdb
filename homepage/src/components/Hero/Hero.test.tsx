import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Hero } from './Hero'

describe('Hero Component', () => {
  describe('Test Case 1: Hero component renders without errors', () => {
    it('should render the Hero component successfully', () => {
      render(<Hero />)
      const heroSection = document.getElementById('hero')
      expect(heroSection).toBeInTheDocument()
      expect(heroSection?.tagName.toLowerCase()).toBe('section')
    })
  })

  describe('Test Case 2: Headline contains key terms', () => {
    it('should contain "persistent" in the headline', () => {
      render(<Hero />)
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline.textContent?.toLowerCase()).toContain('persistent')
    })

    it('should contain "key-value" in the headline', () => {
      render(<Hero />)
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline.textContent?.toLowerCase()).toContain('key-value')
    })

    it('should contain "memcached" in the headline', () => {
      render(<Hero />)
      const headline = screen.getByRole('heading', { level: 1 })
      expect(headline.textContent?.toLowerCase()).toContain('memcached')
    })
  })

  describe('Test Case 3: CTA button presence', () => {
    it('should have a "Get Started" button', () => {
      render(<Hero />)
      const ctaButton = screen.getByRole('link', { name: /get started/i })
      expect(ctaButton).toBeInTheDocument()
    })

    it('should link to the getting-started section', () => {
      render(<Hero />)
      const ctaButton = screen.getByRole('link', { name: /get started/i })
      expect(ctaButton).toHaveAttribute('href', '#getting-started')
    })

    it('should have a visible CTA button with primary styling', () => {
      render(<Hero />)
      const ctaButton = screen.getByRole('link', { name: /get started/i })
      expect(ctaButton).toBeVisible()
      expect(ctaButton).toHaveClass('bg-blue-600')
    })
  })

  describe('Accessibility', () => {
    it('should have proper ARIA labeling', () => {
      render(<Hero />)
      const heroSection = document.getElementById('hero')
      expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-headline')
    })

    it('should have a proper heading hierarchy', () => {
      render(<Hero />)
      const h1 = screen.getByRole('heading', { level: 1 })
      expect(h1).toBeInTheDocument()
    })
  })

  describe('Value Proposition', () => {
    it('should describe MirDB features in the description paragraph', () => {
      render(<Hero />)
      // Get the paragraph element specifically
      const description = document.querySelector('#hero p')
      expect(description).toBeInTheDocument()
      expect(description?.textContent?.toLowerCase()).toContain('memcached protocol')
      expect(description?.textContent?.toLowerCase()).toContain('data persistence')
    })

    it('should mention Rust and performance in the description', () => {
      render(<Hero />)
      const description = document.querySelector('#hero p')
      expect(description?.textContent?.toLowerCase()).toContain('rust')
      expect(description?.textContent?.toLowerCase()).toContain('performance')
    })
  })
})
