/**
 * Unit tests for Lazy Loading Images
 * Scenario 12 - Performance and Loading
 *
 * Test Case 5: Images below fold have loading='lazy' attribute
 * This tests the lazy loading implementation at the component level
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Demo } from '../../../src/components/sections/Demo'

describe('Lazy Loading Images - Unit Tests', () => {
  describe('Test Case 5: Below-fold images have loading="lazy" attribute', () => {
    it('Demo image (below fold) should have loading="lazy" attribute', () => {
      render(<Demo />)

      const image = screen.getByRole('img')
      expect(image).toBeInTheDocument()

      // Verify the loading attribute is set to 'lazy'
      expect(image).toHaveAttribute('loading', 'lazy')
    })

    it('Demo image should have width and height to prevent CLS', () => {
      render(<Demo />)

      const image = screen.getByRole('img')

      // Width and height attributes help prevent Cumulative Layout Shift
      expect(image).toHaveAttribute('width')
      expect(image).toHaveAttribute('height')

      // Verify they have numeric values
      const width = image.getAttribute('width')
      const height = image.getAttribute('height')
      expect(Number(width)).toBeGreaterThan(0)
      expect(Number(height)).toBeGreaterThan(0)
    })

    it('Demo image should have proper src pointing to usage.gif', () => {
      render(<Demo />)

      const image = screen.getByRole('img')
      const src = image.getAttribute('src')

      expect(src).toContain('usage.gif')
    })

    it('Demo image should have descriptive alt text for accessibility', () => {
      render(<Demo />)

      const image = screen.getByRole('img')
      const alt = image.getAttribute('alt')

      expect(alt).toBeTruthy()
      expect(alt!.length).toBeGreaterThan(10) // Descriptive alt text should be meaningful
    })
  })

  describe('Performance Attributes', () => {
    it('lazy loaded images should not block page render', () => {
      const { container } = render(<Demo />)

      // The demo section should render immediately
      const section = container.querySelector('.demo')
      expect(section).toBeInTheDocument()

      // A placeholder should be shown while image loads
      const placeholder = container.querySelector('.demo__placeholder')
      expect(placeholder).toBeInTheDocument()

      // The image should be in the DOM but may not be loaded yet
      const image = screen.getByRole('img')
      expect(image).toBeInTheDocument()

      // Image initially should not have the loaded class
      expect(image).not.toHaveClass('demo__image--loaded')
    })
  })
})
