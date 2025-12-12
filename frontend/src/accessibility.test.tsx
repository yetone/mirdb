import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import App from './App'
import { Navigation } from './components/Navigation'
import { Footer } from './components/Footer'
import { Hero } from './components/Hero'
import { FeaturedContent } from './components/FeaturedContent'
import { FeaturedCard } from './components/FeaturedContent/FeaturedCard'
import type { FeaturedItem } from './types/FeaturedContent.types'

/**
 * Accessibility Tests - Screen Reader Compatibility (NFR-4)
 * Verifies WCAG 2.1 AA compliance for screen reader compatibility
 *
 * Scenario: Accessibility - Screen Reader Compatibility
 * Description: Verify screen reader compatibility with ARIA labels as per NFR-4
 */

// Helper to render with router context
const renderWithRouter = (ui: React.ReactElement) => {
  return render(<MemoryRouter initialEntries={['/']}>{ui}</MemoryRouter>)
}

/**
 * Test Case 2: Check all images for alt attributes
 * Input: Check all images for alt attributes
 * Expected: All images have descriptive alt text
 * Type: unit
 */
describe('TC2: Image Alt Attributes - All images have descriptive alt text', () => {
  const mockFeaturedItems: FeaturedItem[] = [
    {
      id: '1',
      title: 'Feature One',
      description: 'Description for feature one',
      imageUrl: '/images/feature1.jpg',
      imageAlt: 'Feature one visual representation showing database performance',
      detailUrl: '/features/1',
    },
    {
      id: '2',
      title: 'Feature Two',
      description: 'Description for feature two',
      imageUrl: '/images/feature2.jpg',
      imageAlt: 'Feature two illustration demonstrating key-value storage',
      detailUrl: '/features/2',
    },
  ]

  it('should have alt attribute on all FeaturedCard images', () => {
    renderWithRouter(
      <FeaturedCard item={mockFeaturedItems[0]} />
    )

    const image = screen.getByRole('img')
    expect(image).toHaveAttribute('alt')
    expect(image.getAttribute('alt')).toBeTruthy()
    expect(image.getAttribute('alt')!.length).toBeGreaterThan(0)
  })

  it('should have descriptive alt text that is not just the filename', () => {
    renderWithRouter(
      <FeaturedCard item={mockFeaturedItems[0]} />
    )

    const image = screen.getByRole('img')
    const altText = image.getAttribute('alt')!

    // Alt text should not be just a filename pattern (e.g., "image.jpg", "feature1")
    expect(altText).not.toMatch(/^\w+\.(jpg|jpeg|png|gif|webp|svg)$/i)
    // Should be descriptive (more than just a word)
    expect(altText.split(' ').length).toBeGreaterThanOrEqual(2)
  })

  it('should have alt text that describes the image content', () => {
    renderWithRouter(
      <FeaturedCard item={mockFeaturedItems[0]} />
    )

    const image = screen.getByRole('img')
    const altText = image.getAttribute('alt')

    // Alt should contain meaningful description
    expect(altText).toContain('Feature one')
  })

  it('should have unique alt text for different images', () => {
    renderWithRouter(
      <FeaturedContent items={mockFeaturedItems} sectionTitle="Features" />
    )

    const images = screen.getAllByRole('img')
    const altTexts = images.map(img => img.getAttribute('alt'))

    // Verify all alt texts are unique
    const uniqueAltTexts = new Set(altTexts)
    expect(uniqueAltTexts.size).toBe(altTexts.length)
  })

  it('should have non-empty alt attribute on each image in FeaturedContent', () => {
    renderWithRouter(
      <FeaturedContent items={mockFeaturedItems} sectionTitle="Features" />
    )

    const images = screen.getAllByRole('img')
    images.forEach((img) => {
      expect(img).toHaveAttribute('alt')
      const alt = img.getAttribute('alt')
      expect(alt).toBeTruthy()
      expect(alt!.trim().length).toBeGreaterThan(0)
    })
  })

  it('should have alt text that provides context for screen reader users', () => {
    renderWithRouter(
      <FeaturedCard item={mockFeaturedItems[1]} />
    )

    const image = screen.getByRole('img')
    const altText = image.getAttribute('alt')!

    // Alt text should be meaningful for screen reader context
    // Should not be generic like "image" or "photo"
    expect(altText.toLowerCase()).not.toBe('image')
    expect(altText.toLowerCase()).not.toBe('photo')
    expect(altText.toLowerCase()).not.toBe('picture')
  })
})

/**
 * Test Case 3: Verify navigation has aria-label
 * Input: Verify navigation has aria-label
 * Expected: Navigation element has appropriate aria-label
 * Type: unit
 */
describe('TC3: Navigation Aria-Label - Navigation has appropriate aria-label', () => {
  it('should have aria-label on main navigation', () => {
    renderWithRouter(<Navigation />)

    const nav = screen.getByRole('navigation')
    expect(nav).toHaveAttribute('aria-label')
  })

  it('should have descriptive aria-label on navigation', () => {
    renderWithRouter(<Navigation />)

    const nav = screen.getByRole('navigation')
    const ariaLabel = nav.getAttribute('aria-label')

    expect(ariaLabel).toBeTruthy()
    expect(ariaLabel!.toLowerCase()).toContain('navigation')
  })

  it('should have aria-label that identifies the navigation purpose', () => {
    renderWithRouter(<Navigation />)

    const nav = screen.getByRole('navigation')
    const ariaLabel = nav.getAttribute('aria-label')!

    // Should contain meaningful identifier (e.g., "main", "primary", "site")
    const meaningfulTerms = ['main', 'primary', 'site', 'navigation']
    const hasValidLabel = meaningfulTerms.some(term =>
      ariaLabel.toLowerCase().includes(term)
    )
    expect(hasValidLabel).toBe(true)
  })

  it('should have aria-label on footer navigation', () => {
    renderWithRouter(<Footer />)

    const nav = screen.getByRole('navigation')
    expect(nav).toHaveAttribute('aria-label')

    const ariaLabel = nav.getAttribute('aria-label')
    expect(ariaLabel!.toLowerCase()).toContain('footer')
  })

  it('should have different aria-labels for main and footer navigation', () => {
    renderWithRouter(<App />)

    const navigations = screen.getAllByRole('navigation')
    const ariaLabels = navigations.map(nav => nav.getAttribute('aria-label'))

    // All navigation elements should have aria-labels
    ariaLabels.forEach(label => {
      expect(label).toBeTruthy()
    })

    // Labels should be unique to distinguish between navigations
    const uniqueLabels = new Set(ariaLabels)
    expect(uniqueLabels.size).toBe(ariaLabels.length)
  })

  it('should have aria-expanded attribute on mobile menu toggle', () => {
    renderWithRouter(<Navigation />)

    // The mobile menu toggle is hidden on desktop via CSS (display: none)
    // Use testid to access it directly and verify accessibility attributes
    const menuToggle = screen.getByTestId('mobile-menu-toggle')
    expect(menuToggle).toHaveAttribute('aria-expanded')
  })

  it('should have aria-controls attribute on mobile menu toggle', () => {
    renderWithRouter(<Navigation />)

    // The mobile menu toggle is hidden on desktop via CSS (display: none)
    // Use testid to access it directly and verify accessibility attributes
    const menuToggle = screen.getByTestId('mobile-menu-toggle')
    expect(menuToggle).toHaveAttribute('aria-controls')
  })

  it('should have aria-label on mobile menu toggle for screen readers', () => {
    renderWithRouter(<Navigation />)

    // The mobile menu toggle should have aria-label even when hidden
    const menuToggle = screen.getByTestId('mobile-menu-toggle')
    expect(menuToggle).toHaveAttribute('aria-label')
    expect(menuToggle.getAttribute('aria-label')).toMatch(/(open|close).*navigation.*menu/i)
  })
})

/**
 * Test Case 4: Check buttons and links for accessible names
 * Input: Check buttons and links for accessible names
 * Expected: All interactive elements have accessible names
 * Type: unit
 */
describe('TC4: Interactive Elements Accessible Names - All interactive elements have accessible names', () => {
  describe('Buttons', () => {
    it('should have accessible name on mobile menu toggle button via aria-label', () => {
      renderWithRouter(<Navigation />)

      // The mobile menu toggle is hidden on desktop via CSS
      // Use testid to access it and verify aria-label provides accessible name
      const menuToggle = screen.getByTestId('mobile-menu-toggle')
      // Button should have aria-label that provides accessible name
      expect(menuToggle).toHaveAttribute('aria-label')
      expect(menuToggle.getAttribute('aria-label')!.length).toBeGreaterThan(0)
    })

    it('should have descriptive aria-label on mobile menu toggle', () => {
      renderWithRouter(<Navigation />)

      // The mobile menu toggle is hidden on desktop via CSS
      const menuToggle = screen.getByTestId('mobile-menu-toggle')
      const ariaLabel = menuToggle.getAttribute('aria-label')

      expect(ariaLabel).toBeTruthy()
      // Should describe the action (open/close menu)
      expect(ariaLabel!.toLowerCase()).toMatch(/(open|close|toggle)/)
      expect(ariaLabel!.toLowerCase()).toMatch(/(menu|navigation)/)
    })
  })

  describe('Navigation Links', () => {
    it('should have accessible names for all navigation links', () => {
      renderWithRouter(<Navigation />)

      const links = screen.getAllByRole('link')
      links.forEach((link) => {
        expect(link).toHaveAccessibleName()
      })
    })

    it('should have visible text content in navigation links', () => {
      renderWithRouter(<Navigation />)

      const links = screen.getAllByRole('link')
      links.forEach((link) => {
        const textContent = link.textContent?.trim()
        expect(textContent).toBeTruthy()
        expect(textContent!.length).toBeGreaterThan(0)
      })
    })

    it('should have specific accessible names for each nav link', () => {
      renderWithRouter(<Navigation />)

      // Check that specific links are findable by their accessible names
      expect(screen.getByRole('link', { name: 'Home' })).toBeInTheDocument()
      expect(screen.getByRole('link', { name: 'Features' })).toBeInTheDocument()
      expect(screen.getByRole('link', { name: 'About' })).toBeInTheDocument()
      expect(screen.getByRole('link', { name: 'Contact' })).toBeInTheDocument()
    })
  })

  describe('Hero CTA', () => {
    it('should have accessible name on CTA link', () => {
      renderWithRouter(<Hero ctaText="Get Started" ctaHref="/start" />)

      const ctaLink = screen.getByRole('link', { name: /get started/i })
      expect(ctaLink).toHaveAccessibleName()
    })

    it('should have descriptive CTA text that indicates action', () => {
      renderWithRouter(<Hero ctaText="Get Started" ctaHref="/start" />)

      const ctaLink = screen.getByRole('link')
      const accessibleName = ctaLink.textContent

      // CTA should have action-oriented text
      expect(accessibleName).toBeTruthy()
      expect(accessibleName!.length).toBeGreaterThan(0)
    })
  })

  describe('Footer Links', () => {
    it('should have accessible names for all footer links', () => {
      renderWithRouter(<Footer />)

      const links = screen.getAllByRole('link')
      links.forEach((link) => {
        expect(link).toHaveAccessibleName()
      })
    })

    it('should have aria-label on social links', () => {
      renderWithRouter(<Footer />)

      // Social links use icons and need aria-labels
      const twitterLink = screen.getByRole('link', { name: 'Twitter' })
      const linkedInLink = screen.getByRole('link', { name: 'LinkedIn' })
      const githubLink = screen.getByRole('link', { name: 'GitHub' })

      expect(twitterLink).toHaveAttribute('aria-label')
      expect(linkedInLink).toHaveAttribute('aria-label')
      expect(githubLink).toHaveAttribute('aria-label')
    })

    it('should have descriptive names for legal links', () => {
      renderWithRouter(<Footer />)

      expect(screen.getByRole('link', { name: 'Privacy Policy' })).toBeInTheDocument()
      expect(screen.getByRole('link', { name: 'Terms of Service' })).toBeInTheDocument()
    })

    it('should have accessible email and phone links', () => {
      renderWithRouter(<Footer />)

      const emailLink = screen.getByRole('link', { name: /contact@example\.com/i })
      const phoneLink = screen.getByRole('link', { name: /\+1.*555.*123.*4567/i })

      expect(emailLink).toHaveAccessibleName()
      expect(phoneLink).toHaveAccessibleName()
    })
  })

  describe('FeaturedContent Links', () => {
    const mockItem: FeaturedItem = {
      id: '1',
      title: 'Test Feature',
      description: 'Test description',
      imageUrl: '/test.jpg',
      imageAlt: 'Test image description',
      detailUrl: '/features/1',
    }

    it('should have accessible name on featured card link', () => {
      renderWithRouter(<FeaturedCard item={mockItem} />)

      const link = screen.getByRole('link')
      expect(link).toHaveAccessibleName()
    })

    it('should have aria-label describing the card action', () => {
      renderWithRouter(<FeaturedCard item={mockItem} />)

      const link = screen.getByRole('link')
      const ariaLabel = link.getAttribute('aria-label')

      expect(ariaLabel).toBeTruthy()
      expect(ariaLabel!.toLowerCase()).toContain('test feature')
    })
  })
})

/**
 * Integration test: Full App accessibility check
 */
describe('App-Level Accessibility - Screen Reader Compatibility', () => {
  it('should have all interactive elements with accessible names in the full app', () => {
    renderWithRouter(<App />)

    // Check mobile menu toggle button has aria-label (hidden on desktop)
    const mobileToggle = screen.getByTestId('mobile-menu-toggle')
    expect(mobileToggle).toHaveAttribute('aria-label')
    expect(mobileToggle.getAttribute('aria-label')!.length).toBeGreaterThan(0)

    // Get all links and verify accessible names
    const links = screen.getAllByRole('link')
    links.forEach((link) => {
      expect(link).toHaveAccessibleName()
    })
  })

  it('should have all navigation elements with aria-labels', () => {
    renderWithRouter(<App />)

    const navigations = screen.getAllByRole('navigation')
    navigations.forEach((nav) => {
      expect(nav).toHaveAttribute('aria-label')
    })
  })

  it('should properly announce content to screen readers via semantic HTML', () => {
    renderWithRouter(<App />)

    // Verify semantic landmarks exist
    // Note: Both Navigation header and Hero section can have banner role
    const banners = screen.getAllByRole('banner')
    expect(banners.length).toBeGreaterThanOrEqual(1)

    expect(screen.getByRole('main')).toBeInTheDocument()   // main content
    expect(screen.getByRole('contentinfo')).toBeInTheDocument() // footer

    // Verify headings for navigation
    expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument()
  })
})
