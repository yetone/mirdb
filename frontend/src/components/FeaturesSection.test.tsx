import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import FeaturesSection from './FeaturesSection'

vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: { children: React.ReactNode; [key: string]: unknown }) => (
      <div {...props}>{children}</div>
    ),
  },
}))

describe('FeaturesSection', () => {
  describe('Test Case 1: Component renders 4 feature cards with icons, titles, and descriptions', () => {
    it('renders the FeaturesSection component', () => {
      render(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      expect(section).toBeInTheDocument()
    })

    it('renders exactly 4 feature cards', () => {
      render(<FeaturesSection />)

      const cards = screen.getAllByTestId('glassmorphism-card')
      expect(cards).toHaveLength(4)
    })

    it('renders icons for all 4 features', () => {
      render(<FeaturesSection />)

      expect(screen.getByTestId('feature-icon-1')).toBeInTheDocument()
      expect(screen.getByTestId('feature-icon-2')).toBeInTheDocument()
      expect(screen.getByTestId('feature-icon-3')).toBeInTheDocument()
      expect(screen.getByTestId('feature-icon-4')).toBeInTheDocument()
    })

    it('renders titles for all 4 features', () => {
      render(<FeaturesSection />)

      expect(screen.getByTestId('feature-title-1')).toBeInTheDocument()
      expect(screen.getByTestId('feature-title-2')).toBeInTheDocument()
      expect(screen.getByTestId('feature-title-3')).toBeInTheDocument()
      expect(screen.getByTestId('feature-title-4')).toBeInTheDocument()
    })

    it('renders descriptions for all 4 features', () => {
      render(<FeaturesSection />)

      expect(screen.getByTestId('feature-description-1')).toBeInTheDocument()
      expect(screen.getByTestId('feature-description-2')).toBeInTheDocument()
      expect(screen.getByTestId('feature-description-3')).toBeInTheDocument()
      expect(screen.getByTestId('feature-description-4')).toBeInTheDocument()
    })
  })

  describe('Test Case 2: Each feature card uses GlassMorphismCard component', () => {
    it('renders all feature cards with GlassMorphismCard wrapper', () => {
      render(<FeaturesSection />)

      const glassMorphismCards = screen.getAllByTestId('glassmorphism-card')
      expect(glassMorphismCards).toHaveLength(4)

      glassMorphismCards.forEach((card) => {
        expect(card).toHaveClass('backdrop-blur-md')
        expect(card).toHaveClass('rounded-2xl')
      })
    })
  })

  describe('Test Case 3: Verify feature titles match requirements', () => {
    it('displays "Instant URL Shortening" as the first feature', () => {
      render(<FeaturesSection />)

      expect(screen.getByText('Instant URL Shortening')).toBeInTheDocument()
    })

    it('displays "Detailed Analytics" as the second feature', () => {
      render(<FeaturesSection />)

      expect(screen.getByText('Detailed Analytics')).toBeInTheDocument()
    })

    it('displays "Easy Management" as the third feature', () => {
      render(<FeaturesSection />)

      expect(screen.getByText('Easy Management')).toBeInTheDocument()
    })

    it('displays "Secure Sharing" as the fourth feature', () => {
      render(<FeaturesSection />)

      expect(screen.getByText('Secure Sharing')).toBeInTheDocument()
    })

    it('displays all four required features', () => {
      render(<FeaturesSection />)

      const expectedTitles = [
        'Instant URL Shortening',
        'Detailed Analytics',
        'Easy Management',
        'Secure Sharing',
      ]

      expectedTitles.forEach((title) => {
        expect(screen.getByText(title)).toBeInTheDocument()
      })
    })
  })

  describe('Test Case 4: Grid layout structure', () => {
    it('renders features in a grid container', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      expect(grid).toBeInTheDocument()
      expect(grid).toHaveClass('grid')
    })

    it('has responsive grid classes for different viewport sizes', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      expect(grid).toHaveClass('grid-cols-1')
      expect(grid).toHaveClass('sm:grid-cols-2')
      expect(grid).toHaveClass('lg:grid-cols-4')
    })

    it('has proper gap between grid items', () => {
      render(<FeaturesSection />)

      const grid = screen.getByTestId('features-grid')
      expect(grid).toHaveClass('gap-6')
    })
  })

  describe('Section structure and accessibility', () => {
    it('renders with proper section element', () => {
      render(<FeaturesSection />)

      const section = screen.getByRole('region', { name: /powerful features/i })
      expect(section).toBeInTheDocument()
    })

    it('has section heading', () => {
      render(<FeaturesSection />)

      expect(screen.getByRole('heading', { level: 2 })).toHaveTextContent('Powerful Features')
    })

    it('has feature headings at h3 level', () => {
      render(<FeaturesSection />)

      const h3Headings = screen.getAllByRole('heading', { level: 3 })
      expect(h3Headings).toHaveLength(4)
    })

    it('has proper aria-labelledby attribute', () => {
      render(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      expect(section).toHaveAttribute('aria-labelledby', 'features-heading')
    })

    it('icons are marked as decorative with aria-hidden', () => {
      render(<FeaturesSection />)

      const iconContainers = [
        screen.getByTestId('feature-icon-1'),
        screen.getByTestId('feature-icon-2'),
        screen.getByTestId('feature-icon-3'),
        screen.getByTestId('feature-icon-4'),
      ]

      iconContainers.forEach((container) => {
        expect(container).toHaveAttribute('aria-hidden', 'true')
      })
    })
  })

  describe('Visual distinction', () => {
    it('section has proper padding for visual separation', () => {
      render(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      expect(section).toHaveClass('py-20')
      expect(section).toHaveClass('px-4')
    })

    it('section has background color class', () => {
      render(<FeaturesSection />)

      const section = screen.getByTestId('features-section')
      expect(section).toHaveClass('bg-base-100')
    })

    it('has max-width container for content', () => {
      render(<FeaturesSection />)

      const container = screen.getByTestId('features-section').querySelector('.max-w-7xl')
      expect(container).toBeInTheDocument()
    })
  })
})
