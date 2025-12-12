import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, it, expect, vi } from 'vitest'
import { Hero, type HeroProps } from './Hero'

describe('Hero Component', () => {
  const defaultProps: HeroProps = {
    headline: 'Welcome to MirDB',
    subheadline: 'A high-performance key-value store',
    ctaText: 'Get Started',
    ctaHref: '/getting-started',
  }

  describe('Test Case 3: Hero component renders without errors with required props', () => {
    it('renders headline text correctly', () => {
      render(<Hero {...defaultProps} />)
      expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent('Welcome to MirDB')
    })

    it('renders subheadline text correctly', () => {
      render(<Hero {...defaultProps} />)
      expect(screen.getByText('A high-performance key-value store')).toBeInTheDocument()
    })

    it('renders CTA button with correct text', () => {
      render(<Hero {...defaultProps} />)
      const ctaButton = screen.getByRole('link', { name: 'Get Started' })
      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton).toHaveAttribute('href', '/getting-started')
    })

    it('renders hero section with correct structure', () => {
      render(<Hero {...defaultProps} />)
      const heroSection = screen.getByRole('banner')
      expect(heroSection).toBeInTheDocument()
      expect(heroSection).toHaveClass('hero')
    })

    it('CTA button is clickable', async () => {
      const user = userEvent.setup()
      const mockClick = vi.fn()
      render(<Hero {...defaultProps} onCtaClick={mockClick} />)

      const ctaButton = screen.getByRole('link', { name: 'Get Started' })
      await user.click(ctaButton)

      expect(mockClick).toHaveBeenCalledTimes(1)
    })
  })

  describe('Test Case 4: Component handles missing prop gracefully or shows default content', () => {
    it('renders with default headline when headline prop is not provided', () => {
      const propsWithoutHeadline: Partial<HeroProps> = {
        subheadline: 'A high-performance key-value store',
        ctaText: 'Get Started',
        ctaHref: '/getting-started',
      }
      render(<Hero {...propsWithoutHeadline as HeroProps} />)
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument()
    })

    it('renders with default subheadline when subheadline prop is not provided', () => {
      const propsWithoutSubheadline: Partial<HeroProps> = {
        headline: 'Welcome to MirDB',
        ctaText: 'Get Started',
        ctaHref: '/getting-started',
      }
      render(<Hero {...propsWithoutSubheadline as HeroProps} />)
      // Should render without throwing an error
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument()
    })

    it('renders with default CTA text when ctaText prop is not provided', () => {
      const propsWithoutCtaText: Partial<HeroProps> = {
        headline: 'Welcome to MirDB',
        subheadline: 'A high-performance key-value store',
        ctaHref: '/getting-started',
      }
      render(<Hero {...propsWithoutCtaText as HeroProps} />)
      expect(screen.getByRole('link')).toBeInTheDocument()
    })

    it('renders without error when all optional props are missing', () => {
      render(<Hero />)
      expect(screen.getByRole('banner')).toBeInTheDocument()
      expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument()
      expect(screen.getByRole('link')).toBeInTheDocument()
    })
  })
})
