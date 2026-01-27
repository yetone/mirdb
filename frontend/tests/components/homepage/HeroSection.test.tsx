import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { MemoryRouter, useNavigate } from 'react-router-dom'
import { HeroSection } from '../../../src/components/homepage/HeroSection'

// Mock react-router-dom's useNavigate
vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual('react-router-dom')
  return {
    ...actual,
    useNavigate: vi.fn(),
  }
})

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, whileHover, whileTap, initial, animate, transition, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <div {...props}>{children}</div>
    ),
    button: ({ children, whileHover, whileTap, initial, animate, transition, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <button {...props}>{children}</button>
    ),
  },
}))

const mockNavigate = vi.fn()

const renderHeroSection = (props = {}) => {
  return render(
    <MemoryRouter>
      <HeroSection {...props} />
    </MemoryRouter>
  )
}

describe('HeroSection', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    ;(useNavigate as ReturnType<typeof vi.fn>).mockReturnValue(mockNavigate)
  })

  // Test Case 1: Component renders without errors
  describe('Rendering', () => {
    it('should render the component without errors', () => {
      renderHeroSection()
      expect(screen.getByTestId('hero-section')).toBeInTheDocument()
    })

    it('should render the hero section with correct structure', () => {
      renderHeroSection()
      const section = screen.getByTestId('hero-section')
      expect(section.tagName).toBe('SECTION')
      expect(section).toHaveAttribute('aria-labelledby', 'hero-heading')
    })
  })

  // Test Case 2: Check for h1 element with headline text
  describe('Headline', () => {
    it('should render h1 element with headline text about URL shortening', () => {
      renderHeroSection()
      const headline = screen.getByTestId('hero-headline')
      expect(headline).toBeInTheDocument()
      expect(headline.tagName).toBe('H1')
      expect(headline.textContent).toContain('Shorten URLs')
      expect(headline.textContent).toContain('Track')
      expect(headline.textContent).toContain('Click')
    })

    it('should have proper heading id for accessibility', () => {
      renderHeroSection()
      const headline = screen.getByTestId('hero-headline')
      expect(headline).toHaveAttribute('id', 'hero-heading')
    })
  })

  // Test Case 3: Check for subheadline/description element
  describe('Subheadline', () => {
    it('should render a subheadline explaining the service value', () => {
      renderHeroSection()
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline).toBeInTheDocument()
      expect(subheadline.textContent).toContain('short')
      expect(subheadline.textContent).toContain('analytics')
    })

    it('should provide meaningful description of service', () => {
      renderHeroSection()
      const subheadline = screen.getByTestId('hero-subheadline')
      expect(subheadline.textContent).toMatch(/links|url/i)
      expect(subheadline.textContent).toMatch(/track|analytics|clicks/i)
    })
  })

  // Test Case 4: Check for primary CTA button
  describe('Primary CTA Button', () => {
    it('should render a primary CTA button with default text', () => {
      renderHeroSection()
      const ctaButton = screen.getByTestId('hero-cta-primary')
      expect(ctaButton).toBeInTheDocument()
      expect(ctaButton.textContent).toBe('Get Started')
    })

    it('should render a primary CTA button with custom text', () => {
      renderHeroSection({ ctaText: 'Sign Up' })
      const ctaButton = screen.getByTestId('hero-cta-primary')
      expect(ctaButton.textContent).toBe('Sign Up')
    })

    it('should have proper button styling classes', () => {
      renderHeroSection()
      const ctaButton = screen.getByTestId('hero-cta-primary')
      expect(ctaButton).toHaveClass('btn')
      expect(ctaButton).toHaveClass('btn-primary')
    })

    it('should be a button element', () => {
      renderHeroSection()
      const ctaButton = screen.getByTestId('hero-cta-primary')
      expect(ctaButton.tagName).toBe('BUTTON')
    })
  })

  // Test Case 5: Click primary CTA button - Navigation to /register
  describe('CTA Navigation', () => {
    it('should navigate to /register when primary CTA is clicked', () => {
      renderHeroSection()
      const ctaButton = screen.getByTestId('hero-cta-primary')
      fireEvent.click(ctaButton)
      expect(mockNavigate).toHaveBeenCalledWith('/register')
    })

    it('should call custom onCtaClick handler when provided', () => {
      const mockOnCtaClick = vi.fn()
      renderHeroSection({ onCtaClick: mockOnCtaClick })
      const ctaButton = screen.getByTestId('hero-cta-primary')
      fireEvent.click(ctaButton)
      expect(mockOnCtaClick).toHaveBeenCalledTimes(1)
      expect(mockNavigate).not.toHaveBeenCalled()
    })
  })

  // Test Case 6: Accessibility
  describe('Accessibility', () => {
    it('should have correct heading hierarchy with h1', () => {
      renderHeroSection()
      const h1Elements = screen.getAllByRole('heading', { level: 1 })
      expect(h1Elements).toHaveLength(1)
    })

    it('should have accessible section with aria-labelledby', () => {
      renderHeroSection()
      const section = screen.getByTestId('hero-section')
      expect(section).toHaveAttribute('aria-labelledby', 'hero-heading')
    })

    it('should have focusable CTA buttons', () => {
      renderHeroSection()
      const primaryCta = screen.getByTestId('hero-cta-primary')
      const secondaryCta = screen.getByTestId('hero-cta-secondary')

      // Buttons should be focusable (not disabled)
      expect(primaryCta).not.toBeDisabled()
      expect(secondaryCta).not.toBeDisabled()

      // Buttons should have accessible names
      expect(primaryCta).toHaveAccessibleName('Get Started')
      expect(secondaryCta).toHaveAccessibleName('Learn More')
    })

    it('should have sufficient color contrast classes', () => {
      renderHeroSection()
      const subheadline = screen.getByTestId('hero-subheadline')
      // Using DaisyUI's base-content with opacity for contrast
      expect(subheadline).toHaveClass('text-base-content/80')
    })
  })

  // Secondary CTA button tests
  describe('Secondary CTA Button', () => {
    it('should render a secondary CTA button with default text', () => {
      renderHeroSection()
      const secondaryCta = screen.getByTestId('hero-cta-secondary')
      expect(secondaryCta).toBeInTheDocument()
      expect(secondaryCta.textContent).toBe('Learn More')
    })

    it('should render secondary CTA with custom text', () => {
      renderHeroSection({ secondaryCtaText: 'Explore Features' })
      const secondaryCta = screen.getByTestId('hero-cta-secondary')
      expect(secondaryCta.textContent).toBe('Explore Features')
    })

    it('should have outline button styling', () => {
      renderHeroSection()
      const secondaryCta = screen.getByTestId('hero-cta-secondary')
      expect(secondaryCta).toHaveClass('btn-outline')
    })

    it('should call custom onSecondaryCtaClick handler when provided', () => {
      const mockOnSecondaryClick = vi.fn()
      renderHeroSection({ onSecondaryCtaClick: mockOnSecondaryClick })
      const secondaryCta = screen.getByTestId('hero-cta-secondary')
      fireEvent.click(secondaryCta)
      expect(mockOnSecondaryClick).toHaveBeenCalledTimes(1)
    })
  })
})
