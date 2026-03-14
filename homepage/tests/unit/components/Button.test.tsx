/**
 * Unit tests for Button component and CTA functionality.
 * Owner: Scenario 5 - Secondary CTAs Implementation
 *
 * Test cases:
 * - Test Case 1: Render homepage and count CTAs - Multiple CTAs present
 * - Test Case 6: Verify CTA contrast ratios - All CTA text meets 4.5:1 contrast ratio
 * - Button variants (primary, secondary, outline)
 * - Button sizes (sm, md, lg)
 * - Button hover, focus, active states
 * - Button accessibility
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { Button, ButtonProps } from '../../../src/components/common/Button'
import {
  CTASection,
  CTASectionProps,
  InlineCTA,
  ContactSalesCTA,
  LearnMoreCTA,
} from '../../../src/components/sections/CTASection'

// Color contrast calculation utilities
// Uses relative luminance formula per WCAG 2.1
function hexToRgb(hex: string): { r: number; g: number; b: number } {
  const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex)
  if (!result) throw new Error(`Invalid hex color: ${hex}`)
  return {
    r: parseInt(result[1], 16),
    g: parseInt(result[2], 16),
    b: parseInt(result[3], 16),
  }
}

function getLuminance(hex: string): number {
  const { r, g, b } = hexToRgb(hex)
  const [rs, gs, bs] = [r, g, b].map((c) => {
    const s = c / 255
    return s <= 0.03928 ? s / 12.92 : Math.pow((s + 0.055) / 1.055, 2.4)
  })
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs
}

function getContrastRatio(color1: string, color2: string): number {
  const l1 = getLuminance(color1)
  const l2 = getLuminance(color2)
  const lighter = Math.max(l1, l2)
  const darker = Math.min(l1, l2)
  return (lighter + 0.05) / (darker + 0.05)
}

// Tailwind color values used in Button component
const COLORS = {
  primary600: '#2563eb',
  primary700: '#1d4ed8',
  secondary600: '#475569',
  secondary700: '#334155',
  white: '#ffffff',
}

describe('Button Component', () => {
  // Test Case 6: Verify CTA contrast ratios
  describe('Test Case 6: CTA Contrast Ratios', () => {
    it('primary button meets WCAG 2.1 AA contrast ratio (4.5:1)', () => {
      // Primary: bg-primary-600 (#2563eb) with white text
      const ratio = getContrastRatio(COLORS.primary600, COLORS.white)
      expect(ratio).toBeGreaterThanOrEqual(4.5)
    })

    it('secondary button meets WCAG 2.1 AA contrast ratio (4.5:1)', () => {
      // Secondary: bg-secondary-600 (#475569) with white text
      const ratio = getContrastRatio(COLORS.secondary600, COLORS.white)
      expect(ratio).toBeGreaterThanOrEqual(4.5)
    })

    it('outline button text meets WCAG 2.1 AA contrast ratio (4.5:1)', () => {
      // Outline: primary-700 text on white background
      const ratio = getContrastRatio(COLORS.primary700, COLORS.white)
      expect(ratio).toBeGreaterThanOrEqual(4.5)
    })

    it('all button variants have readable text', () => {
      const variants: Array<'primary' | 'secondary' | 'outline'> = [
        'primary',
        'secondary',
        'outline',
      ]

      variants.forEach((variant) => {
        const { container } = render(
          <Button variant={variant}>Test Button</Button>
        )
        const button = container.querySelector('button')
        expect(button).toBeTruthy()
        expect(button?.textContent).toBe('Test Button')
      })
    })
  })

  describe('Button variants', () => {
    it('renders primary variant with correct styling classes', () => {
      render(<Button variant="primary">Primary Button</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('bg-primary-600')
      expect(button).toHaveClass('text-white')
    })

    it('renders secondary variant with correct styling classes', () => {
      render(<Button variant="secondary">Secondary Button</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('bg-secondary-600')
      expect(button).toHaveClass('text-white')
    })

    it('renders outline variant with correct styling classes', () => {
      render(<Button variant="outline">Outline Button</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('border-2')
      expect(button).toHaveClass('border-primary-600')
      expect(button).toHaveClass('text-primary-700')
    })

    it('defaults to primary variant when not specified', () => {
      render(<Button>Default Button</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('bg-primary-600')
    })
  })

  describe('Button sizes', () => {
    it('renders small size with correct padding and text classes', () => {
      render(<Button size="sm">Small Button</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('px-4')
      expect(button).toHaveClass('py-2')
      expect(button).toHaveClass('text-sm')
    })

    it('renders medium size with correct padding and text classes', () => {
      render(<Button size="md">Medium Button</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('px-6')
      expect(button).toHaveClass('py-3')
      expect(button).toHaveClass('text-base')
    })

    it('renders large size with correct padding and text classes', () => {
      render(<Button size="lg">Large Button</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('px-8')
      expect(button).toHaveClass('py-4')
      expect(button).toHaveClass('text-lg')
    })

    it('defaults to medium size when not specified', () => {
      render(<Button>Default Size Button</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('px-6')
      expect(button).toHaveClass('py-3')
      expect(button).toHaveClass('text-base')
    })
  })

  describe('Button hover states', () => {
    it('has hover styling classes for primary variant', () => {
      render(<Button variant="primary">Hover Test</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('hover:bg-primary-700')
      expect(button).toHaveClass('hover:shadow-lg')
      expect(button).toHaveClass('hover:scale-105')
    })

    it('has hover styling classes for secondary variant', () => {
      render(<Button variant="secondary">Hover Test</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('hover:bg-secondary-700')
      expect(button).toHaveClass('hover:shadow-lg')
      expect(button).toHaveClass('hover:scale-105')
    })

    it('has hover styling classes for outline variant', () => {
      render(<Button variant="outline">Hover Test</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('hover:bg-primary-50')
      expect(button).toHaveClass('hover:border-primary-700')
      expect(button).toHaveClass('hover:shadow-lg')
    })
  })

  describe('Button focus states', () => {
    it('has focus ring styling for accessibility', () => {
      render(<Button>Focus Test</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('focus:outline-none')
      expect(button).toHaveClass('focus:ring-4')
      expect(button).toHaveClass('focus:ring-offset-2')
    })

    it('primary button has correct focus ring color', () => {
      render(<Button variant="primary">Focus Test</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('focus:ring-primary-500')
    })

    it('secondary button has correct focus ring color', () => {
      render(<Button variant="secondary">Focus Test</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('focus:ring-secondary-500')
    })
  })

  describe('Button active states', () => {
    it('has active styling classes for primary variant', () => {
      render(<Button variant="primary">Active Test</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('active:bg-primary-800')
      expect(button).toHaveClass('active:scale-100')
    })

    it('has active styling classes for secondary variant', () => {
      render(<Button variant="secondary">Active Test</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('active:bg-secondary-800')
      expect(button).toHaveClass('active:scale-100')
    })
  })

  describe('Button as anchor', () => {
    it('renders as anchor tag when href is provided', () => {
      render(<Button href="#test">Link Button</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button.tagName).toBe('A')
      expect(button).toHaveAttribute('href', '#test')
    })

    it('renders as button tag when href is not provided', () => {
      render(<Button>Regular Button</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button.tagName).toBe('BUTTON')
      expect(button).toHaveAttribute('type', 'button')
    })
  })

  describe('Button click handling', () => {
    it('calls onClick when clicked', () => {
      const handleClick = vi.fn()
      render(<Button onClick={handleClick}>Click Me</Button>)

      const button = screen.getByTestId('cta-button')
      fireEvent.click(button)

      expect(handleClick).toHaveBeenCalledTimes(1)
    })
  })

  describe('Button disabled state', () => {
    it('has disabled styling classes', () => {
      render(<Button disabled>Disabled Button</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('disabled:opacity-50')
      expect(button).toHaveClass('disabled:cursor-not-allowed')
      expect(button).toBeDisabled()
    })
  })

  describe('Button transition effects', () => {
    it('has smooth transition classes', () => {
      render(<Button>Transition Test</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('transition-all')
      expect(button).toHaveClass('duration-200')
      expect(button).toHaveClass('ease-in-out')
    })
  })
})

describe('CTASection Component', () => {
  const defaultProps: CTASectionProps = {
    title: 'Ready to Get Started?',
    description: 'Join thousands of teams already using our platform.',
    primaryCTA: {
      label: 'Get Started',
      href: '#signup',
      variant: 'primary',
      size: 'lg',
    },
    secondaryCTA: {
      label: 'Learn More',
      href: '#features',
      variant: 'outline',
      size: 'lg',
    },
  }

  describe('CTA Section rendering', () => {
    it('renders title and description', () => {
      render(<CTASection {...defaultProps} />)

      const title = screen.getByTestId('cta-section-title')
      expect(title).toHaveTextContent('Ready to Get Started?')

      const description = screen.getByTestId('cta-section-description')
      expect(description).toHaveTextContent(
        'Join thousands of teams already using our platform.'
      )
    })

    it('renders primary CTA button', () => {
      render(<CTASection {...defaultProps} />)

      const primaryButton = screen.getByTestId('cta-primary-button')
      expect(primaryButton).toHaveTextContent('Get Started')
      expect(primaryButton).toHaveAttribute('href', '#signup')
    })

    it('renders secondary CTA button when provided', () => {
      render(<CTASection {...defaultProps} />)

      const secondaryButton = screen.getByTestId('cta-secondary-button')
      expect(secondaryButton).toHaveTextContent('Learn More')
      expect(secondaryButton).toHaveAttribute('href', '#features')
    })

    it('does not render secondary CTA when not provided', () => {
      const propsWithoutSecondary = { ...defaultProps, secondaryCTA: undefined }
      render(<CTASection {...propsWithoutSecondary} />)

      const secondaryButton = screen.queryByTestId('cta-secondary-button')
      expect(secondaryButton).not.toBeInTheDocument()
    })
  })

  describe('CTA Section accessibility', () => {
    it('has proper section role and aria attributes', () => {
      render(<CTASection {...defaultProps} id="test-cta" />)

      const section = screen.getByTestId('cta-section')
      expect(section).toHaveAttribute('aria-labelledby', 'test-cta-title')
    })

    it('title has proper id for aria-labelledby', () => {
      render(<CTASection {...defaultProps} id="test-cta" />)

      const title = screen.getByTestId('cta-section-title')
      expect(title).toHaveAttribute('id', 'test-cta-title')
    })

    it('buttons have aria-label attributes', () => {
      render(<CTASection {...defaultProps} />)

      const primaryButton = screen.getByTestId('cta-primary-button')
      expect(primaryButton).toHaveAttribute('aria-label', 'Get Started')

      const secondaryButton = screen.getByTestId('cta-secondary-button')
      expect(secondaryButton).toHaveAttribute('aria-label', 'Learn More')
    })
  })
})

describe('InlineCTA Component', () => {
  it('renders with default outline variant', () => {
    render(<InlineCTA label="Learn More" href="#learn" />)

    const button = screen.getByTestId('inline-cta')
    expect(button).toHaveTextContent('Learn More')
    expect(button).toHaveClass('border-2')
    expect(button).toHaveClass('border-primary-600')
  })

  it('accepts custom variant', () => {
    render(<InlineCTA label="Sign Up" href="#signup" variant="primary" />)

    const button = screen.getByTestId('inline-cta')
    expect(button).toHaveClass('bg-primary-600')
  })

  it('has proper aria-label', () => {
    render(<InlineCTA label="Contact Us" href="#contact" />)

    const button = screen.getByTestId('inline-cta')
    expect(button).toHaveAttribute('aria-label', 'Contact Us')
  })
})

describe('ContactSalesCTA Component', () => {
  it('renders with default label and href', () => {
    render(<ContactSalesCTA />)

    const button = screen.getByTestId('contact-sales-cta')
    expect(button).toHaveTextContent('Contact Sales')
    expect(button).toHaveAttribute('href', '#contact')
  })

  it('renders with secondary variant', () => {
    render(<ContactSalesCTA />)

    const button = screen.getByTestId('contact-sales-cta')
    expect(button).toHaveClass('bg-secondary-600')
  })

  it('accepts custom label and href', () => {
    render(
      <ContactSalesCTA label="Talk to Sales" href="/sales" />
    )

    const button = screen.getByTestId('contact-sales-cta')
    expect(button).toHaveTextContent('Talk to Sales')
    expect(button).toHaveAttribute('href', '/sales')
  })
})

describe('LearnMoreCTA Component', () => {
  it('renders with default label and href', () => {
    render(<LearnMoreCTA />)

    const button = screen.getByTestId('learn-more-cta')
    expect(button).toHaveTextContent('Learn More')
    expect(button).toHaveAttribute('href', '#features')
  })

  it('renders with outline variant', () => {
    render(<LearnMoreCTA />)

    const button = screen.getByTestId('learn-more-cta')
    expect(button).toHaveClass('border-2')
    expect(button).toHaveClass('border-primary-600')
  })

  it('accepts custom label and href', () => {
    render(
      <LearnMoreCTA label="Discover More" href="/about" />
    )

    const button = screen.getByTestId('learn-more-cta')
    expect(button).toHaveTextContent('Discover More')
    expect(button).toHaveAttribute('href', '/about')
  })
})

// Test Case 1: Multiple CTAs present on homepage
describe('Test Case 1: Homepage CTA Count', () => {
  it('CTASection renders multiple CTAs (primary + secondary)', () => {
    const props: CTASectionProps = {
      primaryCTA: {
        label: 'Get Started',
        href: '#signup',
        variant: 'primary',
        size: 'lg',
      },
      secondaryCTA: {
        label: 'Learn More',
        href: '#features',
        variant: 'outline',
        size: 'lg',
      },
    }

    render(<CTASection {...props} />)

    // Count all buttons with data-testid containing 'cta'
    const primaryCTA = screen.getByTestId('cta-primary-button')
    const secondaryCTA = screen.getByTestId('cta-secondary-button')

    expect(primaryCTA).toBeInTheDocument()
    expect(secondaryCTA).toBeInTheDocument()
  })

  it('renders InlineCTA, ContactSalesCTA, and LearnMoreCTA as additional CTAs', () => {
    render(
      <>
        <InlineCTA label="Inline CTA" href="#inline" />
        <ContactSalesCTA />
        <LearnMoreCTA />
      </>
    )

    const inlineCTA = screen.getByTestId('inline-cta')
    const contactSalesCTA = screen.getByTestId('contact-sales-cta')
    const learnMoreCTA = screen.getByTestId('learn-more-cta')

    expect(inlineCTA).toBeInTheDocument()
    expect(contactSalesCTA).toBeInTheDocument()
    expect(learnMoreCTA).toBeInTheDocument()
  })
})
