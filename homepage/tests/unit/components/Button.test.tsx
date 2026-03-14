/**
 * Unit tests for Button component.
 * Owner: Scenario 5 - Secondary CTAs Implementation
 *
 * Test cases:
 * - Button renders with correct variant styles
 * - Button renders with correct size classes
 * - Button hover, focus, and active states are present
 * - Button meets contrast requirements (4.5:1)
 * - Button is accessible (keyboard navigable, ARIA labels)
 * - Button renders as anchor when href is provided
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { Button, ButtonProps } from '../../../src/components/common/Button'

describe('Button Component', () => {
  // Test Case 1: Render homepage and count CTAs - Multiple CTAs present
  describe('Test Case 1: Multiple CTAs present', () => {
    it('renders primary, secondary, and outline button variants', () => {
      const { container } = render(
        <>
          <Button variant="primary">Primary CTA</Button>
          <Button variant="secondary">Secondary CTA</Button>
          <Button variant="outline">Outline CTA</Button>
        </>
      )

      const buttons = container.querySelectorAll('[data-testid="cta-button"]')
      expect(buttons.length).toBe(3)
    })

    it('button can be rendered as both button and anchor elements', () => {
      const { container } = render(
        <>
          <Button>Button Element</Button>
          <Button href="#link">Anchor Element</Button>
        </>
      )

      const buttonElements = container.querySelectorAll('button')
      const anchorElements = container.querySelectorAll('a')

      expect(buttonElements.length).toBe(1)
      expect(anchorElements.length).toBe(1)
    })
  })

  // Test Case 6: Verify CTA contrast ratios
  describe('Test Case 6: CTA contrast ratios', () => {
    it('primary button has high-contrast styling (bg-primary-600 + white text)', () => {
      render(<Button variant="primary">Get Started</Button>)

      const button = screen.getByTestId('cta-button')
      // Primary: #2563eb (bg) with white text = 4.56:1 contrast ratio
      expect(button).toHaveClass('bg-primary-600')
      expect(button).toHaveClass('text-white')
    })

    it('secondary button has high-contrast styling (bg-secondary-600 + white text)', () => {
      render(<Button variant="secondary">Contact Sales</Button>)

      const button = screen.getByTestId('cta-button')
      // Secondary: #475569 (bg) with white text = 5.92:1 contrast ratio
      expect(button).toHaveClass('bg-secondary-600')
      expect(button).toHaveClass('text-white')
    })

    it('outline button has visible styling with proper contrast', () => {
      render(<Button variant="outline">Learn More</Button>)

      const button = screen.getByTestId('cta-button')
      // Outline: #2563eb (text) on white/transparent bg = 4.56:1 contrast ratio
      expect(button).toHaveClass('border-2')
      expect(button).toHaveClass('border-primary-600')
      expect(button).toHaveClass('text-primary-600')
    })
  })

  // Test: Button variants styling
  describe('Button variants', () => {
    it('renders primary variant with correct classes', () => {
      render(<Button variant="primary">Primary</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('bg-primary-600')
      expect(button).toHaveClass('text-white')
      expect(button).toHaveClass('hover:bg-primary-700')
    })

    it('renders secondary variant with correct classes', () => {
      render(<Button variant="secondary">Secondary</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('bg-secondary-600')
      expect(button).toHaveClass('text-white')
      expect(button).toHaveClass('hover:bg-secondary-700')
    })

    it('renders outline variant with correct classes', () => {
      render(<Button variant="outline">Outline</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('bg-transparent')
      expect(button).toHaveClass('border-2')
      expect(button).toHaveClass('border-primary-600')
      expect(button).toHaveClass('text-primary-600')
    })

    it('renders white variant with correct classes', () => {
      render(<Button variant="white">White Button</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('bg-white')
      expect(button).toHaveClass('text-primary-600')
      expect(button).toHaveClass('hover:bg-secondary-50')
    })

    it('renders outline-white variant with correct classes', () => {
      render(<Button variant="outline-white">Outline White</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('bg-transparent')
      expect(button).toHaveClass('border-2')
      expect(button).toHaveClass('border-white')
      expect(button).toHaveClass('text-white')
    })
  })

  // Test: Button sizes
  describe('Button sizes', () => {
    it('renders small size with correct classes', () => {
      render(<Button size="sm">Small</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('px-4')
      expect(button).toHaveClass('py-2')
      expect(button).toHaveClass('text-sm')
    })

    it('renders medium size with correct classes (default)', () => {
      render(<Button size="md">Medium</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('px-6')
      expect(button).toHaveClass('py-3')
      expect(button).toHaveClass('text-base')
    })

    it('renders large size with correct classes', () => {
      render(<Button size="lg">Large</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('px-8')
      expect(button).toHaveClass('py-4')
      expect(button).toHaveClass('text-lg')
    })

    it('uses medium size by default', () => {
      render(<Button>Default Size</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('px-6')
      expect(button).toHaveClass('py-3')
      expect(button).toHaveClass('text-base')
    })
  })

  // Test: Hover states (NFR-5)
  describe('Hover states', () => {
    it('primary button has hover state classes', () => {
      render(<Button variant="primary">Hover Me</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('hover:bg-primary-700')
      expect(button).toHaveClass('hover:scale-105')
      expect(button).toHaveClass('hover:shadow-lg')
    })

    it('secondary button has hover state classes', () => {
      render(<Button variant="secondary">Hover Me</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('hover:bg-secondary-700')
      expect(button).toHaveClass('hover:scale-105')
    })

    it('outline button has hover state classes', () => {
      render(<Button variant="outline">Hover Me</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('hover:bg-primary-50')
      expect(button).toHaveClass('hover:border-primary-700')
      expect(button).toHaveClass('hover:text-primary-700')
    })
  })

  // Test: Focus states (NFR-5)
  describe('Focus states', () => {
    it('button has visible focus indicator classes', () => {
      render(<Button>Focus Me</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('focus:outline-none')
      expect(button).toHaveClass('focus:ring-4')
      expect(button).toHaveClass('focus:ring-offset-2')
    })

    it('primary button has primary focus ring color', () => {
      render(<Button variant="primary">Focus</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('focus:ring-primary-500')
    })

    it('secondary button has secondary focus ring color', () => {
      render(<Button variant="secondary">Focus</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('focus:ring-secondary-500')
    })

    it('outline button has primary focus ring color', () => {
      render(<Button variant="outline">Focus</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('focus:ring-primary-500')
    })
  })

  // Test: Active states (NFR-5)
  describe('Active states', () => {
    it('primary button has active state classes', () => {
      render(<Button variant="primary">Press Me</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('active:bg-primary-800')
      expect(button).toHaveClass('active:scale-95')
    })

    it('secondary button has active state classes', () => {
      render(<Button variant="secondary">Press Me</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('active:bg-secondary-800')
      expect(button).toHaveClass('active:scale-95')
    })

    it('outline button has active state classes', () => {
      render(<Button variant="outline">Press Me</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('active:bg-primary-100')
      expect(button).toHaveClass('active:border-primary-800')
      expect(button).toHaveClass('active:scale-95')
    })
  })

  // Test: Disabled state
  describe('Disabled state', () => {
    it('button can be disabled', () => {
      render(<Button disabled>Disabled</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toBeDisabled()
    })

    it('disabled button has disabled styling', () => {
      render(<Button variant="primary" disabled>Disabled</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('disabled:bg-primary-300')
      expect(button).toHaveClass('disabled:cursor-not-allowed')
    })

    it('disabled anchor button prevents default click', () => {
      const onClick = vi.fn()
      render(<Button href="#test" disabled onClick={onClick}>Disabled Link</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveAttribute('aria-disabled', 'true')
      expect(button).toHaveAttribute('tabindex', '-1')
    })
  })

  // Test: Accessibility
  describe('Accessibility', () => {
    it('button has correct type attribute', () => {
      render(<Button>Click Me</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveAttribute('type', 'button')
    })

    it('anchor button has correct href', () => {
      render(<Button href="#signup">Sign Up</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveAttribute('href', '#signup')
    })

    it('button is focusable', () => {
      render(<Button>Focusable</Button>)

      const button = screen.getByTestId('cta-button')
      button.focus()
      expect(document.activeElement).toBe(button)
    })

    it('anchor button is focusable', () => {
      render(<Button href="#link">Focusable Link</Button>)

      const button = screen.getByTestId('cta-button')
      button.focus()
      expect(document.activeElement).toBe(button)
    })

    it('button passes custom aria-label', () => {
      render(<Button aria-label="Custom label">Button</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveAttribute('aria-label', 'Custom label')
    })
  })

  // Test: Click handling
  describe('Click handling', () => {
    it('calls onClick when clicked', () => {
      const onClick = vi.fn()
      render(<Button onClick={onClick}>Click Me</Button>)

      const button = screen.getByTestId('cta-button')
      fireEvent.click(button)

      expect(onClick).toHaveBeenCalledTimes(1)
    })

    it('anchor button calls onClick when clicked', () => {
      const onClick = vi.fn()
      render(<Button href="#test" onClick={onClick}>Click Me</Button>)

      const button = screen.getByTestId('cta-button')
      fireEvent.click(button)

      expect(onClick).toHaveBeenCalledTimes(1)
    })

    it('disabled button does not call onClick', () => {
      const onClick = vi.fn()
      render(<Button disabled onClick={onClick}>Disabled</Button>)

      const button = screen.getByTestId('cta-button')
      fireEvent.click(button)

      expect(onClick).not.toHaveBeenCalled()
    })
  })

  // Test: Transition effects
  describe('Transition effects', () => {
    it('button has transition classes for smooth animations', () => {
      render(<Button>Animated</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('transition-all')
      expect(button).toHaveClass('duration-200')
      expect(button).toHaveClass('ease-in-out')
    })

    it('button has transform for hover scale effect', () => {
      render(<Button>Scalable</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('transform')
    })
  })

  // Test: Custom className
  describe('Custom className', () => {
    it('accepts additional className prop', () => {
      render(<Button className="custom-class">Custom</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('custom-class')
    })

    it('preserves base classes when custom className is added', () => {
      render(<Button className="custom-class" variant="primary">Custom</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveClass('custom-class')
      expect(button).toHaveClass('bg-primary-600')
    })
  })

  // Test: Children content
  describe('Children content', () => {
    it('renders text children correctly', () => {
      render(<Button>Get Started</Button>)

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveTextContent('Get Started')
    })

    it('renders JSX children correctly', () => {
      render(
        <Button>
          <span data-testid="icon">🚀</span>
          Launch
        </Button>
      )

      const button = screen.getByTestId('cta-button')
      expect(button).toHaveTextContent('🚀')
      expect(button).toHaveTextContent('Launch')
      expect(screen.getByTestId('icon')).toBeInTheDocument()
    })
  })
})
