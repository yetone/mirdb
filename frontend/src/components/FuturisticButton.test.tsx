import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import FuturisticButton from './FuturisticButton'

const renderButton = (props: Parameters<typeof FuturisticButton>[0]) => {
  return render(
    <BrowserRouter>
      <FuturisticButton {...props} />
    </BrowserRouter>
  )
}

describe('FuturisticButton', () => {
  describe('Rendering', () => {
    it('renders as a button by default', () => {
      renderButton({ children: 'Click me' })
      const button = screen.getByRole('button', { name: /click me/i })
      expect(button).toBeInTheDocument()
    })

    it('renders as a link when as="link" is specified', () => {
      renderButton({ children: 'Go to page', as: 'link', to: '/test' })
      const link = screen.getByRole('link', { name: /go to page/i })
      expect(link).toBeInTheDocument()
      expect(link).toHaveAttribute('href', '/test')
    })

    it('renders children correctly', () => {
      renderButton({ children: 'Test Button' })
      expect(screen.getByText('Test Button')).toBeInTheDocument()
    })

    it('applies custom className', () => {
      renderButton({ children: 'Custom', className: 'custom-class' })
      const button = screen.getByRole('button')
      expect(button).toHaveClass('custom-class')
    })

    it('supports data-testid attribute', () => {
      renderButton({ children: 'Test', 'data-testid': 'futuristic-btn' })
      expect(screen.getByTestId('futuristic-btn')).toBeInTheDocument()
    })
  })

  describe('Variants', () => {
    it('applies primary variant styles by default', () => {
      renderButton({ children: 'Primary' })
      const button = screen.getByRole('button')
      expect(button.className).toContain('bg-gradient-to-r')
      expect(button.className).toContain('from-primary')
      expect(button.className).toContain('to-secondary')
    })

    it('applies secondary variant styles', () => {
      renderButton({ children: 'Secondary', variant: 'secondary' })
      const button = screen.getByRole('button')
      expect(button.className).toContain('bg-base-100/80')
    })

    it('applies ghost variant styles', () => {
      renderButton({ children: 'Ghost', variant: 'ghost' })
      const button = screen.getByRole('button')
      expect(button.className).toContain('bg-transparent')
    })
  })

  describe('Sizes', () => {
    it('applies medium size by default', () => {
      renderButton({ children: 'Medium' })
      const button = screen.getByRole('button')
      expect(button.className).toContain('min-h-[44px]')
    })

    it('applies small size correctly', () => {
      renderButton({ children: 'Small', size: 'sm' })
      const button = screen.getByRole('button')
      expect(button.className).toContain('btn-sm')
      expect(button.className).toContain('min-h-[36px]')
    })

    it('applies large size correctly', () => {
      renderButton({ children: 'Large', size: 'lg' })
      const button = screen.getByRole('button')
      expect(button.className).toContain('btn-lg')
      expect(button.className).toContain('min-h-[52px]')
    })
  })

  describe('Styling Features', () => {
    it('has futuristic hover effects (scale transform)', () => {
      renderButton({ children: 'Hover Me' })
      const button = screen.getByRole('button')
      expect(button.className).toContain('hover:scale-105')
    })

    it('has shadow effects', () => {
      renderButton({ children: 'Shadow' })
      const button = screen.getByRole('button')
      expect(button.className).toContain('shadow')
    })

    it('has transition for smooth animations', () => {
      renderButton({ children: 'Animated' })
      const button = screen.getByRole('button')
      expect(button.className).toContain('transition-all')
      expect(button.className).toContain('duration-300')
    })

    it('respects reduced motion preferences', () => {
      renderButton({ children: 'Accessible' })
      const button = screen.getByRole('button')
      expect(button.className).toContain('motion-reduce:transition-none')
      expect(button.className).toContain('motion-reduce:hover:scale-100')
    })

    it('has rounded corners for modern look', () => {
      renderButton({ children: 'Rounded' })
      const button = screen.getByRole('button')
      expect(button.className).toContain('rounded-xl')
    })

    it('has backdrop blur effect', () => {
      renderButton({ children: 'Blur' })
      const button = screen.getByRole('button')
      expect(button.className).toContain('backdrop-blur-sm')
    })
  })

  describe('Accessibility', () => {
    it('has focus ring styles for keyboard navigation', () => {
      renderButton({ children: 'Focus' })
      const button = screen.getByRole('button')
      expect(button.className).toContain('focus-visible:ring-2')
      expect(button.className).toContain('focus-visible:ring-primary')
    })

    it('can be disabled', () => {
      renderButton({ children: 'Disabled', disabled: true })
      const button = screen.getByRole('button')
      expect(button).toBeDisabled()
      expect(button.className).toContain('opacity-50')
      expect(button.className).toContain('cursor-not-allowed')
    })

    it('has proper button type attribute', () => {
      renderButton({ children: 'Submit', type: 'submit' })
      const button = screen.getByRole('button')
      expect(button).toHaveAttribute('type', 'submit')
    })

    it('defaults to type="button"', () => {
      renderButton({ children: 'Default' })
      const button = screen.getByRole('button')
      expect(button).toHaveAttribute('type', 'button')
    })
  })

  describe('Interactions', () => {
    it('calls onClick handler when clicked', () => {
      const handleClick = vi.fn()
      renderButton({ children: 'Clickable', onClick: handleClick })
      const button = screen.getByRole('button')
      fireEvent.click(button)
      expect(handleClick).toHaveBeenCalledTimes(1)
    })

    it('does not call onClick when disabled', () => {
      const handleClick = vi.fn()
      renderButton({ children: 'Disabled', onClick: handleClick, disabled: true })
      const button = screen.getByRole('button')
      fireEvent.click(button)
      expect(handleClick).not.toHaveBeenCalled()
    })
  })

  describe('Link Mode', () => {
    it('navigates to correct route when as="link"', () => {
      renderButton({ children: 'Register', as: 'link', to: '/register' })
      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('href', '/register')
    })

    it('applies same styling classes as button in link mode', () => {
      renderButton({ children: 'Link', as: 'link', to: '/test' })
      const link = screen.getByRole('link')
      expect(link.className).toContain('btn')
      expect(link.className).toContain('rounded-xl')
      expect(link.className).toContain('transition-all')
    })

    it('applies primary variant styles in link mode', () => {
      renderButton({ children: 'Primary Link', as: 'link', to: '/test', variant: 'primary' })
      const link = screen.getByRole('link')
      expect(link.className).toContain('bg-gradient-to-r')
      expect(link.className).toContain('from-primary')
    })

    it('supports data-testid in link mode', () => {
      renderButton({
        children: 'Test Link',
        as: 'link',
        to: '/test',
        'data-testid': 'test-link-btn'
      })
      expect(screen.getByTestId('test-link-btn')).toBeInTheDocument()
    })
  })
})
