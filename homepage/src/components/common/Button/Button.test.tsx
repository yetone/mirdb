/**
 * Button Component Tests
 * Owner: Scenario 5 - Navigation and Links
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { Button } from './Button'

describe('Button', () => {
  describe('Rendering', () => {
    it('renders as a button element by default', () => {
      render(<Button>Click me</Button>)
      expect(screen.getByRole('button')).toBeInTheDocument()
      expect(screen.getByText('Click me')).toBeInTheDocument()
    })

    it('renders as an anchor element when href is provided', () => {
      render(<Button href="https://example.com">Link</Button>)
      expect(screen.getByRole('link')).toBeInTheDocument()
      expect(screen.getByText('Link')).toBeInTheDocument()
    })

    it('renders with primary variant by default', () => {
      render(<Button>Primary</Button>)
      const button = screen.getByRole('button')
      expect(button).toHaveClass('btn--primary')
    })

    it('renders with secondary variant when specified', () => {
      render(<Button variant="secondary">Secondary</Button>)
      const button = screen.getByRole('button')
      expect(button).toHaveClass('btn--secondary')
    })

    it('renders with custom className', () => {
      render(<Button className="custom-class">Custom</Button>)
      const button = screen.getByRole('button')
      expect(button).toHaveClass('btn', 'btn--primary', 'custom-class')
    })
  })

  describe('Anchor variant', () => {
    it('sets href attribute correctly', () => {
      render(<Button href="https://example.com">Link</Button>)
      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('href', 'https://example.com')
    })

    it('sets target attribute correctly', () => {
      render(
        <Button href="https://example.com" target="_blank">
          External Link
        </Button>
      )
      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('target', '_blank')
    })

    it('auto-sets rel="noopener noreferrer" for target="_blank"', () => {
      render(
        <Button href="https://example.com" target="_blank">
          External Link
        </Button>
      )
      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('rel', 'noopener noreferrer')
    })

    it('allows custom rel attribute', () => {
      render(
        <Button href="https://example.com" rel="custom-rel">
          Link
        </Button>
      )
      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('rel', 'custom-rel')
    })

    it('does not set rel when target is not _blank', () => {
      render(
        <Button href="https://example.com" target="_self">
          Link
        </Button>
      )
      const link = screen.getByRole('link')
      expect(link).not.toHaveAttribute('rel')
    })
  })

  describe('Interactivity', () => {
    it('calls onClick handler when clicked', () => {
      const handleClick = vi.fn()
      render(<Button onClick={handleClick}>Click me</Button>)

      fireEvent.click(screen.getByRole('button'))
      expect(handleClick).toHaveBeenCalledTimes(1)
    })

    it('calls onClick handler on anchor variant', () => {
      const handleClick = vi.fn()
      render(
        <Button href="https://example.com" onClick={handleClick}>
          Click me
        </Button>
      )

      fireEvent.click(screen.getByRole('link'))
      expect(handleClick).toHaveBeenCalledTimes(1)
    })

    it('does not call onClick when disabled', () => {
      const handleClick = vi.fn()
      render(
        <Button onClick={handleClick} disabled>
          Disabled
        </Button>
      )

      fireEvent.click(screen.getByRole('button'))
      expect(handleClick).not.toHaveBeenCalled()
    })
  })

  describe('Disabled state', () => {
    it('applies disabled attribute on button', () => {
      render(<Button disabled>Disabled</Button>)
      expect(screen.getByRole('button')).toBeDisabled()
    })

    it('applies aria-disabled on anchor variant', () => {
      render(
        <Button href="https://example.com" disabled>
          Disabled Link
        </Button>
      )
      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('aria-disabled', 'true')
    })

    it('sets tabIndex=-1 on disabled anchor', () => {
      render(
        <Button href="https://example.com" disabled>
          Disabled Link
        </Button>
      )
      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('tabIndex', '-1')
    })
  })

  describe('Accessibility', () => {
    it('applies aria-label when provided', () => {
      render(<Button aria-label="Submit form">Submit</Button>)
      expect(screen.getByRole('button')).toHaveAttribute(
        'aria-label',
        'Submit form'
      )
    })

    it('applies aria-label on anchor variant', () => {
      render(
        <Button href="https://github.com" aria-label="Go to GitHub">
          GitHub
        </Button>
      )
      expect(screen.getByRole('link')).toHaveAttribute(
        'aria-label',
        'Go to GitHub'
      )
    })

    it('has correct type attribute by default', () => {
      render(<Button>Button</Button>)
      expect(screen.getByRole('button')).toHaveAttribute('type', 'button')
    })

    it('accepts custom type attribute', () => {
      render(<Button type="submit">Submit</Button>)
      expect(screen.getByRole('button')).toHaveAttribute('type', 'submit')
    })
  })

  describe('Variants', () => {
    it('renders correctly in primary variant', () => {
      render(<Button variant="primary">Primary Button</Button>)
      const button = screen.getByRole('button')
      expect(button).toHaveClass('btn')
      expect(button).toHaveClass('btn--primary')
      expect(button).not.toHaveClass('btn--secondary')
    })

    it('renders correctly in secondary variant', () => {
      render(<Button variant="secondary">Secondary Button</Button>)
      const button = screen.getByRole('button')
      expect(button).toHaveClass('btn')
      expect(button).toHaveClass('btn--secondary')
      expect(button).not.toHaveClass('btn--primary')
    })
  })
})
