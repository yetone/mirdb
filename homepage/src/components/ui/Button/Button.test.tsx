/**
 * Button Component Tests
 * Owner: Scenario 1 - Hero Section & Value Proposition
 */
import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { Button } from './Button'

describe('Button', () => {
  describe('Rendering', () => {
    it('renders as a button element by default', () => {
      render(<Button>Click me</Button>)
      const button = screen.getByRole('button', { name: /click me/i })
      expect(button).toBeInTheDocument()
      expect(button.tagName).toBe('BUTTON')
    })

    it('renders as an anchor element when href is provided', () => {
      render(<Button href="https://example.com">Link text</Button>)
      const link = screen.getByRole('link', { name: /link text/i })
      expect(link).toBeInTheDocument()
      expect(link.tagName).toBe('A')
      expect(link).toHaveAttribute('href', 'https://example.com')
    })

    it('renders children correctly', () => {
      render(<Button>Test Content</Button>)
      expect(screen.getByText('Test Content')).toBeInTheDocument()
    })
  })

  describe('Primary Variant', () => {
    it('renders with primary styles by default', () => {
      render(<Button>Primary Button</Button>)
      const button = screen.getByRole('button')
      expect(button.className).toMatch(/primary/i)
    })

    it('renders with primary styles when variant is explicitly set', () => {
      render(<Button variant="primary">Primary Button</Button>)
      const button = screen.getByRole('button')
      expect(button.className).toMatch(/primary/i)
    })
  })

  describe('Secondary Variant', () => {
    it('renders with secondary styles when variant is secondary', () => {
      render(<Button variant="secondary">Secondary Button</Button>)
      const button = screen.getByRole('button')
      expect(button.className).toMatch(/secondary/i)
    })
  })

  describe('Accessibility', () => {
    it('applies aria-label when provided', () => {
      render(<Button aria-label="Custom label">Visible text</Button>)
      const button = screen.getByRole('button', { name: /custom label/i })
      expect(button).toHaveAttribute('aria-label', 'Custom label')
    })

    it('is focusable', () => {
      render(<Button>Focusable button</Button>)
      const button = screen.getByRole('button')
      button.focus()
      expect(button).toHaveFocus()
    })

    it('has type="button" to prevent form submission', () => {
      render(<Button>Button</Button>)
      const button = screen.getByRole('button')
      expect(button).toHaveAttribute('type', 'button')
    })
  })

  describe('Link Behavior', () => {
    it('sets target="_blank" when specified', () => {
      render(
        <Button href="https://github.com" target="_blank">
          GitHub
        </Button>
      )
      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('target', '_blank')
    })

    it('automatically adds rel="noopener noreferrer" for target="_blank"', () => {
      render(
        <Button href="https://github.com" target="_blank">
          GitHub
        </Button>
      )
      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('rel', 'noopener noreferrer')
    })

    it('uses custom rel when provided', () => {
      render(
        <Button href="https://example.com" rel="author">
          Author
        </Button>
      )
      const link = screen.getByRole('link')
      expect(link).toHaveAttribute('rel', 'author')
    })
  })

  describe('Click Handling', () => {
    it('calls onClick handler when clicked', () => {
      const handleClick = vi.fn()
      render(<Button onClick={handleClick}>Click me</Button>)
      fireEvent.click(screen.getByRole('button'))
      expect(handleClick).toHaveBeenCalledTimes(1)
    })
  })

  describe('Custom Styling', () => {
    it('applies custom className', () => {
      render(<Button className="custom-class">Styled Button</Button>)
      const button = screen.getByRole('button')
      expect(button.className).toContain('custom-class')
    })
  })
})
