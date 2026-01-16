import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import FuturisticButton from './FuturisticButton'

const renderWithRouter = (component: React.ReactNode) => {
  return render(
    <BrowserRouter>
      {component}
    </BrowserRouter>
  )
}

describe('FuturisticButton', () => {
  describe('as button', () => {
    it('renders as a button by default', () => {
      renderWithRouter(<FuturisticButton data-testid="btn">Click me</FuturisticButton>)
      const button = screen.getByTestId('btn')
      expect(button.tagName).toBe('BUTTON')
    })

    it('renders with children content', () => {
      renderWithRouter(<FuturisticButton>Click me</FuturisticButton>)
      expect(screen.getByText('Click me')).toBeInTheDocument()
    })

    it('handles click events', () => {
      const handleClick = vi.fn()
      renderWithRouter(
        <FuturisticButton onClick={handleClick} data-testid="btn">
          Click me
        </FuturisticButton>
      )
      fireEvent.click(screen.getByTestId('btn'))
      expect(handleClick).toHaveBeenCalledTimes(1)
    })

    it('can be disabled', () => {
      renderWithRouter(
        <FuturisticButton disabled data-testid="btn">
          Click me
        </FuturisticButton>
      )
      expect(screen.getByTestId('btn')).toBeDisabled()
    })
  })

  describe('as link', () => {
    it('renders as a link when as="link"', () => {
      renderWithRouter(
        <FuturisticButton as="link" to="/test" data-testid="btn">
          Click me
        </FuturisticButton>
      )
      const link = screen.getByTestId('btn')
      expect(link.tagName).toBe('A')
      expect(link).toHaveAttribute('href', '/test')
    })
  })

  describe('variants', () => {
    it('applies primary variant styles by default', () => {
      renderWithRouter(<FuturisticButton data-testid="btn">Primary</FuturisticButton>)
      const button = screen.getByTestId('btn')
      expect(button).toHaveClass('bg-primary')
    })

    it('applies secondary variant styles', () => {
      renderWithRouter(
        <FuturisticButton variant="secondary" data-testid="btn">
          Secondary
        </FuturisticButton>
      )
      const button = screen.getByTestId('btn')
      expect(button).toHaveClass('bg-secondary')
    })

    it('applies outline variant styles', () => {
      renderWithRouter(
        <FuturisticButton variant="outline" data-testid="btn">
          Outline
        </FuturisticButton>
      )
      const button = screen.getByTestId('btn')
      expect(button).toHaveClass('border-primary')
    })

    it('applies ghost variant styles', () => {
      renderWithRouter(
        <FuturisticButton variant="ghost" data-testid="btn">
          Ghost
        </FuturisticButton>
      )
      const button = screen.getByTestId('btn')
      expect(button).toHaveClass('bg-transparent')
    })
  })

  describe('sizes', () => {
    it('applies medium size by default', () => {
      renderWithRouter(<FuturisticButton data-testid="btn">Medium</FuturisticButton>)
      const button = screen.getByTestId('btn')
      expect(button).toHaveClass('px-6', 'py-3')
    })

    it('applies small size', () => {
      renderWithRouter(
        <FuturisticButton size="sm" data-testid="btn">
          Small
        </FuturisticButton>
      )
      const button = screen.getByTestId('btn')
      expect(button).toHaveClass('px-4', 'py-2')
    })

    it('applies large size', () => {
      renderWithRouter(
        <FuturisticButton size="lg" data-testid="btn">
          Large
        </FuturisticButton>
      )
      const button = screen.getByTestId('btn')
      expect(button).toHaveClass('px-8', 'py-4')
    })
  })

  describe('accessibility', () => {
    it('has focus ring for keyboard navigation', () => {
      renderWithRouter(<FuturisticButton data-testid="btn">Focus</FuturisticButton>)
      const button = screen.getByTestId('btn')
      expect(button).toHaveClass('focus:ring-2')
    })
  })
})
