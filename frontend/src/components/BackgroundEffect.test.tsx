import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import BackgroundEffect from './BackgroundEffect'
import { ThemeProvider } from '../contexts/ThemeContext'

const renderWithTheme = (component: React.ReactNode) => {
  return render(
    <ThemeProvider>
      {component}
    </ThemeProvider>
  )
}

describe('BackgroundEffect', () => {
  it('renders without crashing', () => {
    renderWithTheme(<BackgroundEffect data-testid="bg-effect" />)
    expect(screen.getByTestId('bg-effect')).toBeInTheDocument()
  })

  it('has fixed positioning', () => {
    renderWithTheme(<BackgroundEffect data-testid="bg-effect" />)
    const element = screen.getByTestId('bg-effect')
    expect(element).toHaveClass('fixed', 'inset-0')
  })

  it('has negative z-index for background layering', () => {
    renderWithTheme(<BackgroundEffect data-testid="bg-effect" />)
    const element = screen.getByTestId('bg-effect')
    expect(element).toHaveClass('-z-10')
  })

  it('is not interactive (pointer-events-none)', () => {
    renderWithTheme(<BackgroundEffect data-testid="bg-effect" />)
    const element = screen.getByTestId('bg-effect')
    expect(element).toHaveClass('pointer-events-none')
  })

  it('is hidden from screen readers', () => {
    renderWithTheme(<BackgroundEffect data-testid="bg-effect" />)
    const element = screen.getByTestId('bg-effect')
    expect(element).toHaveAttribute('aria-hidden', 'true')
  })

  it('contains animated gradient orbs', () => {
    renderWithTheme(<BackgroundEffect data-testid="bg-effect" />)
    const element = screen.getByTestId('bg-effect')
    // Check for blur class indicating gradient orbs (using blur-2xl for performance)
    const children = element.querySelectorAll('.blur-2xl')
    expect(children.length).toBeGreaterThanOrEqual(3)
  })

  it('contains gradient background elements', () => {
    renderWithTheme(<BackgroundEffect data-testid="bg-effect" />)
    const element = screen.getByTestId('bg-effect')
    // Check for gradient orbs with rounded-full class
    const gradientOrbs = element.querySelectorAll('.rounded-full')
    expect(gradientOrbs.length).toBeGreaterThanOrEqual(3)
  })
})
