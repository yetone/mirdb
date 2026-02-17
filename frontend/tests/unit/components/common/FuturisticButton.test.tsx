/**
 * FuturisticButton component tests.
 * Scenario 13 - Component Integration
 *
 * Tests:
 * - Component renders with futuristic styling
 * - CTA buttons in HeroSection use FuturisticButton with hover states
 * - Multiple variants (primary, secondary, outline) work correctly
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import { FuturisticButton } from '../../../../src/components/common/FuturisticButton'
import { HeroSection } from '../../../../src/components/home/HeroSection'
import { ThemeProvider } from '../../../../src/contexts/ThemeContext'

// Helper to wrap components with necessary providers
function renderWithProviders(ui: React.ReactElement) {
  return render(
    <BrowserRouter>
      <ThemeProvider>{ui}</ThemeProvider>
    </BrowserRouter>
  )
}

describe('FuturisticButton', () => {
  it('renders children correctly', () => {
    render(<FuturisticButton>Click Me</FuturisticButton>)

    expect(screen.getByRole('button', { name: 'Click Me' })).toBeInTheDocument()
  })

  it('applies primary variant styling by default', () => {
    render(<FuturisticButton>Primary</FuturisticButton>)

    const button = screen.getByRole('button', { name: 'Primary' })
    expect(button).toHaveClass('bg-primary')
    expect(button).toHaveClass('text-primary-content')
  })

  it('applies secondary variant styling', () => {
    render(<FuturisticButton variant="secondary">Secondary</FuturisticButton>)

    const button = screen.getByRole('button', { name: 'Secondary' })
    expect(button).toHaveClass('bg-secondary')
    expect(button).toHaveClass('text-secondary-content')
  })

  it('applies outline variant styling', () => {
    render(<FuturisticButton variant="outline">Outline</FuturisticButton>)

    const button = screen.getByRole('button', { name: 'Outline' })
    expect(button).toHaveClass('border-primary')
    expect(button).toHaveClass('text-primary')
  })

  it('has hover state transformation classes', () => {
    render(<FuturisticButton>Hover Test</FuturisticButton>)

    const button = screen.getByRole('button', { name: 'Hover Test' })
    expect(button).toHaveClass('hover:scale-105')
    expect(button).toHaveClass('transition-all')
    expect(button).toHaveClass('duration-300')
  })

  it('handles click events', () => {
    const handleClick = vi.fn()
    render(<FuturisticButton onClick={handleClick}>Click</FuturisticButton>)

    const button = screen.getByRole('button', { name: 'Click' })
    fireEvent.click(button)

    expect(handleClick).toHaveBeenCalledTimes(1)
  })

  it('shows loading state', () => {
    render(<FuturisticButton loading>Submit</FuturisticButton>)

    expect(screen.getByText('Loading...')).toBeInTheDocument()
    expect(screen.getByRole('button')).toBeDisabled()
  })

  it('handles disabled state', () => {
    render(<FuturisticButton disabled>Disabled</FuturisticButton>)

    const button = screen.getByRole('button', { name: 'Disabled' })
    expect(button).toBeDisabled()
    expect(button).toHaveClass('disabled:opacity-50')
  })

  it('has keyboard accessibility with focus ring', () => {
    render(<FuturisticButton>Accessible</FuturisticButton>)

    const button = screen.getByRole('button', { name: 'Accessible' })
    expect(button).toHaveClass('focus:outline-none')
    expect(button).toHaveClass('focus:ring-2')
  })
})

describe('FuturisticButton Integration - HeroSection', () => {
  it('CTA buttons use FuturisticButton component with hover states', () => {
    const handleRegister = vi.fn()
    const handleLogin = vi.fn()

    renderWithProviders(
      <HeroSection onRegisterClick={handleRegister} onLoginClick={handleLogin} />
    )

    // Find the CTA buttons
    const primaryCta = screen.getByTestId('hero-primary-cta')
    const loginCta = screen.getByTestId('hero-login-cta')

    // Verify buttons exist
    expect(primaryCta).toBeInTheDocument()
    expect(loginCta).toBeInTheDocument()

    // Verify primary CTA has FuturisticButton styling
    expect(primaryCta).toHaveClass('bg-primary')
    expect(primaryCta).toHaveClass('hover:scale-105')
    expect(primaryCta).toHaveClass('transition-all')

    // Verify login CTA has outline variant styling
    expect(loginCta).toHaveClass('border-primary')
    expect(loginCta).toHaveClass('hover:scale-105')
    expect(loginCta).toHaveClass('transition-all')
  })

  it('CTA buttons are clickable and trigger navigation callbacks', () => {
    const handleRegister = vi.fn()
    const handleLogin = vi.fn()

    renderWithProviders(
      <HeroSection onRegisterClick={handleRegister} onLoginClick={handleLogin} />
    )

    const primaryCta = screen.getByTestId('hero-primary-cta')
    const loginCta = screen.getByTestId('hero-login-cta')

    fireEvent.click(primaryCta)
    expect(handleRegister).toHaveBeenCalledTimes(1)

    fireEvent.click(loginCta)
    expect(handleLogin).toHaveBeenCalledTimes(1)
  })

  it('CTA buttons have proper accessibility labels', () => {
    renderWithProviders(<HeroSection />)

    const primaryCta = screen.getByTestId('hero-primary-cta')
    const loginCta = screen.getByTestId('hero-login-cta')

    expect(primaryCta).toHaveAttribute('aria-label')
    expect(loginCta).toHaveAttribute('aria-label')
  })
})
