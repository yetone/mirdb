/**
 * Homepage Component Tests
 * Owner: All scenarios contribute test cases
 *
 * Test coverage for:
 * - Hero section rendering (Scenario 1)
 * - Features section rendering (Scenario 2)
 * - CTA navigation (Scenario 3)
 * - Responsive layouts (Scenarios 4-5)
 * - Theme support (Scenarios 6-8)
 * - Footer rendering (Scenario 9)
 * - Accessibility (Scenarios 10-11)
 * - Public access (Scenario 12)
 * - Component integration (Scenario 13)
 * - Animations (Scenario 14)
 * - Route integration (Scenario 15)
 */
import { describe, it, expect } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithProviders } from '../utils/renderWithProviders'
import Home from '../../src/pages/Home'

describe('Home Page - Hero Section Display (Scenario 1)', () => {
  it('should render h1 heading with product value proposition', () => {
    renderWithProviders(<Home />)

    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toBeInTheDocument()
    expect(heading.textContent).toContain('Shorten URLs')
    expect(heading.textContent).toContain('Track Clicks')
    expect(heading.textContent).toContain('Grow Your Reach')
  })

  it('should render primary CTA button with Get Started text', () => {
    renderWithProviders(<Home />)

    const getStartedButton = screen.getByRole('button', { name: /get started/i })
    expect(getStartedButton).toBeInTheDocument()
    expect(getStartedButton).toBeVisible()
  })

  it('should render secondary CTA button with Login text', () => {
    renderWithProviders(<Home />)

    const loginButton = screen.getByRole('button', { name: /login/i })
    expect(loginButton).toBeInTheDocument()
    expect(loginButton).toBeVisible()
  })

  it('should render BackgroundEffect component in the DOM', () => {
    renderWithProviders(<Home />)

    const backgroundEffect = screen.getByTestId('background-effect')
    expect(backgroundEffect).toBeInTheDocument()
  })

  it('should render subheadline text below the main headline', () => {
    renderWithProviders(<Home />)

    const subheadline = screen.getByTestId('subheadline')
    expect(subheadline).toBeInTheDocument()
    expect(subheadline).toBeVisible()
    expect(subheadline.textContent).toContain('Create short, memorable links')
    expect(subheadline.textContent).toContain('Track performance')
    expect(subheadline.textContent).toContain('dashboard')
  })

  it('should have Get Started button linked to /register', () => {
    renderWithProviders(<Home />)

    const getStartedLink = screen.getByRole('link', { name: /get started/i })
    expect(getStartedLink).toHaveAttribute('href', '/register')
  })

  it('should have Login button linked to /login', () => {
    renderWithProviders(<Home />)

    const loginLink = screen.getByRole('link', { name: /login/i })
    expect(loginLink).toHaveAttribute('href', '/login')
  })

  it('should display product name', () => {
    renderWithProviders(<Home />)

    expect(screen.getByText('URL Shortener')).toBeInTheDocument()
  })
})
