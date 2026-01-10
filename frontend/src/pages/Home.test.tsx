import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import Home from './Home'

describe('Home Page Integration', () => {
  // Test Case 2: Navigate to '/' route - Hero section is visible with gradient background
  it('displays hero section with gradient background when navigating to "/" route', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <Home />
      </MemoryRouter>
    )

    const heroSection = screen.getByTestId('hero-section')
    expect(heroSection).toBeInTheDocument()
    expect(heroSection).toHaveClass('bg-gradient-to-br')
  })

  it('hero section contains all required elements', () => {
    render(
      <MemoryRouter initialEntries={['/']}>
        <Home />
      </MemoryRouter>
    )

    // Headline
    const headline = screen.getByRole('heading', { level: 1 })
    expect(headline).toBeInTheDocument()

    // Subheadline
    const subheadline = screen.getByTestId('hero-subheadline')
    expect(subheadline).toBeInTheDocument()

    // CTAs
    const primaryCTA = screen.getByRole('link', { name: /get started/i })
    expect(primaryCTA).toBeInTheDocument()

    const loginCTA = screen.getByRole('link', { name: /log in/i })
    expect(loginCTA).toBeInTheDocument()
  })
})
