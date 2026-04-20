/**
 * Unit tests for Home page component.
 * Owner: Scenario 1 - Hero Section Value Proposition Display
 *
 * Test coverage:
 * - HeroSection is rendered
 * - Page has proper document structure
 */

import { describe, it, expect } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { BrowserRouter } from 'react-router-dom'
import Home from '../../src/pages/Home'

const renderHome = () => {
  return render(
    <BrowserRouter>
      <Home />
    </BrowserRouter>
  )
}

describe('Home Page', () => {
  it('should render the HeroSection component', () => {
    renderHome()

    // Check for hero section
    const heroSection = screen.getByLabelText(/hero section/i)
    expect(heroSection).toBeInTheDocument()
  })

  it('should render headline from HeroSection', () => {
    renderHome()

    const headline = screen.getByRole('heading', { level: 1 })
    expect(headline).toBeInTheDocument()
    expect(headline).toHaveTextContent(/shorten links/i)
  })

  it('should render subheadline with URL shortening and analytics info', () => {
    renderHome()

    // Scope to hero section to avoid matching feature cards
    const heroSection = screen.getByLabelText(/hero section/i)
    expect(within(heroSection).getByText(/analytics/i)).toBeInTheDocument()
  })

  it('should have a main element as the root', () => {
    renderHome()

    const main = screen.getByRole('main')
    expect(main).toBeInTheDocument()
  })
})
