/**
 * Integration Test: HeroSection with BackgroundEffect
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests the integration between HeroSection and BackgroundEffect
 * as they appear on the homepage.
 */

import { describe, it, expect } from 'vitest'
import { screen } from '@testing-library/react'
import { renderWithProviders } from '../utils/test-utils'
import Home from '../../src/pages/Home'

describe('Homepage Hero with BackgroundEffect Integration', () => {
  // Test Case 5: BackgroundEffect component is rendered and visible
  it('renders BackgroundEffect component on the homepage', () => {
    renderWithProviders(<Home />)

    // BackgroundEffect should be present with its test id
    const backgroundEffect = screen.getByTestId('background-effect')
    expect(backgroundEffect).toBeInTheDocument()
  })

  it('renders both HeroSection and BackgroundEffect together', () => {
    const { container } = renderWithProviders(<Home />)

    // Check HeroSection is present
    const heroSection = container.querySelector('section[aria-label="Hero"]')
    expect(heroSection).toBeInTheDocument()

    // Check BackgroundEffect is present
    const backgroundEffect = screen.getByTestId('background-effect')
    expect(backgroundEffect).toBeInTheDocument()
  })

  it('BackgroundEffect does not interfere with hero content', () => {
    renderWithProviders(<Home />)

    // Verify hero content is still accessible with background present
    const headline = screen.getByRole('heading', { level: 1 })
    expect(headline).toBeInTheDocument()
    expect(headline.textContent?.toLowerCase()).toMatch(/shorten/i)

    const getStartedButton = screen.getByRole('button', { name: /get started/i })
    expect(getStartedButton).toBeInTheDocument()
  })
})
