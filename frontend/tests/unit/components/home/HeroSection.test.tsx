/**
 * HeroSection Component Tests
 * Owner: Scenario 2 - Hero Section Display
 *
 * Unit tests for the HeroSection component.
 * Tests cover:
 * - Product name heading (h1) is present and visible
 * - Tagline text describing URL shortening value proposition is displayed
 * - URL input field with placeholder 'Paste your long URL here...' exists
 * - Primary action button labeled 'Shorten' is present
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { HeroSection } from '../../../../src/components/home/HeroSection'

describe('HeroSection', () => {
  // Test Case 1: Product name heading (h1) is present and visible
  it('displays the product name as a prominent h1 heading', () => {
    render(<HeroSection />)

    const heading = screen.getByRole('heading', { level: 1 })
    expect(heading).toBeInTheDocument()
    expect(heading).toHaveTextContent('URL Shortener')
    expect(heading).toBeVisible()
  })

  // Test Case 2: Tagline text describing URL shortening value proposition is displayed
  it('displays a tagline describing the URL shortening value proposition', () => {
    render(<HeroSection />)

    const tagline = screen.getByText(/Transform your long URLs into short, shareable links/i)
    expect(tagline).toBeInTheDocument()
    expect(tagline).toBeVisible()

    // Also check for analytics mention in the value proposition
    expect(screen.getByText(/Track clicks and analyze performance/i)).toBeInTheDocument()
  })

  // Test Case 3: URL input field with placeholder 'Paste your long URL here...' exists
  it('renders URL input field with correct placeholder', () => {
    render(<HeroSection />)

    const urlInput = screen.getByPlaceholderText('Paste your long URL here...')
    expect(urlInput).toBeInTheDocument()
    expect(urlInput).toBeVisible()
    expect(urlInput).toHaveAttribute('type', 'url')
  })

  // Test Case 4: Primary action button labeled 'Shorten' is present
  it('displays a Shorten button as the primary action', () => {
    render(<HeroSection />)

    const shortenButton = screen.getByRole('button', { name: /shorten/i })
    expect(shortenButton).toBeInTheDocument()
    expect(shortenButton).toBeVisible()
    expect(shortenButton).toHaveTextContent('Shorten')
  })

  // Additional test: Hero section has proper accessibility
  it('has proper accessibility attributes', () => {
    render(<HeroSection />)

    const heroSection = screen.getByRole('region', { name: /hero section/i })
    expect(heroSection).toBeInTheDocument()

    const urlInput = screen.getByLabelText(/URL to shorten/i)
    expect(urlInput).toBeInTheDocument()
  })

  // Additional test: Input field is interactive
  it('allows typing in the URL input field', async () => {
    const user = userEvent.setup()
    render(<HeroSection />)

    const urlInput = screen.getByPlaceholderText('Paste your long URL here...')
    await user.type(urlInput, 'https://example.com/long-url')

    expect(urlInput).toHaveValue('https://example.com/long-url')
  })
})
