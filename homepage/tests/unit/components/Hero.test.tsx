import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Hero } from '@/components/sections/Hero'

describe('Hero Component', () => {
  // Test Case 1: Component renders with product name 'MirDB' visible
  it('renders with product name MirDB visible', () => {
    render(<Hero />)

    const productName = screen.getByRole('heading', { level: 1 })
    expect(productName).toHaveTextContent('MirDB')
  })

  // Test Case 2: Tagline mentions 'persistent key-value store' or similar value proposition
  it('displays tagline mentioning persistent key-value store', () => {
    render(<Hero />)

    const taglineElement = screen.getByText(/persistent key-value store/i)
    expect(taglineElement).toBeInTheDocument()
  })

  // Test Case 3: 'Get Started' CTA button is present and has correct href
  it('renders Get Started CTA button with correct href', () => {
    render(<Hero />)

    const ctaButton = screen.getByRole('link', { name: /get started/i })
    expect(ctaButton).toBeInTheDocument()
    expect(ctaButton).toHaveAttribute('href', '#quick-start')
  })

  // Test Case 5: Logo image loads successfully without errors
  it('renders logo image successfully', () => {
    render(<Hero />)

    const logo = screen.getByRole('img', { name: /mirdb logo/i })
    expect(logo).toBeInTheDocument()
    expect(logo).toHaveAttribute('src', '/logo.gif')
  })

  it('displays value proposition text', () => {
    render(<Hero />)

    const valueProposition = screen.getByText(/high-performance|lsm tree|rust/i)
    expect(valueProposition).toBeInTheDocument()
  })

  it('has proper accessibility attributes', () => {
    render(<Hero />)

    const heroSection = screen.getByRole('region', { name: /hero/i })
    expect(heroSection).toBeInTheDocument()
  })
})
