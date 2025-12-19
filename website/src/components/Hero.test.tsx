import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Hero, HeroProps } from './Hero'

describe('Hero Component', () => {
  const defaultProps: HeroProps = {
    productName: 'MirDB',
    tagline: 'Memcached, but persistent.',
    description: 'A persistent key-value store with memcached protocol compatibility.',
    ctaButtons: [
      { label: 'Get Started', href: '#quickstart', primary: true },
      { label: 'View on GitHub', href: 'https://github.com/example/mirdb', primary: false },
    ],
  }

  it('renders the product name prominently', () => {
    render(<Hero {...defaultProps} />)

    const productName = screen.getByRole('heading', { level: 1 })
    expect(productName).toHaveTextContent('MirDB')
    expect(productName).toBeInTheDocument()
  })

  it('renders the tagline', () => {
    render(<Hero {...defaultProps} />)

    expect(screen.getByText('Memcached, but persistent.')).toBeInTheDocument()
  })

  it('renders the value proposition description mentioning persistent key-value store and memcached', () => {
    render(<Hero {...defaultProps} />)

    const description = screen.getByText(/persistent key-value store/i)
    expect(description).toBeInTheDocument()
    expect(description).toHaveTextContent(/memcached/i)
  })

  it('renders CTA buttons with correct labels', () => {
    render(<Hero {...defaultProps} />)

    const getStartedButton = screen.getByRole('link', { name: /get started/i })
    const githubButton = screen.getByRole('link', { name: /view on github/i })

    expect(getStartedButton).toBeInTheDocument()
    expect(githubButton).toBeInTheDocument()
  })

  it('renders CTA buttons with correct href attributes', () => {
    render(<Hero {...defaultProps} />)

    const getStartedButton = screen.getByRole('link', { name: /get started/i })
    const githubButton = screen.getByRole('link', { name: /view on github/i })

    expect(getStartedButton).toHaveAttribute('href', '#quickstart')
    expect(githubButton).toHaveAttribute('href', 'https://github.com/example/mirdb')
  })

  it('applies primary styling to primary CTA button', () => {
    render(<Hero {...defaultProps} />)

    const getStartedButton = screen.getByRole('link', { name: /get started/i })
    expect(getStartedButton).toHaveClass('cta-primary')
  })

  it('applies secondary styling to non-primary CTA button', () => {
    render(<Hero {...defaultProps} />)

    const githubButton = screen.getByRole('link', { name: /view on github/i })
    expect(githubButton).toHaveClass('cta-secondary')
  })

  it('renders with required props for name, tagline, and CTAs', () => {
    const { container } = render(<Hero {...defaultProps} />)

    // Verify the hero section exists
    const heroSection = container.querySelector('.hero')
    expect(heroSection).toBeInTheDocument()

    // Verify all required elements are present
    expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent('MirDB')
    expect(screen.getByText('Memcached, but persistent.')).toBeInTheDocument()
    expect(screen.getAllByRole('link').length).toBeGreaterThanOrEqual(1)
  })

  it('renders at least one CTA button', () => {
    const minimalProps: HeroProps = {
      productName: 'MirDB',
      tagline: 'Memcached, but persistent.',
      description: 'A persistent key-value store with memcached compatibility.',
      ctaButtons: [
        { label: 'Get Started', href: '#quickstart', primary: true },
      ],
    }

    render(<Hero {...minimalProps} />)

    const ctaButtons = screen.getAllByRole('link')
    expect(ctaButtons.length).toBeGreaterThanOrEqual(1)
  })
})
