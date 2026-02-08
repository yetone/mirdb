/**
 * Hero Component Tests
 * Owner: Scenario 1 - Hero Section Display
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Hero } from './Hero'
import { PRODUCT_NAME, TAGLINE, GITHUB_URL } from '../../utils/constants'

describe('Hero', () => {
  it('renders without crashing', () => {
    render(<Hero />)
    expect(screen.getByRole('region', { name: /hero/i })).toBeInTheDocument()
  })

  it('displays the product name MirDB', () => {
    render(<Hero />)
    expect(screen.getByRole('heading', { name: PRODUCT_NAME })).toBeInTheDocument()
  })

  it('displays the tagline with Persistent Key-Value Store and Memcached', () => {
    render(<Hero />)
    const tagline = screen.getByText(TAGLINE)
    expect(tagline).toBeInTheDocument()
    expect(tagline.textContent).toContain('Persistent Key-Value Store')
    expect(tagline.textContent).toContain('Memcached')
  })

  it('displays the logo.gif image', () => {
    render(<Hero />)
    const logo = screen.getByRole('img', { name: /logo/i })
    expect(logo).toBeInTheDocument()
    expect(logo.getAttribute('src')).toContain('logo.gif')
  })

  it('has a primary CTA button linking to GitHub', () => {
    render(<Hero />)
    const ctaLink = screen.getByRole('link', { name: /github/i })
    expect(ctaLink).toBeInTheDocument()
    expect(ctaLink.getAttribute('href')).toBe(GITHUB_URL)
  })

  it('CTA opens in a new tab with proper security attributes', () => {
    render(<Hero />)
    const ctaLink = screen.getByRole('link', { name: /github/i })
    expect(ctaLink.getAttribute('target')).toBe('_blank')
    expect(ctaLink.getAttribute('rel')).toContain('noopener')
    expect(ctaLink.getAttribute('rel')).toContain('noreferrer')
  })

  it('renders all required hero elements', () => {
    render(<Hero />)

    // Product name
    expect(screen.getByRole('heading', { level: 1 })).toHaveTextContent(PRODUCT_NAME)

    // Tagline
    expect(screen.getByText(TAGLINE)).toBeInTheDocument()

    // Logo
    expect(screen.getByRole('img')).toBeInTheDocument()

    // CTA
    expect(screen.getByRole('link', { name: /github/i })).toBeInTheDocument()
  })
})
