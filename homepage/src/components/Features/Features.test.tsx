import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Features } from './Features'

describe('Features', () => {
  it('displays Memcached protocol feature text', () => {
    render(<Features />)

    expect(screen.getByText('Memcached Protocol')).toBeInTheDocument()
    expect(screen.getByText(/memcached text protocol/i)).toBeInTheDocument()
  })

  it('displays persistence feature text', () => {
    render(<Features />)

    expect(screen.getByText('Data Persistence')).toBeInTheDocument()
    expect(screen.getByText(/persists data to disk/i)).toBeInTheDocument()
  })

  it('displays LSM tree architecture feature text', () => {
    render(<Features />)

    expect(screen.getByText('LSM Tree Architecture')).toBeInTheDocument()
    expect(screen.getByText(/Log-Structured Merge-tree/i)).toBeInTheDocument()
  })

  it('renders at least 3 feature items', () => {
    render(<Features />)

    const featureCards = screen.getAllByRole('article')
    expect(featureCards.length).toBeGreaterThanOrEqual(3)
  })

  it('renders the features section with proper test id', () => {
    render(<Features />)

    expect(screen.getByTestId('features-section')).toBeInTheDocument()
  })

  it('renders feature cards with proper test ids', () => {
    render(<Features />)

    expect(screen.getByTestId('feature-memcached')).toBeInTheDocument()
    expect(screen.getByTestId('feature-persistence')).toBeInTheDocument()
    expect(screen.getByTestId('feature-lsm')).toBeInTheDocument()
  })

  it('displays section title "Key Features"', () => {
    render(<Features />)

    expect(screen.getByText('Key Features')).toBeInTheDocument()
  })
})
