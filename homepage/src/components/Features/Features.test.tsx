import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Features } from './Features'
import { FeatureCard } from './FeatureCard'
import { features } from '../../data/features'

describe('Features Component', () => {
  // Test Case 1: Features component renders without errors
  it('renders without errors', () => {
    expect(() => render(<Features />)).not.toThrow()
  })

  // Test Case 2: Exactly 6 feature cards are rendered
  it('renders exactly 6 feature cards', () => {
    render(<Features />)
    const featureCards = screen.getAllByTestId('feature-card')
    expect(featureCards).toHaveLength(6)
  })

  // Test Case 3: Memcached Protocol feature exists
  it('displays Memcached Protocol Support feature', () => {
    render(<Features />)
    expect(screen.getByText('Memcached Protocol Support')).toBeInTheDocument()
  })

  // Test Case 4: SSTables/Data Persistence feature exists
  it('displays Data Persistence via SSTables feature', () => {
    render(<Features />)
    expect(screen.getByText('Data Persistence via SSTables')).toBeInTheDocument()
  })

  // Test Case 5: LSM Tree feature exists
  it('displays LSM Tree Architecture feature', () => {
    render(<Features />)
    expect(screen.getByText('LSM Tree Architecture')).toBeInTheDocument()
  })

  // Test Case 6: Tokio/Async networking feature exists
  it('displays Async Networking with Tokio feature', () => {
    render(<Features />)
    expect(screen.getByText('Async Networking with Tokio')).toBeInTheDocument()
  })

  // Test Case 7: Skip List/Memtable feature exists
  it('displays Skip List Memtable feature', () => {
    render(<Features />)
    expect(screen.getByText('Skip List Memtable')).toBeInTheDocument()
  })

  // Test Case 8: Compaction feature exists
  it('displays Multi-level Compaction feature', () => {
    render(<Features />)
    expect(screen.getByText('Multi-level Compaction')).toBeInTheDocument()
  })

  it('renders the section with proper heading', () => {
    render(<Features />)
    expect(screen.getByRole('heading', { name: /key features/i })).toBeInTheDocument()
  })

  it('has accessible section landmark', () => {
    render(<Features />)
    const section = screen.getByRole('region', { name: /key features/i })
    expect(section).toBeInTheDocument()
  })
})

describe('FeatureCard Component', () => {
  const mockFeature = {
    title: 'Test Feature',
    description: 'Test description for the feature',
    icon: 'storage',
  }

  it('renders feature title', () => {
    render(<FeatureCard feature={mockFeature} />)
    expect(screen.getByText('Test Feature')).toBeInTheDocument()
  })

  it('renders feature description', () => {
    render(<FeatureCard feature={mockFeature} />)
    expect(screen.getByText('Test description for the feature')).toBeInTheDocument()
  })

  it('renders as an article element', () => {
    render(<FeatureCard feature={mockFeature} />)
    expect(screen.getByRole('article')).toBeInTheDocument()
  })

  it('has hover transition classes for visual feedback', () => {
    render(<FeatureCard feature={mockFeature} />)
    const card = screen.getByTestId('feature-card')
    expect(card.className).toContain('hover:')
    expect(card.className).toContain('transition')
  })
})

describe('Features Data', () => {
  it('contains exactly 6 features', () => {
    expect(features).toHaveLength(6)
  })

  it('each feature has required properties', () => {
    features.forEach((feature) => {
      expect(feature).toHaveProperty('title')
      expect(feature).toHaveProperty('description')
      expect(feature).toHaveProperty('icon')
      expect(typeof feature.title).toBe('string')
      expect(typeof feature.description).toBe('string')
      expect(feature.title.length).toBeGreaterThan(0)
      expect(feature.description.length).toBeGreaterThan(0)
    })
  })
})
