import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Comparison, ComparisonProps } from './Comparison'

const defaultProps: ComparisonProps = {
  title: 'Why MirDB over Memcached?',
  subtitle: 'All the speed of memcached, with the durability you need',
  items: [
    {
      feature: 'Data Persistence',
      mirdb: 'Yes - data survives restarts',
      memcached: 'No - data lost on restart',
      advantage: true,
    },
    {
      feature: 'Protocol Compatibility',
      mirdb: 'Memcached protocol',
      memcached: 'Memcached protocol',
      advantage: false,
    },
  ],
  persistenceHighlight: 'Unlike memcached, your data survives restarts. MirDB persists data to disk using SSTables.',
  compatibilityMessage: 'Drop-in replacement for memcached. Existing clients work without modification.',
}

describe('Comparison Component', () => {
  it('renders the comparison section with correct class', () => {
    render(<Comparison {...defaultProps} />)
    const section = document.querySelector('.comparison')
    expect(section).toBeInTheDocument()
  })

  it('displays the title and subtitle', () => {
    render(<Comparison {...defaultProps} />)
    expect(screen.getByText('Why MirDB over Memcached?')).toBeInTheDocument()
    expect(screen.getByText('All the speed of memcached, with the durability you need')).toBeInTheDocument()
  })

  it('displays the persistence highlight message', () => {
    render(<Comparison {...defaultProps} />)
    expect(screen.getByText(/Unlike memcached, your data survives restarts/i)).toBeInTheDocument()
  })

  it('displays the compatibility message', () => {
    render(<Comparison {...defaultProps} />)
    expect(screen.getByText(/Drop-in replacement for memcached/i)).toBeInTheDocument()
    expect(screen.getByText(/Existing clients work without modification/i)).toBeInTheDocument()
  })

  it('renders comparison table with headers', () => {
    render(<Comparison {...defaultProps} />)
    expect(screen.getByText('Feature')).toBeInTheDocument()
    expect(screen.getByText('MirDB')).toBeInTheDocument()
    expect(screen.getByText('Memcached')).toBeInTheDocument()
  })

  it('renders comparison items in the table', () => {
    render(<Comparison {...defaultProps} />)
    expect(screen.getByText('Data Persistence')).toBeInTheDocument()
    expect(screen.getByText('Yes - data survives restarts')).toBeInTheDocument()
    expect(screen.getByText('No - data lost on restart')).toBeInTheDocument()
  })

  it('has accessible heading structure', () => {
    render(<Comparison {...defaultProps} />)
    const heading = screen.getByRole('heading', { level: 2 })
    expect(heading).toBeInTheDocument()
    expect(heading).toHaveTextContent('Why MirDB over Memcached?')
  })

  it('has correct id for navigation', () => {
    render(<Comparison {...defaultProps} />)
    const section = document.querySelector('#comparison')
    expect(section).toBeInTheDocument()
  })
})
