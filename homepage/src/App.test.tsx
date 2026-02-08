import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import App from './App'

describe('App', () => {
  it('renders without crashing', () => {
    render(<App />)
    expect(screen.getByRole('main')).toBeInTheDocument()
  })

  it('contains the Header component', () => {
    render(<App />)
    expect(screen.getByRole('banner')).toBeInTheDocument()
  })

  it('contains the Hero section', () => {
    render(<App />)
    expect(screen.getByRole('region', { name: /hero/i })).toBeInTheDocument()
  })
})
