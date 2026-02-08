/**
 * Usage Component Tests
 * Owner: Scenario 4 - Usage Demonstration
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Usage } from './Usage'

describe('Usage', () => {
  it('renders the usage section', () => {
    render(<Usage />)
    expect(screen.getByRole('region', { name: /usage/i })).toBeInTheDocument()
  })
})
