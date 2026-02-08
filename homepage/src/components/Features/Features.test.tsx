/**
 * Features Component Tests
 * Owner: Scenario 2 - Features Section Display
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Features } from './Features'

describe('Features', () => {
  it('renders the features section', () => {
    render(<Features />)
    expect(screen.getByRole('region', { name: /features/i })).toBeInTheDocument()
  })
})
