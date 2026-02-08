/**
 * Installation Component Tests
 * Owner: Scenario 3 - Installation Instructions
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Installation } from './Installation'

describe('Installation', () => {
  it('renders the installation section', () => {
    render(<Installation />)
    expect(screen.getByRole('region', { name: /installation/i })).toBeInTheDocument()
  })
})
