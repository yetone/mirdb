/**
 * Header Component Tests
 * Owner: Scenario 1 - Hero Section Display
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Header } from './Header'
import { PRODUCT_NAME, GITHUB_URL } from '../../utils/constants'

describe('Header', () => {
  it('renders without crashing', () => {
    render(<Header />)
    expect(screen.getByRole('banner')).toBeInTheDocument()
  })

  it('displays the brand name', () => {
    render(<Header />)
    expect(screen.getByText(PRODUCT_NAME)).toBeInTheDocument()
  })

  it('contains navigation element', () => {
    render(<Header />)
    expect(screen.getByRole('navigation')).toBeInTheDocument()
  })

  it('has links to main sections', () => {
    render(<Header />)

    expect(screen.getByRole('link', { name: /features/i })).toHaveAttribute('href', '#features')
    expect(screen.getByRole('link', { name: /installation/i })).toHaveAttribute('href', '#installation')
    expect(screen.getByRole('link', { name: /usage/i })).toHaveAttribute('href', '#usage')
  })

  it('has a GitHub link with correct URL', () => {
    render(<Header />)
    const githubLink = screen.getByRole('link', { name: /github/i })
    expect(githubLink).toHaveAttribute('href', GITHUB_URL)
    expect(githubLink).toHaveAttribute('target', '_blank')
    expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer')
  })

  it('brand link points to homepage', () => {
    render(<Header />)
    const brandLink = screen.getByRole('link', { name: PRODUCT_NAME })
    expect(brandLink).toHaveAttribute('href', '/')
  })
})
