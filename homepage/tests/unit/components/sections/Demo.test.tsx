/**
 * Unit tests for Demo component.
 * Owner: Scenario 3 - Usage Demo GIF Display
 *
 * Test cases:
 * 1. Image element with src containing 'usage.gif' is present
 * 2. Image has appropriate alt text describing the demo
 * 3. Image has loading='lazy' for lazy loading
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { Demo } from '../../../../src/components/sections/Demo'

describe('Demo Component', () => {
  // Test case 1: Image element with src containing 'usage.gif' is present
  it('should render an image element with src containing usage.gif', () => {
    render(<Demo />)

    const image = screen.getByRole('img')
    expect(image).toBeInTheDocument()
    expect(image).toHaveAttribute('src', expect.stringContaining('usage.gif'))
  })

  // Test case 2: Image has appropriate alt text describing the demo
  it('should have appropriate alt text describing the demo', () => {
    render(<Demo />)

    const image = screen.getByRole('img')
    expect(image).toHaveAttribute('alt')

    const altText = image.getAttribute('alt')
    expect(altText).toBeTruthy()
    // Alt text should mention MirDB and usage/demonstration
    expect(altText?.toLowerCase()).toMatch(/mirdb/i)
    expect(altText?.toLowerCase()).toMatch(/usage|demonstration|demo/i)
  })

  // Test case 3: Image has loading='lazy' for lazy loading
  it('should have loading attribute set to lazy', () => {
    render(<Demo />)

    const image = screen.getByRole('img')
    expect(image).toHaveAttribute('loading', 'lazy')
  })

  // Additional test: Demo section has proper semantic structure
  it('should have proper semantic structure with section and heading', () => {
    render(<Demo />)

    const section = document.querySelector('section.demo')
    expect(section).toBeInTheDocument()
    expect(section).toHaveAttribute('aria-labelledby', 'demo-heading')

    const heading = screen.getByRole('heading', { level: 2 })
    expect(heading).toBeInTheDocument()
    expect(heading).toHaveAttribute('id', 'demo-heading')
  })

  // Additional test: Loading state is shown initially
  it('should show loading placeholder initially', () => {
    render(<Demo />)

    const placeholder = document.querySelector('.demo__placeholder')
    expect(placeholder).toBeInTheDocument()
    expect(screen.getByText(/loading demo/i)).toBeInTheDocument()
  })

  // Additional test: Image has width and height for CLS optimization
  it('should have width and height attributes for layout stability', () => {
    render(<Demo />)

    const image = screen.getByRole('img')
    expect(image).toHaveAttribute('width')
    expect(image).toHaveAttribute('height')
  })

  // Additional test: Loading state disappears when image loads
  it('should hide loading state after image loads', () => {
    render(<Demo />)

    const image = screen.getByRole('img')

    // Simulate image load
    fireEvent.load(image)

    // Image should be visible (have the loaded class)
    expect(image).toHaveClass('demo__image--loaded')
  })

  // Additional test: Error state is shown when image fails to load
  it('should show error state when image fails to load', () => {
    render(<Demo />)

    const image = screen.getByRole('img')

    // Simulate image error
    fireEvent.error(image)

    // Error message should be shown
    const errorAlert = screen.getByRole('alert')
    expect(errorAlert).toBeInTheDocument()
    expect(screen.getByText(/failed to load demo/i)).toBeInTheDocument()
  })

  // Additional test: Description text is present
  it('should have a description explaining the demo', () => {
    render(<Demo />)

    const description = screen.getByText(/memcached/i)
    expect(description).toBeInTheDocument()
  })
})
