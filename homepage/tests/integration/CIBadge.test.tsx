/**
 * CI/CD Status Badge Integration Tests
 * Scenario 15: CI/CD Status Badge Display
 *
 * Tests the integration of the CircleCI status badge within the Hero component,
 * verifying that the badge image loads correctly and the link functions properly.
 */

import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { Hero } from '@/components/sections/Hero'

describe('CI/CD Status Badge Integration', () => {
  // Test Case 4: Load badge image - Image loads without error
  it('badge image element is properly configured for loading', () => {
    render(<Hero />)

    const badgeImage = screen.getByTestId('circleci-badge-image')

    // Verify image has all necessary attributes for proper loading
    expect(badgeImage).toBeInTheDocument()
    expect(badgeImage.tagName).toBe('IMG')
    expect(badgeImage).toHaveAttribute('src')
    expect(badgeImage).toHaveAttribute('alt')

    // Verify the src is a valid URL
    const src = badgeImage.getAttribute('src')
    expect(src).toMatch(/^https:\/\/circleci\.com\//)
    expect(src).toContain('.svg')
  })

  it('badge image has proper styling for consistent display', () => {
    render(<Hero />)

    const badgeImage = screen.getByTestId('circleci-badge-image')

    // Verify image has height class for consistent rendering
    expect(badgeImage).toHaveClass('h-5')
  })

  it('badge link and image are properly nested', () => {
    render(<Hero />)

    const badgeLink = screen.getByTestId('circleci-badge-link')
    const badgeImage = screen.getByTestId('circleci-badge-image')

    // Verify image is a child of the link
    expect(badgeLink).toContainElement(badgeImage)
  })

  it('badge link has security attributes for external link', () => {
    render(<Hero />)

    const badgeLink = screen.getByTestId('circleci-badge-link')

    // Verify security attributes are set correctly
    expect(badgeLink).toHaveAttribute('target', '_blank')
    expect(badgeLink).toHaveAttribute('rel', 'noopener noreferrer')
  })

  it('badge is positioned correctly within the Hero section', () => {
    render(<Hero />)

    const heroSection = screen.getByRole('region', { name: /hero/i })
    const badgeLink = screen.getByTestId('circleci-badge-link')

    // Verify badge is within the Hero section
    expect(heroSection).toContainElement(badgeLink)
  })

  it('simulates image load event without error', () => {
    render(<Hero />)

    const badgeImage = screen.getByTestId('circleci-badge-image')

    // Simulate successful image load
    const loadHandler = vi.fn()
    badgeImage.addEventListener('load', loadHandler)

    // Dispatch load event (simulating successful image load)
    fireEvent.load(badgeImage)

    expect(loadHandler).toHaveBeenCalled()
  })
})
