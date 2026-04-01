/**
 * Status Badges Unit Tests.
 * Owner: Scenario 5 - Project Status Badges
 *
 * Test Cases:
 * - TC1: Render page and look for CircleCI badge (Image element with CircleCI badge URL exists)
 * - TC2: Check badge alt text (Badge image has descriptive alt text)
 * - TC3: Check badge link href (Badge is wrapped in anchor linking to CircleCI project page)
 * - TC4: Click on status badge (Verifies link is configured for external navigation)
 */

import { describe, it, expect } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { StatusBadges } from './StatusBadges'
import { CIRCLECI_BADGE_URL, CIRCLECI_STATUS_URL } from '../../utils/constants'

describe('StatusBadges', () => {
  // Test Case 1: Render page and look for CircleCI badge
  describe('TC1: CircleCI badge image', () => {
    it('renders CircleCI badge image with correct URL', () => {
      render(<StatusBadges />)

      const badgeImage = screen.getByTestId('circleci-badge-image')
      expect(badgeImage).toBeInTheDocument()
      expect(badgeImage).toHaveAttribute('src', CIRCLECI_BADGE_URL)
    })

    it('renders image element with CircleCI badge URL', () => {
      render(<StatusBadges />)

      const badgeImage = screen.getByRole('img', { name: 'Build Status' })
      expect(badgeImage).toBeInTheDocument()
      expect(badgeImage.getAttribute('src')).toContain('circleci.com')
    })
  })

  // Test Case 2: Check badge alt text
  describe('TC2: Badge alt text', () => {
    it('has descriptive alt text for accessibility', () => {
      render(<StatusBadges />)

      const badgeImage = screen.getByTestId('circleci-badge-image')
      expect(badgeImage).toHaveAttribute('alt', 'Build Status')
    })

    it('alt text accurately describes the badge content', () => {
      render(<StatusBadges />)

      const badgeImage = screen.getByAltText('Build Status')
      expect(badgeImage).toBeInTheDocument()
    })
  })

  // Test Case 3: Check badge link href
  describe('TC3: Badge link configuration', () => {
    it('wraps badge in anchor linking to CircleCI project page', () => {
      render(<StatusBadges />)

      const badgeLink = screen.getByTestId('circleci-badge-link')
      expect(badgeLink).toBeInTheDocument()
      expect(badgeLink).toHaveAttribute('href', CIRCLECI_STATUS_URL)
    })

    it('configures link for external navigation', () => {
      render(<StatusBadges />)

      const badgeLink = screen.getByTestId('circleci-badge-link')
      expect(badgeLink).toHaveAttribute('target', '_blank')
      expect(badgeLink).toHaveAttribute('rel', 'noopener noreferrer')
    })

    it('has accessible aria-label for the badge link', () => {
      render(<StatusBadges />)

      const badgeLink = screen.getByTestId('circleci-badge-link')
      expect(badgeLink).toHaveAttribute('aria-label', 'View CircleCI build status')
    })
  })

  // Test Case 4: Click on status badge (E2E-style integration test)
  describe('TC4: Badge click behavior', () => {
    it('badge link is clickable and configured for external navigation', () => {
      render(<StatusBadges />)

      const badgeLink = screen.getByTestId('circleci-badge-link')

      // Verify the link element exists and is an anchor
      expect(badgeLink.tagName).toBe('A')

      // Verify click does not throw and link is accessible
      expect(() => fireEvent.click(badgeLink)).not.toThrow()

      // Verify external navigation configuration
      expect(badgeLink).toHaveAttribute('href', CIRCLECI_STATUS_URL)
      expect(badgeLink).toHaveAttribute('target', '_blank')
    })

    it('clicking badge would navigate to CircleCI status page', () => {
      render(<StatusBadges />)

      const badgeLink = screen.getByRole('link', { name: 'View CircleCI build status' })

      // Simulate click event
      fireEvent.click(badgeLink)

      // Verify the href points to CircleCI status page
      const href = badgeLink.getAttribute('href')
      expect(href).toBe(CIRCLECI_STATUS_URL)
      expect(href).toContain('circleci.com/gh/yetone/mirdb')
    })
  })

  // Additional component tests
  describe('Component functionality', () => {
    it('renders container with data-testid', () => {
      render(<StatusBadges />)

      const container = screen.getByTestId('status-badges')
      expect(container).toBeInTheDocument()
    })

    it('accepts custom className prop', () => {
      render(<StatusBadges className="custom-class" />)

      const container = screen.getByTestId('status-badges')
      expect(container).toHaveClass('custom-class')
    })
  })
})
