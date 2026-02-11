/**
 * Unit tests for Badge component.
 * Owner: Scenario 6 - Build Status Badge
 */

import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Badge } from '../../../../src/components/ui/Badge'
import { CIRCLECI_BADGE_URL, CIRCLECI_BUILD_URL } from '../../../../src/config/content'

describe('Badge Component', () => {
  const defaultProps = {
    src: CIRCLECI_BADGE_URL,
    alt: 'Build Status',
    href: CIRCLECI_BUILD_URL,
  }

  it('should render image element with src containing circleci badge URL', () => {
    render(<Badge {...defaultProps} />)

    const image = screen.getByRole('img')
    expect(image).toBeInTheDocument()
    expect(image).toHaveAttribute('src', expect.stringContaining('circleci'))
  })

  it('should wrap badge in link to CircleCI build page', () => {
    render(<Badge {...defaultProps} />)

    const link = screen.getByRole('link')
    expect(link).toBeInTheDocument()
    expect(link).toHaveAttribute('href', CIRCLECI_BUILD_URL)
  })

  it('should have alt text describing build status', () => {
    render(<Badge {...defaultProps} />)

    const image = screen.getByRole('img')
    expect(image).toHaveAttribute('alt', 'Build Status')
  })

  it('should open link in new tab with security attributes', () => {
    render(<Badge {...defaultProps} />)

    const link = screen.getByRole('link')
    expect(link).toHaveAttribute('target', '_blank')
    expect(link).toHaveAttribute('rel', 'noopener noreferrer')
  })

  it('should render with custom alt text', () => {
    render(<Badge {...defaultProps} alt="Custom Build Status" />)

    const image = screen.getByRole('img')
    expect(image).toHaveAttribute('alt', 'Custom Build Status')
  })

  it('should render with correct CSS class', () => {
    render(<Badge {...defaultProps} />)

    const link = screen.getByRole('link')
    expect(link).toHaveClass('badge')
  })

  it('should render with custom className', () => {
    render(<Badge {...defaultProps} className="custom-badge" />)

    const link = screen.getByRole('link')
    expect(link).toHaveClass('badge')
    expect(link).toHaveClass('custom-badge')
  })
})
