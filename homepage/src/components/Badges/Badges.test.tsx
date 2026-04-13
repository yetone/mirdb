/**
 * Badges Component Unit Tests
 * Owner: Scenario 5 - CI/CD Status Badges
 *
 * Tests:
 * - StatusBadge renders image with correct src
 * - StatusBadge has descriptive alt text
 * - StatusBadge is wrapped in link with correct href
 * - StatusBadge has proper security attributes
 */
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { StatusBadge } from './StatusBadge';
import { CIRCLECI_URL, CIRCLECI_BADGE_URL } from '../../utils/constants';

describe('StatusBadge', () => {
  const defaultProps = {
    src: CIRCLECI_BADGE_URL,
    alt: 'Build Status',
    href: CIRCLECI_URL,
  };

  it('renders badge image with correct src', () => {
    render(<StatusBadge {...defaultProps} />);

    const image = screen.getByTestId('status-badge-image');
    expect(image).toHaveAttribute('src', CIRCLECI_BADGE_URL);
  });

  it('renders badge image with descriptive alt text', () => {
    render(<StatusBadge {...defaultProps} />);

    const image = screen.getByTestId('status-badge-image');
    expect(image).toHaveAttribute('alt', 'Build Status');
  });

  it('wraps badge in link pointing to CI dashboard', () => {
    render(<StatusBadge {...defaultProps} />);

    const link = screen.getByTestId('status-badge');
    expect(link.tagName).toBe('A');
    expect(link).toHaveAttribute('href', CIRCLECI_URL);
  });

  it('has proper security attributes on link', () => {
    render(<StatusBadge {...defaultProps} />);

    const link = screen.getByTestId('status-badge');
    expect(link).toHaveAttribute('target', '_blank');
    expect(link).toHaveAttribute('rel', 'noopener noreferrer');
  });

  it('renders image wrapped in anchor tag with correct structure', () => {
    render(<StatusBadge {...defaultProps} />);

    const link = screen.getByTestId('status-badge');
    const image = screen.getByTestId('status-badge-image');

    // Verify image is a child of the link
    expect(link).toContainElement(image);

    // Verify the link has the correct href
    expect(link).toHaveAttribute('href', CIRCLECI_URL);

    // Verify the image has the correct src
    expect(image).toHaveAttribute('src', CIRCLECI_BADGE_URL);
  });

  it('applies correct CSS classes', () => {
    render(<StatusBadge {...defaultProps} />);

    const link = screen.getByTestId('status-badge');
    const image = screen.getByTestId('status-badge-image');

    expect(link).toHaveClass('status-badge-link');
    expect(image).toHaveClass('status-badge-image');
  });

  it('renders with custom alt text', () => {
    render(
      <StatusBadge
        src="https://example.com/badge.svg"
        alt="Custom Alt Text"
        href="https://example.com"
      />
    );

    const image = screen.getByTestId('status-badge-image');
    expect(image).toHaveAttribute('alt', 'Custom Alt Text');
  });

  it('renders with custom href', () => {
    render(
      <StatusBadge
        src="https://example.com/badge.svg"
        alt="Badge"
        href="https://custom-dashboard.example.com"
      />
    );

    const link = screen.getByTestId('status-badge');
    expect(link).toHaveAttribute('href', 'https://custom-dashboard.example.com');
  });
});
