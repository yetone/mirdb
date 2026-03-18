/**
 * Badge Component Unit Tests.
 * Owner: Scenario 5 - Status Badges and Project Health
 *
 * Tests:
 * - Component renders with correct props
 * - Image and link attributes are properly set
 * - Accessibility attributes are present
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Badge } from '../../../src/components/ui/Badge';

describe('Badge Component', () => {
  const defaultProps = {
    imageUrl: 'https://circleci.com/gh/yetone/mirdb.svg?style=svg',
    linkUrl: 'https://circleci.com/gh/yetone/mirdb',
    alt: 'CircleCI Build Status',
  };

  it('TC4: renders with correct badge URL and link URL props', () => {
    render(<Badge {...defaultProps} />);

    // Verify the image is rendered with correct src
    const image = screen.getByAltText('CircleCI Build Status');
    expect(image).toBeTruthy();
    expect(image.getAttribute('src')).toBe(defaultProps.imageUrl);

    // Verify the link has correct href
    const link = screen.getByTestId('status-badge-link');
    expect(link).toBeTruthy();
    expect(link.getAttribute('href')).toBe(defaultProps.linkUrl);
  });

  it('renders the badge image with alt text for accessibility', () => {
    render(<Badge {...defaultProps} />);

    const image = screen.getByAltText('CircleCI Build Status');
    expect(image).toBeTruthy();
    expect(image.getAttribute('alt')).toBe(defaultProps.alt);
  });

  it('opens link in new tab with security attributes', () => {
    render(<Badge {...defaultProps} />);

    const link = screen.getByTestId('status-badge-link');
    expect(link.getAttribute('target')).toBe('_blank');
    expect(link.getAttribute('rel')).toBe('noopener noreferrer');
  });

  it('renders with different imageUrl and linkUrl props', () => {
    const customProps = {
      imageUrl: 'https://img.shields.io/github/stars/yetone/mirdb',
      linkUrl: 'https://github.com/yetone/mirdb',
      alt: 'GitHub Stars',
    };

    render(<Badge {...customProps} />);

    const image = screen.getByAltText('GitHub Stars');
    expect(image).toBeTruthy();
    expect(image.getAttribute('src')).toBe(customProps.imageUrl);

    const link = screen.getByTestId('status-badge-link');
    expect(link.getAttribute('href')).toBe(customProps.linkUrl);
  });

  it('wraps image in an anchor tag', () => {
    const { container } = render(<Badge {...defaultProps} />);

    // Verify the structure: anchor > img
    const anchor = container.querySelector('a');
    expect(anchor).toBeTruthy();

    const image = anchor?.querySelector('img');
    expect(image).toBeTruthy();
    expect(image?.getAttribute('src')).toBe(defaultProps.imageUrl);
  });

  it('has data-testid attributes for testing', () => {
    render(<Badge {...defaultProps} />);

    const link = screen.getByTestId('status-badge-link');
    const image = screen.getByTestId('status-badge-image');

    expect(link).toBeTruthy();
    expect(image).toBeTruthy();
  });
});
