/**
 * Common Components Unit Tests
 * Owner: Scenario 4 - GitHub Repository Links
 *
 * Tests for shared utility components.
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { ExternalLink } from './ExternalLink';

describe('ExternalLink', () => {
  it('renders children correctly', () => {
    render(<ExternalLink href="https://example.com">Click me</ExternalLink>);
    expect(screen.getByText('Click me')).toBeInTheDocument();
  });

  it('sets href attribute correctly', () => {
    render(<ExternalLink href="https://github.com/yetone/mirdb">GitHub</ExternalLink>);
    const link = screen.getByRole('link', { name: 'GitHub' });
    expect(link).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
  });

  it('applies target="_blank" attribute for external URLs', () => {
    render(<ExternalLink href="https://github.com/yetone/mirdb">GitHub</ExternalLink>);
    const link = screen.getByRole('link', { name: 'GitHub' });
    expect(link).toHaveAttribute('target', '_blank');
  });

  it('applies rel="noopener noreferrer" for security', () => {
    render(<ExternalLink href="https://github.com/yetone/mirdb">GitHub</ExternalLink>);
    const link = screen.getByRole('link', { name: 'GitHub' });
    expect(link).toHaveAttribute('rel', 'noopener noreferrer');
  });

  it('applies className when provided', () => {
    render(
      <ExternalLink href="https://example.com" className="custom-class">
        Link
      </ExternalLink>
    );
    const link = screen.getByRole('link', { name: 'Link' });
    expect(link).toHaveClass('custom-class');
  });

  it('renders without className when not provided', () => {
    render(<ExternalLink href="https://example.com">Link</ExternalLink>);
    const link = screen.getByRole('link', { name: 'Link' });
    expect(link).not.toHaveAttribute('class');
  });

  it('renders with React elements as children', () => {
    render(
      <ExternalLink href="https://example.com">
        <span data-testid="child-element">Nested content</span>
      </ExternalLink>
    );
    expect(screen.getByTestId('child-element')).toBeInTheDocument();
    expect(screen.getByText('Nested content')).toBeInTheDocument();
  });

  it('has all required security attributes for GitHub URL', () => {
    render(<ExternalLink href="https://github.com/yetone/mirdb">View on GitHub</ExternalLink>);
    const link = screen.getByRole('link', { name: 'View on GitHub' });

    // Verify all security attributes are present
    expect(link).toHaveAttribute('target', '_blank');
    expect(link).toHaveAttribute('rel');

    const relValue = link.getAttribute('rel');
    expect(relValue).toContain('noopener');
  });
});
