/**
 * Badge Component Unit Tests
 * Owner: Scenario 8 - Rust Crate Badge Display
 *
 * Tests for Badge and CratesBadge components:
 * - Badge renders with label and value
 * - Badge renders as link when href provided
 * - CratesBadge displays version number
 * - CratesBadge links to crates.io
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Badge, CratesBadge } from '../../../src/components/ui/Badge';
import { CRATES_URL } from '../../../src/utils/constants';

describe('Badge Component', () => {
  describe('Basic Badge', () => {
    it('renders with label and value', () => {
      render(<Badge label="Status" value="Active" testId="test-badge" />);

      expect(screen.getByTestId('test-badge-label')).toHaveTextContent('Status');
      expect(screen.getByTestId('test-badge-value')).toHaveTextContent('Active');
    });

    it('renders as plain span without href', () => {
      render(<Badge label="Version" value="1.0.0" testId="plain-badge" />);

      const badge = screen.getByTestId('plain-badge');
      expect(badge.tagName).toBe('SPAN');
      expect(screen.queryByTestId('plain-badge-link')).not.toBeInTheDocument();
    });

    it('renders as link when href is provided', () => {
      render(
        <Badge
          label="GitHub"
          value="View"
          href="https://github.com/example"
          testId="link-badge"
        />
      );

      const link = screen.getByTestId('link-badge-link');
      expect(link).toBeInTheDocument();
      expect(link).toHaveAttribute('href', 'https://github.com/example');
      expect(link).toHaveAttribute('target', '_blank');
      expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('applies correct variant class', () => {
      render(<Badge label="Test" value="Value" variant="success" testId="variant-badge" />);

      const badge = screen.getByTestId('variant-badge');
      expect(badge).toHaveClass('badge--success');
    });

    it('has accessible aria-label when rendered as link', () => {
      render(
        <Badge
          label="Status"
          value="Online"
          href="https://example.com"
          testId="aria-badge"
        />
      );

      const link = screen.getByTestId('aria-badge-link');
      expect(link).toHaveAttribute('aria-label', 'Status: Online');
    });
  });

  describe('CratesBadge', () => {
    it('displays crates.io label', () => {
      render(<CratesBadge version="0.1.0" />);

      expect(screen.getByTestId('crates-badge-label')).toHaveTextContent('crates.io');
    });

    it('displays version number with v prefix', () => {
      render(<CratesBadge version="0.1.0" />);

      expect(screen.getByTestId('crates-badge-value')).toHaveTextContent('v0.1.0');
    });

    it('handles version with existing v prefix', () => {
      render(<CratesBadge version="v1.2.3" />);

      expect(screen.getByTestId('crates-badge-value')).toHaveTextContent('v1.2.3');
    });

    it('links to correct crates.io URL for mirdb', () => {
      render(<CratesBadge version="0.1.0" />);

      const link = screen.getByTestId('crates-badge-link');
      expect(link).toHaveAttribute('href', CRATES_URL);
    });

    it('links to custom crate URL when crateName specified', () => {
      render(<CratesBadge version="1.0.0" crateName="other-crate" />);

      const link = screen.getByTestId('crates-badge-link');
      expect(link).toHaveAttribute('href', 'https://crates.io/crates/other-crate');
    });

    it('applies crates variant styling', () => {
      render(<CratesBadge version="0.1.0" />);

      const badge = screen.getByTestId('crates-badge');
      expect(badge).toHaveClass('badge--crates');
    });

    it('opens in new tab with security attributes', () => {
      render(<CratesBadge version="0.1.0" />);

      const link = screen.getByTestId('crates-badge-link');
      expect(link).toHaveAttribute('target', '_blank');
      expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('is accessible with proper aria-label', () => {
      render(<CratesBadge version="0.1.0" />);

      const link = screen.getByTestId('crates-badge-link');
      expect(link).toHaveAttribute('aria-label', 'crates.io: v0.1.0');
    });

    it('accepts additional className', () => {
      render(<CratesBadge version="0.1.0" className="custom-class" />);

      const badge = screen.getByTestId('crates-badge');
      expect(badge).toHaveClass('custom-class');
    });
  });

  describe('Version Display Validation', () => {
    it('displays semantic version correctly', () => {
      render(<CratesBadge version="2.0.0-beta.1" />);

      expect(screen.getByTestId('crates-badge-value')).toHaveTextContent('v2.0.0-beta.1');
    });

    it('displays pre-release versions', () => {
      render(<CratesBadge version="0.0.1-alpha" />);

      expect(screen.getByTestId('crates-badge-value')).toHaveTextContent('v0.0.1-alpha');
    });
  });
});
