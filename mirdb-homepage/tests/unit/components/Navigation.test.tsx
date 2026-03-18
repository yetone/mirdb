/**
 * Navigation Component Unit Tests.
 * Owner: Scenario 9 - Navigation and Footer
 *
 * Tests:
 * - Navigation component renders links correctly
 * - Anchor links have proper hrefs
 * - External links open in new tab
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { Navigation } from '../../../src/components/layout/Navigation';
import type { NavItem } from '../../../src/types';

// Mock the useSmoothScroll hook
vi.mock('../../../src/hooks/useSmoothScroll', () => ({
  useSmoothScroll: () => ({
    scrollTo: vi.fn(),
  }),
}));

describe('Navigation', () => {
  const mockNavItems: NavItem[] = [
    { label: 'Features', href: '#features' },
    { label: 'Usage', href: '#usage' },
    { label: 'Quick Start', href: '#quickstart' },
    { label: 'GitHub', href: 'https://github.com/yetone/mirdb' },
  ];

  it('renders navigation element with correct aria label', () => {
    render(<Navigation items={mockNavItems} />);

    const nav = screen.getByRole('navigation', { name: /main navigation/i });
    expect(nav).toBeInTheDocument();
  });

  it('renders all nav items with anchor links', () => {
    render(<Navigation items={mockNavItems} />);

    expect(screen.getByTestId('nav-link-features')).toBeInTheDocument();
    expect(screen.getByTestId('nav-link-usage')).toBeInTheDocument();
    expect(screen.getByTestId('nav-link-quick-start')).toBeInTheDocument();
    expect(screen.getByTestId('nav-link-github')).toBeInTheDocument();
  });

  it('renders anchor links with correct href attributes', () => {
    render(<Navigation items={mockNavItems} />);

    const featuresLink = screen.getByTestId('nav-link-features');
    expect(featuresLink).toHaveAttribute('href', '#features');

    const usageLink = screen.getByTestId('nav-link-usage');
    expect(usageLink).toHaveAttribute('href', '#usage');

    const quickStartLink = screen.getByTestId('nav-link-quick-start');
    expect(quickStartLink).toHaveAttribute('href', '#quickstart');
  });

  it('renders external links with target="_blank" and rel="noopener noreferrer"', () => {
    render(<Navigation items={mockNavItems} />);

    const githubLink = screen.getByTestId('nav-link-github');
    expect(githubLink).toHaveAttribute('target', '_blank');
    expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  it('does not add target or rel to anchor links', () => {
    render(<Navigation items={mockNavItems} />);

    const featuresLink = screen.getByTestId('nav-link-features');
    expect(featuresLink).not.toHaveAttribute('target');
    expect(featuresLink).not.toHaveAttribute('rel');
  });

  it('renders correct link text', () => {
    render(<Navigation items={mockNavItems} />);

    expect(screen.getByText('Features')).toBeInTheDocument();
    expect(screen.getByText('Usage')).toBeInTheDocument();
    expect(screen.getByText('Quick Start')).toBeInTheDocument();
    expect(screen.getByText('GitHub')).toBeInTheDocument();
  });

  it('applies custom className', () => {
    render(<Navigation items={mockNavItems} className="custom-class" />);

    const nav = screen.getByTestId('navigation');
    expect(nav).toHaveClass('custom-class');
  });

  it('prevents default on anchor link clicks', () => {
    render(<Navigation items={mockNavItems} />);

    const featuresLink = screen.getByTestId('nav-link-features');
    const clickEvent = new MouseEvent('click', { bubbles: true });

    const preventDefaultSpy = vi.spyOn(clickEvent, 'preventDefault');
    fireEvent(featuresLink, clickEvent);

    expect(preventDefaultSpy).toHaveBeenCalled();
  });

  it('does not prevent default on external link clicks', () => {
    render(<Navigation items={mockNavItems} />);

    const githubLink = screen.getByTestId('nav-link-github');
    const clickEvent = new MouseEvent('click', { bubbles: true });

    const preventDefaultSpy = vi.spyOn(clickEvent, 'preventDefault');
    fireEvent(githubLink, clickEvent);

    expect(preventDefaultSpy).not.toHaveBeenCalled();
  });

  it('renders empty navigation when no items provided', () => {
    render(<Navigation items={[]} />);

    const nav = screen.getByTestId('navigation');
    const list = nav.querySelector('ul');
    expect(list?.children.length).toBe(0);
  });
});
