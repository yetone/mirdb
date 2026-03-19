/**
 * Unit tests for Navigation component.
 * Owner: Scenario 8 - Navigation Header
 */

import { render, screen, fireEvent } from '@testing-library/react';
import { Navigation } from '@/components/layout/Navigation';
import { NAV_LINKS } from '@/lib/constants';

// Mock useSmoothScroll hook
const mockScrollTo = jest.fn();
jest.mock('@/hooks/useSmoothScroll', () => ({
  useSmoothScroll: () => ({ scrollTo: mockScrollTo }),
}));

describe('Navigation', () => {
  beforeEach(() => {
    mockScrollTo.mockClear();
  });

  it('renders the navigation element with correct aria label', () => {
    render(<Navigation />);
    const nav = screen.getByRole('navigation', { name: /main navigation/i });
    expect(nav).toBeInTheDocument();
  });

  it('renders all navigation links from NAV_LINKS', () => {
    render(<Navigation />);

    NAV_LINKS.forEach((link) => {
      const linkElements = screen.getAllByRole('menuitem', { name: link.label });
      expect(linkElements.length).toBeGreaterThan(0);
    });
  });

  it('renders links to Features, How It Works, Status, Quick Start, Resources', () => {
    render(<Navigation />);

    expect(screen.getAllByText('Features').length).toBeGreaterThan(0);
    expect(screen.getAllByText('How It Works').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Status').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Quick Start').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Resources').length).toBeGreaterThan(0);
  });

  it('calls scrollTo when a navigation link is clicked', () => {
    render(<Navigation />);

    const featuresLink = screen.getAllByRole('menuitem', { name: 'Features' })[0];
    fireEvent.click(featuresLink);

    expect(mockScrollTo).toHaveBeenCalledWith('#features');
  });

  it('renders mobile menu button on mobile viewport', () => {
    render(<Navigation />);
    const menuButton = screen.getByRole('button', { name: /open menu/i });
    expect(menuButton).toBeInTheDocument();
  });

  it('toggles mobile menu when menu button is clicked', () => {
    render(<Navigation />);
    const menuButton = screen.getByRole('button', { name: /open menu/i });

    // Menu should be closed initially
    expect(screen.queryByRole('button', { name: /close menu/i })).not.toBeInTheDocument();

    // Open menu
    fireEvent.click(menuButton);
    expect(screen.getByRole('button', { name: /close menu/i })).toBeInTheDocument();
    expect(screen.getByRole('button')).toHaveAttribute('aria-expanded', 'true');
  });

  it('closes mobile menu when a link is clicked', () => {
    render(<Navigation />);
    const menuButton = screen.getByRole('button', { name: /open menu/i });

    // Open menu
    fireEvent.click(menuButton);

    // Click a link in the mobile menu
    const mobileMenu = screen.getByRole('menu');
    const featuresLink = mobileMenu.querySelector('a[href="#features"]');
    if (featuresLink) {
      fireEvent.click(featuresLink);
    }

    // Menu should be closed
    expect(screen.getByRole('button', { name: /open menu/i })).toBeInTheDocument();
  });

  it('applies custom className when provided', () => {
    render(<Navigation className="custom-nav-class" />);
    const nav = screen.getByRole('navigation');
    expect(nav).toHaveClass('custom-nav-class');
  });

  it('navigation links have correct href attributes', () => {
    render(<Navigation />);

    NAV_LINKS.forEach((link) => {
      const linkElements = screen.getAllByRole('menuitem', { name: link.label });
      linkElements.forEach((element) => {
        expect(element).toHaveAttribute('href', link.href);
      });
    });
  });
});
