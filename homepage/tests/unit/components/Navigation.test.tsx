/**
 * Unit tests for Navigation Components
 * Owner: Scenario 4 - Navigation and Header
 *
 * Tests cover:
 * - Header component with logo and navigation
 * - Navigation component with nav items
 * - Active section highlighting
 * - MobileMenu component visibility and items
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { Header } from '../../../src/components/layout/Header';
import { Navigation } from '../../../src/components/layout/Navigation';
import { MobileMenu } from '../../../src/components/layout/MobileMenu';
import { NAV_ITEMS } from '../../../src/constants/navigation';
import { NavItem } from '../../../src/types';

const mockNavItems: NavItem[] = [
  { id: 'hero', label: 'Home', href: '#hero' },
  { id: 'features', label: 'Features', href: '#features' },
  { id: 'quick-start', label: 'Quick Start', href: '#quick-start' },
];

describe('Header Component', () => {
  beforeEach(() => {
    // Reset any mocks
    vi.clearAllMocks();
  });

  // Test Case 1: Render Header component - Header contains logo and navigation links
  it('renders Header with logo and navigation links', () => {
    render(<Header />);

    // Check logo is present
    const logo = screen.getByTestId('header-logo');
    expect(logo).toBeInTheDocument();

    // Check header contains navigation
    const desktopNav = screen.getByTestId('desktop-navigation');
    expect(desktopNav).toBeInTheDocument();

    // Check navigation links are present
    NAV_ITEMS.forEach((item) => {
      const link = screen.getByTestId(`nav-link-${item.id}`);
      expect(link).toBeInTheDocument();
      expect(link).toHaveTextContent(item.label);
    });
  });

  it('renders the header element', () => {
    render(<Header />);

    const header = screen.getByTestId('header');
    expect(header).toBeInTheDocument();
    expect(header.tagName.toLowerCase()).toBe('header');
  });

  it('renders logo image with correct src', () => {
    render(<Header />);

    const logoImage = screen.getByAltText('MirDB');
    expect(logoImage).toBeInTheDocument();
    expect(logoImage).toHaveAttribute('src', '/assets/logo.gif');
  });

  it('renders MirDB text next to logo', () => {
    render(<Header />);

    expect(screen.getByText('MirDB')).toBeInTheDocument();
  });

  it('renders mobile menu button', () => {
    render(<Header />);

    const menuButton = screen.getByTestId('mobile-menu-button');
    expect(menuButton).toBeInTheDocument();
    expect(menuButton).toHaveAttribute('aria-label', 'Open menu');
  });

  it('opens mobile menu when hamburger button is clicked', () => {
    render(<Header />);

    const menuButton = screen.getByTestId('mobile-menu-button');
    fireEvent.click(menuButton);

    const mobileMenu = screen.getByTestId('mobile-menu');
    expect(mobileMenu).toHaveClass('translate-x-0');
  });

  it('header has fixed positioning for sticky behavior', () => {
    render(<Header />);

    const header = screen.getByTestId('header');
    expect(header).toHaveClass('fixed');
    expect(header).toHaveClass('top-0');
    expect(header).toHaveClass('left-0');
    expect(header).toHaveClass('right-0');
  });
});

describe('Navigation Component', () => {
  // Test Case 3: Render Navigation with nav items - All navigation links are rendered
  it('renders all navigation links', () => {
    render(<Navigation items={mockNavItems} />);

    const navigation = screen.getByTestId('desktop-navigation');
    expect(navigation).toBeInTheDocument();

    mockNavItems.forEach((item) => {
      const link = screen.getByTestId(`nav-link-${item.id}`);
      expect(link).toBeInTheDocument();
      expect(link).toHaveTextContent(item.label);
      expect(link).toHaveAttribute('href', item.href);
    });
  });

  // Test Case 5: Render Navigation with activeSection prop - Active section link has distinct styling
  it('applies active styling to the current section link', () => {
    render(<Navigation items={mockNavItems} activeSection="features" />);

    const activeLink = screen.getByTestId('nav-link-features');
    expect(activeLink).toHaveAttribute('aria-current', 'page');
    expect(activeLink).toHaveClass('text-primary-600');
    expect(activeLink).toHaveClass('bg-primary-50');

    // Non-active links should not have active styling
    const inactiveLink = screen.getByTestId('nav-link-hero');
    expect(inactiveLink).not.toHaveAttribute('aria-current');
    expect(inactiveLink).not.toHaveClass('bg-primary-50');
  });

  it('calls onNavClick when a navigation link is clicked', () => {
    const onNavClick = vi.fn();
    render(<Navigation items={mockNavItems} onNavClick={onNavClick} />);

    const featuresLink = screen.getByTestId('nav-link-features');
    fireEvent.click(featuresLink);

    expect(onNavClick).toHaveBeenCalledWith('features');
  });

  it('scrolls to section when clicked without onNavClick callback', () => {
    const mockElement = { scrollIntoView: vi.fn() };
    vi.spyOn(document, 'getElementById').mockReturnValue(mockElement as unknown as HTMLElement);

    render(<Navigation items={mockNavItems} />);

    const featuresLink = screen.getByTestId('nav-link-features');
    fireEvent.click(featuresLink);

    expect(document.getElementById).toHaveBeenCalledWith('features');
    expect(mockElement.scrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' });

    vi.restoreAllMocks();
  });

  it('has proper ARIA label for accessibility', () => {
    render(<Navigation items={mockNavItems} />);

    const navigation = screen.getByTestId('desktop-navigation');
    expect(navigation).toHaveAttribute('aria-label', 'Main navigation');
  });

  it('is hidden on mobile by default', () => {
    render(<Navigation items={mockNavItems} />);

    const navigation = screen.getByTestId('desktop-navigation');
    expect(navigation).toHaveClass('hidden');
    expect(navigation).toHaveClass('md:flex');
  });
});

describe('MobileMenu Component', () => {
  // Test Case 9: Render MobileMenu with isOpen=true - Menu is visible with all navigation items
  it('renders visible menu with all navigation items when isOpen is true', () => {
    const onClose = vi.fn();
    render(<MobileMenu isOpen={true} onClose={onClose} items={mockNavItems} />);

    const mobileMenu = screen.getByTestId('mobile-menu');
    expect(mobileMenu).toBeInTheDocument();
    expect(mobileMenu).toHaveClass('translate-x-0');

    // Check all navigation items are rendered
    mockNavItems.forEach((item) => {
      const link = screen.getByTestId(`mobile-nav-link-${item.id}`);
      expect(link).toBeInTheDocument();
      expect(link).toHaveTextContent(item.label);
    });
  });

  it('menu is hidden when isOpen is false', () => {
    const onClose = vi.fn();
    render(<MobileMenu isOpen={false} onClose={onClose} items={mockNavItems} />);

    const mobileMenu = screen.getByTestId('mobile-menu');
    expect(mobileMenu).toHaveClass('translate-x-full');
  });

  it('calls onClose when close button is clicked', () => {
    const onClose = vi.fn();
    render(<MobileMenu isOpen={true} onClose={onClose} items={mockNavItems} />);

    const closeButton = screen.getByTestId('mobile-menu-close');
    fireEvent.click(closeButton);

    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('calls onClose when backdrop is clicked', () => {
    const onClose = vi.fn();
    render(<MobileMenu isOpen={true} onClose={onClose} items={mockNavItems} />);

    const backdrop = screen.getByTestId('mobile-menu-backdrop');
    fireEvent.click(backdrop);

    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('calls onClose and navigates when a nav link is clicked', () => {
    const onClose = vi.fn();
    const onNavClick = vi.fn();
    render(
      <MobileMenu
        isOpen={true}
        onClose={onClose}
        items={mockNavItems}
        onNavClick={onNavClick}
      />
    );

    const featuresLink = screen.getByTestId('mobile-nav-link-features');
    fireEvent.click(featuresLink);

    expect(onNavClick).toHaveBeenCalledWith('features');
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it('applies active styling to current section in mobile menu', () => {
    const onClose = vi.fn();
    render(
      <MobileMenu
        isOpen={true}
        onClose={onClose}
        items={mockNavItems}
        activeSection="features"
      />
    );

    const activeLink = screen.getByTestId('mobile-nav-link-features');
    expect(activeLink).toHaveAttribute('aria-current', 'page');
    expect(activeLink).toHaveClass('text-primary-600');
    expect(activeLink).toHaveClass('bg-primary-50');
  });

  it('has proper accessibility attributes', () => {
    const onClose = vi.fn();
    render(<MobileMenu isOpen={true} onClose={onClose} items={mockNavItems} />);

    const mobileMenu = screen.getByTestId('mobile-menu');
    expect(mobileMenu).toHaveAttribute('role', 'dialog');
    expect(mobileMenu).toHaveAttribute('aria-modal', 'true');
    expect(mobileMenu).toHaveAttribute('aria-label', 'Mobile navigation menu');
  });

  it('navigation items have minimum touch target size', () => {
    const onClose = vi.fn();
    render(<MobileMenu isOpen={true} onClose={onClose} items={mockNavItems} />);

    mockNavItems.forEach((item) => {
      const link = screen.getByTestId(`mobile-nav-link-${item.id}`);
      expect(link).toHaveClass('min-h-[44px]');
    });
  });
});
