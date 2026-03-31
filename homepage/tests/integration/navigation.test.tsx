/**
 * Integration tests for Navigation System
 * Owner: Scenario 4 - Navigation and Header
 *
 * Tests cover:
 * - Mobile viewport behavior (hamburger menu visibility)
 * - Escape key closing mobile menu
 * - Responsive navigation behavior
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { Header } from '../../src/components/layout/Header';
import { MobileMenu } from '../../src/components/layout/MobileMenu';
import { NavItem } from '../../src/types';

const mockNavItems: NavItem[] = [
  { id: 'hero', label: 'Home', href: '#hero' },
  { id: 'features', label: 'Features', href: '#features' },
  { id: 'quick-start', label: 'Quick Start', href: '#quick-start' },
];

// Helper to mock window resize
const setViewportWidth = (width: number) => {
  Object.defineProperty(window, 'innerWidth', {
    writable: true,
    configurable: true,
    value: width,
  });
  window.dispatchEvent(new Event('resize'));
};

// Helper to create mock element with getBoundingClientRect
const createMockElement = (top = 0, bottom = 500) => ({
  scrollIntoView: vi.fn(),
  getBoundingClientRect: () => ({ top, bottom, left: 0, right: 0, width: 0, height: bottom - top }),
});

describe('Navigation Integration Tests', () => {
  let originalGetElementById: typeof document.getElementById;

  beforeEach(() => {
    vi.clearAllMocks();
    // Reset viewport
    Object.defineProperty(window, 'innerWidth', {
      writable: true,
      configurable: true,
      value: 1024,
    });
    // Store original getElementById
    originalGetElementById = document.getElementById;
    // Mock getElementById to return element with getBoundingClientRect
    vi.spyOn(document, 'getElementById').mockImplementation((id) => {
      return createMockElement() as unknown as HTMLElement;
    });
  });

  afterEach(() => {
    // Reset body overflow
    document.body.style.overflow = '';
    // Restore mocks
    vi.restoreAllMocks();
  });

  // Test Case 6: Render page on mobile viewport (375px) - Hamburger menu button is visible, desktop nav is hidden
  describe('Mobile Viewport Behavior', () => {
    it('shows hamburger menu button (which is always rendered but hidden on desktop via CSS)', () => {
      render(<Header />);

      const menuButton = screen.getByTestId('mobile-menu-button');
      expect(menuButton).toBeInTheDocument();
      // The button has md:hidden class which hides it on medium+ screens
      expect(menuButton).toHaveClass('md:hidden');
    });

    it('desktop navigation has responsive classes to hide on mobile', () => {
      render(<Header />);

      const desktopNav = screen.getByTestId('desktop-navigation');
      expect(desktopNav).toHaveClass('hidden');
      expect(desktopNav).toHaveClass('md:flex');
    });

    it('mobile menu button has minimum touch target size for mobile accessibility', () => {
      render(<Header />);

      const menuButton = screen.getByTestId('mobile-menu-button');
      expect(menuButton).toHaveClass('min-w-[44px]');
      expect(menuButton).toHaveClass('min-h-[44px]');
    });
  });

  // Test Case 10: Press Escape key while mobile menu is open - Mobile menu closes
  describe('Escape Key Behavior', () => {
    it('closes mobile menu when Escape key is pressed', async () => {
      const onClose = vi.fn();
      render(<MobileMenu isOpen={true} onClose={onClose} items={mockNavItems} />);

      // Verify menu is open
      const mobileMenu = screen.getByTestId('mobile-menu');
      expect(mobileMenu).toHaveClass('translate-x-0');

      // Press Escape key
      fireEvent.keyDown(document, { key: 'Escape' });

      await waitFor(() => {
        expect(onClose).toHaveBeenCalledTimes(1);
      });
    });

    it('does not call onClose for other keys', () => {
      const onClose = vi.fn();
      render(<MobileMenu isOpen={true} onClose={onClose} items={mockNavItems} />);

      // Press Enter key (should not close)
      fireEvent.keyDown(document, { key: 'Enter' });

      expect(onClose).not.toHaveBeenCalled();
    });

    it('does not call onClose when menu is already closed', () => {
      const onClose = vi.fn();
      render(<MobileMenu isOpen={false} onClose={onClose} items={mockNavItems} />);

      // Press Escape key
      fireEvent.keyDown(document, { key: 'Escape' });

      expect(onClose).not.toHaveBeenCalled();
    });
  });

  describe('Body Overflow Behavior', () => {
    it('sets body overflow to hidden when mobile menu opens', () => {
      const onClose = vi.fn();
      const { rerender } = render(
        <MobileMenu isOpen={false} onClose={onClose} items={mockNavItems} />
      );

      expect(document.body.style.overflow).toBe('');

      rerender(<MobileMenu isOpen={true} onClose={onClose} items={mockNavItems} />);

      expect(document.body.style.overflow).toBe('hidden');
    });

    it('resets body overflow when mobile menu closes', () => {
      const onClose = vi.fn();
      const { rerender } = render(
        <MobileMenu isOpen={true} onClose={onClose} items={mockNavItems} />
      );

      expect(document.body.style.overflow).toBe('hidden');

      rerender(<MobileMenu isOpen={false} onClose={onClose} items={mockNavItems} />);

      expect(document.body.style.overflow).toBe('');
    });
  });

  describe('Header Scroll Behavior', () => {
    it('header adds background styling when scrolled', () => {
      render(<Header />);

      const header = screen.getByTestId('header');

      // Initially should be transparent
      expect(header).toHaveClass('bg-transparent');

      // Simulate scroll
      Object.defineProperty(window, 'scrollY', { value: 50, configurable: true });
      fireEvent.scroll(window);

      // After scroll, should have background
      expect(header).toHaveClass('bg-white/95');
      expect(header).toHaveClass('backdrop-blur-sm');
      expect(header).toHaveClass('shadow-sm');
    });
  });

  describe('Navigation Link Interaction', () => {
    it('clicking logo scrolls to hero section', () => {
      const mockElement = createMockElement();
      vi.mocked(document.getElementById).mockReturnValue(mockElement as unknown as HTMLElement);

      render(<Header />);

      const logo = screen.getByTestId('header-logo');
      fireEvent.click(logo);

      expect(document.getElementById).toHaveBeenCalledWith('hero');
      expect(mockElement.scrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' });
    });
  });

  describe('Full Navigation Flow', () => {
    it('opens mobile menu, navigates, and closes', () => {
      const mockElement = createMockElement();
      vi.mocked(document.getElementById).mockReturnValue(mockElement as unknown as HTMLElement);

      render(<Header />);

      // Open mobile menu
      const menuButton = screen.getByTestId('mobile-menu-button');
      fireEvent.click(menuButton);

      // Verify menu is open
      const mobileMenu = screen.getByTestId('mobile-menu');
      expect(mobileMenu).toHaveClass('translate-x-0');

      // Click on a navigation link
      const featuresLink = screen.getByTestId('mobile-nav-link-features');
      fireEvent.click(featuresLink);

      // Verify navigation occurred
      expect(document.getElementById).toHaveBeenCalledWith('features');
      expect(mockElement.scrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' });

      // Verify menu closed
      expect(mobileMenu).toHaveClass('translate-x-full');
    });
  });

  describe('Accessibility', () => {
    it('mobile menu button has aria-expanded attribute', () => {
      render(<Header />);

      const menuButton = screen.getByTestId('mobile-menu-button');
      expect(menuButton).toHaveAttribute('aria-expanded', 'false');

      fireEvent.click(menuButton);

      expect(menuButton).toHaveAttribute('aria-expanded', 'true');
    });

    it('close button has accessible label', () => {
      const onClose = vi.fn();
      render(<MobileMenu isOpen={true} onClose={onClose} items={mockNavItems} />);

      const closeButton = screen.getByTestId('mobile-menu-close');
      expect(closeButton).toHaveAttribute('aria-label', 'Close menu');
    });
  });
});
