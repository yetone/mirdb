/**
 * Navigation Components Unit Tests
 * Owner: Scenario 17 - Navigation and Anchor Links
 *
 * Tests for MobileMenu React component
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MobileMenu } from '../../../src/components/Navigation/MobileMenu';

const mockNavLinks = [
  { href: '#features', label: 'Features' },
  { href: '#architecture', label: 'Architecture' },
  { href: '#configuration', label: 'Configuration' },
  { href: '#installation', label: 'Installation' },
  { href: '#commands', label: 'Commands' },
  { href: '#roadmap', label: 'Roadmap' },
];

describe('MobileMenu', () => {
  let originalScrollIntoView: typeof Element.prototype.scrollIntoView;
  let originalMatchMedia: typeof window.matchMedia;

  beforeEach(() => {
    // Mock scrollIntoView
    originalScrollIntoView = Element.prototype.scrollIntoView;
    Element.prototype.scrollIntoView = vi.fn();

    // Mock matchMedia for reduced motion check
    originalMatchMedia = window.matchMedia;
    window.matchMedia = vi.fn().mockImplementation((query) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }));

    // Mock history.pushState
    vi.spyOn(window.history, 'pushState').mockImplementation(() => {});
  });

  afterEach(() => {
    Element.prototype.scrollIntoView = originalScrollIntoView;
    window.matchMedia = originalMatchMedia;
    vi.restoreAllMocks();
    document.body.style.overflow = '';
  });

  describe('Hamburger Button', () => {
    it('should render hamburger button', () => {
      render(<MobileMenu navLinks={mockNavLinks} />);

      const hamburgerButton = screen.getByRole('button', { name: /open menu/i });
      expect(hamburgerButton).toBeInTheDocument();
    });

    it('should have aria-expanded=false initially', () => {
      render(<MobileMenu navLinks={mockNavLinks} />);

      const hamburgerButton = screen.getByRole('button', { name: /open menu/i });
      expect(hamburgerButton).toHaveAttribute('aria-expanded', 'false');
    });

    it('should have aria-controls pointing to mobile-menu', () => {
      render(<MobileMenu navLinks={mockNavLinks} />);

      const hamburgerButton = screen.getByRole('button', { name: /open menu/i });
      expect(hamburgerButton).toHaveAttribute('aria-controls', 'mobile-menu');
    });
  });

  describe('Menu Toggle', () => {
    it('should open menu when hamburger button is clicked', async () => {
      const user = userEvent.setup();
      render(<MobileMenu navLinks={mockNavLinks} />);

      const hamburgerButton = screen.getByRole('button', { name: /open menu/i });
      await user.click(hamburgerButton);

      const menu = screen.getByRole('dialog');
      expect(menu).toBeInTheDocument();
    });

    it('should update aria-expanded when menu opens', async () => {
      const user = userEvent.setup();
      render(<MobileMenu navLinks={mockNavLinks} />);

      const hamburgerButton = screen.getByRole('button', { name: /open menu/i });
      await user.click(hamburgerButton);

      // The button's label changes to "Close menu" when open
      // There are two close buttons: one is the hamburger toggle, one is inside the menu panel
      const closeButtons = screen.getAllByRole('button', { name: /close menu/i });
      // The first close button is the hamburger toggle with aria-expanded
      const toggleButton = closeButtons[0];
      expect(toggleButton).toHaveAttribute('aria-expanded', 'true');
    });

    it('should close menu when close button is clicked', async () => {
      const user = userEvent.setup();
      render(<MobileMenu navLinks={mockNavLinks} />);

      // Open menu
      const hamburgerButton = screen.getByRole('button', { name: /open menu/i });
      await user.click(hamburgerButton);

      // Click close button inside menu
      const closeButtons = screen.getAllByRole('button', { name: /close menu/i });
      await user.click(closeButtons[1]); // The second close button is inside the menu panel

      // Menu should be hidden (translated off-screen)
      const menu = screen.getByRole('dialog');
      expect(menu).toHaveClass('translate-x-full');
    });

    it('should close menu when pressing Escape key', async () => {
      const user = userEvent.setup();
      render(<MobileMenu navLinks={mockNavLinks} />);

      // Open menu
      const hamburgerButton = screen.getByRole('button', { name: /open menu/i });
      await user.click(hamburgerButton);

      // Press Escape
      await user.keyboard('{Escape}');

      // Menu should be closed
      const menu = screen.getByRole('dialog');
      expect(menu).toHaveClass('translate-x-full');
    });

    it('should close menu when clicking overlay', async () => {
      const user = userEvent.setup();
      render(<MobileMenu navLinks={mockNavLinks} />);

      // Open menu
      const hamburgerButton = screen.getByRole('button', { name: /open menu/i });
      await user.click(hamburgerButton);

      // Click overlay (the backdrop)
      const overlay = document.querySelector('.fixed.inset-0.bg-background\\/80');
      expect(overlay).toBeInTheDocument();
      await user.click(overlay!);

      // Menu should be closed
      const menu = screen.getByRole('dialog');
      expect(menu).toHaveClass('translate-x-full');
    });
  });

  describe('Navigation Links', () => {
    it('should render all navigation links', async () => {
      const user = userEvent.setup();
      render(<MobileMenu navLinks={mockNavLinks} />);

      // Open menu
      const hamburgerButton = screen.getByRole('button', { name: /open menu/i });
      await user.click(hamburgerButton);

      // Check all links are present
      for (const link of mockNavLinks) {
        const navLink = screen.getByRole('link', { name: link.label });
        expect(navLink).toBeInTheDocument();
        expect(navLink).toHaveAttribute('href', link.href);
      }
    });

    it('should close menu after clicking a navigation link', async () => {
      const user = userEvent.setup();

      // Create a target element for scrolling
      const targetElement = document.createElement('div');
      targetElement.id = 'features';
      document.body.appendChild(targetElement);

      render(<MobileMenu navLinks={mockNavLinks} />);

      // Open menu
      const hamburgerButton = screen.getByRole('button', { name: /open menu/i });
      await user.click(hamburgerButton);

      // Click Features link
      const featuresLink = screen.getByRole('link', { name: 'Features' });
      await user.click(featuresLink);

      // Wait for menu to close
      await waitFor(() => {
        const menu = screen.getByRole('dialog');
        expect(menu).toHaveClass('translate-x-full');
      });

      // Cleanup
      document.body.removeChild(targetElement);
    });

    it('should update URL hash when clicking navigation link', async () => {
      const user = userEvent.setup();
      const pushStateSpy = vi.spyOn(window.history, 'pushState');

      // Create a target element
      const targetElement = document.createElement('div');
      targetElement.id = 'features';
      document.body.appendChild(targetElement);

      render(<MobileMenu navLinks={mockNavLinks} />);

      // Open menu and click link
      await user.click(screen.getByRole('button', { name: /open menu/i }));
      await user.click(screen.getByRole('link', { name: 'Features' }));

      expect(pushStateSpy).toHaveBeenCalledWith(null, '', '#features');

      // Cleanup
      document.body.removeChild(targetElement);
    });
  });

  describe('Body Scroll Lock', () => {
    it('should prevent body scroll when menu is open', async () => {
      const user = userEvent.setup();
      render(<MobileMenu navLinks={mockNavLinks} />);

      // Open menu
      await user.click(screen.getByRole('button', { name: /open menu/i }));

      expect(document.body.style.overflow).toBe('hidden');
    });

    it('should restore body scroll when menu is closed', async () => {
      const user = userEvent.setup();
      render(<MobileMenu navLinks={mockNavLinks} />);

      // Open menu
      await user.click(screen.getByRole('button', { name: /open menu/i }));

      // Close menu
      await user.keyboard('{Escape}');

      expect(document.body.style.overflow).toBe('');
    });
  });

  describe('Accessibility', () => {
    it('should have proper role="dialog" on menu', async () => {
      const user = userEvent.setup();
      render(<MobileMenu navLinks={mockNavLinks} />);

      await user.click(screen.getByRole('button', { name: /open menu/i }));

      const menu = screen.getByRole('dialog');
      expect(menu).toBeInTheDocument();
      expect(menu).toHaveAttribute('aria-modal', 'true');
    });

    it('should have navigation landmark inside menu', async () => {
      const user = userEvent.setup();
      render(<MobileMenu navLinks={mockNavLinks} />);

      await user.click(screen.getByRole('button', { name: /open menu/i }));

      const nav = screen.getByRole('navigation', { name: /mobile navigation/i });
      expect(nav).toBeInTheDocument();
    });

    it('should focus first link when menu opens', async () => {
      const user = userEvent.setup();
      render(<MobileMenu navLinks={mockNavLinks} />);

      await user.click(screen.getByRole('button', { name: /open menu/i }));

      await waitFor(() => {
        const firstLink = screen.getByRole('link', { name: 'Features' });
        expect(firstLink).toHaveFocus();
      });
    });
  });

  describe('Reduced Motion', () => {
    it('should not use transition duration when reduced motion is preferred', async () => {
      // Mock reduced motion preference
      window.matchMedia = vi.fn().mockImplementation((query) => ({
        matches: query === '(prefers-reduced-motion: reduce)',
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }));

      const user = userEvent.setup();
      render(<MobileMenu navLinks={mockNavLinks} />);

      await user.click(screen.getByRole('button', { name: /open menu/i }));

      const menu = screen.getByRole('dialog');
      // When reduced motion is preferred, transition classes should not be applied
      expect(menu).not.toHaveClass('duration-300');
    });
  });

  describe('GitHub Link', () => {
    it('should render GitHub link in mobile menu', async () => {
      const user = userEvent.setup();
      render(<MobileMenu navLinks={mockNavLinks} />);

      await user.click(screen.getByRole('button', { name: /open menu/i }));

      const githubLink = screen.getByRole('link', { name: /view on github/i });
      expect(githubLink).toBeInTheDocument();
      expect(githubLink).toHaveAttribute('href', 'https://github.com/mirdb/mirdb');
      expect(githubLink).toHaveAttribute('target', '_blank');
      expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });
});
