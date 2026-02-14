import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { Navbar } from '../../../../src/components/layout/Navbar';
import { NAV_LINKS, GITHUB_URL, DOCS_URL } from '../../../../src/utils/constants';

describe('Navbar', () => {
  beforeEach(() => {
    // Reset any mocks
    vi.clearAllMocks();
  });

  // Test Case 1: Component renders with fixed/sticky positioning
  describe('positioning', () => {
    it('renders with sticky positioning', () => {
      render(<Navbar />);
      const nav = screen.getByRole('navigation');
      expect(nav).toHaveClass('sticky', 'top-0');
    });

    it('has z-index for proper layering', () => {
      render(<Navbar />);
      const nav = screen.getByRole('navigation');
      expect(nav).toHaveClass('z-50');
    });
  });

  // Test Case 2: All navigation links are present
  describe('navigation links', () => {
    it('renders all navigation links: Home, Features, Documentation, GitHub', () => {
      render(<Navbar />);

      // Check for all navigation links (multiple instances exist for desktop + mobile)
      expect(screen.getAllByRole('link', { name: /home/i }).length).toBeGreaterThanOrEqual(1);
      expect(screen.getAllByRole('link', { name: /features/i }).length).toBeGreaterThanOrEqual(1);
      expect(screen.getAllByRole('link', { name: /documentation/i }).length).toBeGreaterThanOrEqual(1);
      expect(screen.getAllByRole('link', { name: /github/i }).length).toBeGreaterThanOrEqual(1);
    });

    it('has correct href attributes for internal links', () => {
      render(<Navbar />);

      // Use getAllByRole since there are desktop and mobile versions
      const homeLinks = screen.getAllByRole('link', { name: /home/i });
      const featuresLinks = screen.getAllByRole('link', { name: /features/i });

      // Check the first link (desktop version)
      expect(homeLinks[0]).toHaveAttribute('href', '#home');
      expect(featuresLinks[0]).toHaveAttribute('href', '#features');
    });

    it('has correct href attributes for external links', () => {
      render(<Navbar />);

      const docLinks = screen.getAllByRole('link', { name: /documentation/i });
      const githubLinks = screen.getAllByRole('link', { name: /github/i });

      // Get the first (desktop) link
      expect(docLinks[0]).toHaveAttribute('href', DOCS_URL);
      expect(githubLinks[0]).toHaveAttribute('href', GITHUB_URL);
    });
  });

  // Test Case 7: External GitHub link opens in new tab with proper rel attribute
  describe('external links security', () => {
    it('GitHub link opens in new tab with rel="noopener noreferrer"', () => {
      render(<Navbar />);

      const githubLinks = screen.getAllByRole('link', { name: /github/i });
      const githubLink = githubLinks[0]; // Desktop link

      expect(githubLink).toHaveAttribute('target', '_blank');
      expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('Documentation link opens in new tab with rel="noopener noreferrer"', () => {
      render(<Navbar />);

      const docLinks = screen.getAllByRole('link', { name: /documentation/i });
      const docLink = docLinks[0]; // Desktop link

      expect(docLink).toHaveAttribute('target', '_blank');
      expect(docLink).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });

  // Test Case 5: Mobile hamburger menu visibility
  describe('responsive mobile menu', () => {
    it('hamburger menu button is present', () => {
      render(<Navbar />);

      const menuButton = screen.getByRole('button', { name: /open menu/i });
      expect(menuButton).toBeInTheDocument();
    });

    it('hamburger icon is visible in the button', () => {
      render(<Navbar />);

      const hamburgerIcon = screen.getByTestId('hamburger-icon');
      expect(hamburgerIcon).toBeInTheDocument();
    });

    it('mobile menu is hidden by default', () => {
      render(<Navbar />);

      const mobileMenu = document.getElementById('mobile-menu');
      expect(mobileMenu).toHaveClass('hidden');
    });

    // Test Case 6: Mobile menu toggle expands/collapses
    it('clicking toggle shows mobile menu', async () => {
      const user = userEvent.setup();
      render(<Navbar />);

      const menuButton = screen.getByRole('button', { name: /open menu/i });
      await user.click(menuButton);

      const mobileMenu = document.getElementById('mobile-menu');
      expect(mobileMenu).toHaveClass('block');
      expect(mobileMenu).not.toHaveClass('hidden');
    });

    it('clicking toggle again hides mobile menu', async () => {
      const user = userEvent.setup();
      render(<Navbar />);

      const menuButton = screen.getByRole('button', { name: /open menu/i });

      // Open menu
      await user.click(menuButton);
      let mobileMenu = document.getElementById('mobile-menu');
      expect(mobileMenu).toHaveClass('block');

      // Close menu - need to get the button again with new label
      const closeButton = screen.getByRole('button', { name: /close menu/i });
      await user.click(closeButton);

      mobileMenu = document.getElementById('mobile-menu');
      expect(mobileMenu).toHaveClass('hidden');
    });

    it('mobile menu has all navigation links', async () => {
      const user = userEvent.setup();
      render(<Navbar />);

      // Open mobile menu
      const menuButton = screen.getByRole('button', { name: /open menu/i });
      await user.click(menuButton);

      // All links should be visible in mobile menu (now there are duplicates - desktop and mobile)
      const homeLinks = screen.getAllByRole('link', { name: /home/i });
      const featuresLinks = screen.getAllByRole('link', { name: /features/i });
      const docLinks = screen.getAllByRole('link', { name: /documentation/i });
      const githubLinks = screen.getAllByRole('link', { name: /github/i });

      // Should have at least 2 of each (desktop + mobile)
      expect(homeLinks.length).toBeGreaterThanOrEqual(2);
      expect(featuresLinks.length).toBeGreaterThanOrEqual(2);
      expect(docLinks.length).toBeGreaterThanOrEqual(2);
      expect(githubLinks.length).toBeGreaterThanOrEqual(2);
    });
  });

  // Test accessibility
  describe('accessibility', () => {
    it('navigation has proper aria-label', () => {
      render(<Navbar />);
      const nav = screen.getByRole('navigation');
      expect(nav).toHaveAttribute('aria-label', 'Main navigation');
    });

    it('mobile menu button has aria-expanded attribute', () => {
      render(<Navbar />);
      const menuButton = screen.getByRole('button', { name: /open menu/i });
      expect(menuButton).toHaveAttribute('aria-expanded', 'false');
    });

    it('mobile menu button has aria-controls attribute', () => {
      render(<Navbar />);
      const menuButton = screen.getByRole('button', { name: /open menu/i });
      expect(menuButton).toHaveAttribute('aria-controls', 'mobile-menu');
    });

    it('aria-expanded updates when menu is opened', async () => {
      const user = userEvent.setup();
      render(<Navbar />);

      const menuButton = screen.getByRole('button', { name: /open menu/i });
      await user.click(menuButton);

      const closeButton = screen.getByRole('button', { name: /close menu/i });
      expect(closeButton).toHaveAttribute('aria-expanded', 'true');
    });
  });

  // Test smooth scroll behavior
  describe('smooth scroll navigation', () => {
    it('clicking internal link triggers scrollIntoView', async () => {
      const user = userEvent.setup();

      // Create a mock element to scroll to
      const mockElement = document.createElement('div');
      mockElement.id = 'features';
      mockElement.scrollIntoView = vi.fn();
      document.body.appendChild(mockElement);

      render(<Navbar />);

      const featuresLinks = screen.getAllByRole('link', { name: /features/i });
      await user.click(featuresLinks[0]);

      expect(mockElement.scrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' });

      // Cleanup
      document.body.removeChild(mockElement);
    });

    it('clicking internal link in mobile menu closes the menu', async () => {
      const user = userEvent.setup();

      // Create mock target element
      const mockElement = document.createElement('div');
      mockElement.id = 'home';
      mockElement.scrollIntoView = vi.fn();
      document.body.appendChild(mockElement);

      render(<Navbar />);

      // Open mobile menu
      const menuButton = screen.getByRole('button', { name: /open menu/i });
      await user.click(menuButton);

      // Click internal link in mobile menu
      const homeLinks = screen.getAllByRole('link', { name: /home/i });
      const mobileHomeLink = homeLinks[homeLinks.length - 1]; // Last one is mobile
      await user.click(mobileHomeLink);

      // Menu should be hidden
      const mobileMenu = document.getElementById('mobile-menu');
      expect(mobileMenu).toHaveClass('hidden');

      // Cleanup
      document.body.removeChild(mockElement);
    });
  });

  // Test brand/logo link
  describe('brand/logo', () => {
    it('renders MirDB brand text', () => {
      render(<Navbar />);
      expect(screen.getByText('MirDB')).toBeInTheDocument();
    });

    it('brand link points to home', () => {
      render(<Navbar />);
      // Get the brand link (first link with MirDB text)
      const brandLink = screen.getByRole('link', { name: 'MirDB' });
      expect(brandLink).toHaveAttribute('href', '#home');
    });
  });
});
