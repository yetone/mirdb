import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import App from '../../src/App';

describe('Navigation Integration Tests', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  // Test Case 3: Smooth scroll navigation
  describe('smooth scroll navigation', () => {
    it('clicking Features link scrolls to Features section', async () => {
      const user = userEvent.setup();
      render(<App />);

      // Get the features section element
      const featuresSection = document.getElementById('features');
      expect(featuresSection).toBeInTheDocument();

      // Mock scrollIntoView on the features section
      featuresSection!.scrollIntoView = vi.fn();

      // Click on Features link in navbar
      const featuresLinks = screen.getAllByRole('link', { name: /features/i });
      await user.click(featuresLinks[0]);

      // Verify scrollIntoView was called with smooth behavior
      expect(featuresSection!.scrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' });
    });

    it('clicking Home link scrolls to Home section', async () => {
      const user = userEvent.setup();
      render(<App />);

      // Get the home section element
      const homeSection = document.getElementById('home');
      expect(homeSection).toBeInTheDocument();

      // Mock scrollIntoView
      homeSection!.scrollIntoView = vi.fn();

      // Click on Home link in navbar
      const homeLinks = screen.getAllByRole('link', { name: /home/i });
      await user.click(homeLinks[0]);

      // Verify scrollIntoView was called
      expect(homeSection!.scrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' });
    });

    it('brand logo link scrolls to home section', async () => {
      const user = userEvent.setup();
      render(<App />);

      const homeSection = document.getElementById('home');
      homeSection!.scrollIntoView = vi.fn();

      // Click on brand/logo
      const brandLink = screen.getByRole('link', { name: 'MirDB' });
      await user.click(brandLink);

      expect(homeSection!.scrollIntoView).toHaveBeenCalledWith({ behavior: 'smooth' });
    });
  });

  // Test Case 5: Mobile menu responsive behavior
  describe('responsive mobile menu', () => {
    it('mobile menu is hidden by default', () => {
      render(<App />);

      const mobileMenu = document.getElementById('mobile-menu');
      expect(mobileMenu).toHaveClass('hidden');
    });

    it('hamburger menu button toggles mobile menu visibility', async () => {
      const user = userEvent.setup();
      render(<App />);

      // Initially hidden
      let mobileMenu = document.getElementById('mobile-menu');
      expect(mobileMenu).toHaveClass('hidden');

      // Click to open
      const menuButton = screen.getByRole('button', { name: /open menu/i });
      await user.click(menuButton);

      mobileMenu = document.getElementById('mobile-menu');
      expect(mobileMenu).toHaveClass('block');
      expect(mobileMenu).not.toHaveClass('hidden');
    });

    // Test Case 6: Mobile menu expands/collapses
    it('mobile navigation menu expands and collapses on toggle', async () => {
      const user = userEvent.setup();
      render(<App />);

      // Open menu
      const openButton = screen.getByRole('button', { name: /open menu/i });
      await user.click(openButton);

      let mobileMenu = document.getElementById('mobile-menu');
      expect(mobileMenu).toHaveClass('block');

      // Close menu
      const closeButton = screen.getByRole('button', { name: /close menu/i });
      await user.click(closeButton);

      mobileMenu = document.getElementById('mobile-menu');
      expect(mobileMenu).toHaveClass('hidden');
    });

    it('mobile menu contains all navigation links', async () => {
      const user = userEvent.setup();
      render(<App />);

      // Open mobile menu
      const menuButton = screen.getByRole('button', { name: /open menu/i });
      await user.click(menuButton);

      const mobileMenu = document.getElementById('mobile-menu');
      expect(mobileMenu).not.toBeNull();

      // Check that mobile menu contains all links
      const mobileMenuElement = mobileMenu!;
      expect(within(mobileMenuElement).getByRole('link', { name: /home/i })).toBeInTheDocument();
      expect(within(mobileMenuElement).getByRole('link', { name: /features/i })).toBeInTheDocument();
      expect(within(mobileMenuElement).getByRole('link', { name: /documentation/i })).toBeInTheDocument();
      expect(within(mobileMenuElement).getByRole('link', { name: /github/i })).toBeInTheDocument();
    });

    it('clicking navigation link in mobile menu closes the menu', async () => {
      const user = userEvent.setup();
      render(<App />);

      // Ensure home section exists and has scrollIntoView
      const homeSection = document.getElementById('home');
      homeSection!.scrollIntoView = vi.fn();

      // Open mobile menu
      const menuButton = screen.getByRole('button', { name: /open menu/i });
      await user.click(menuButton);

      // Click on Home link in mobile menu
      const mobileMenu = document.getElementById('mobile-menu');
      const mobileHomeLink = within(mobileMenu!).getByRole('link', { name: /home/i });
      await user.click(mobileHomeLink);

      // Menu should close after navigation
      expect(document.getElementById('mobile-menu')).toHaveClass('hidden');
    });
  });

  // Full page layout integration
  describe('full page layout', () => {
    it('renders Navbar at the top', () => {
      render(<App />);

      const nav = screen.getByRole('navigation');
      expect(nav).toBeInTheDocument();
    });

    it('renders Footer at the bottom', () => {
      render(<App />);

      const footer = screen.getByRole('contentinfo');
      expect(footer).toBeInTheDocument();
    });

    it('renders main content between Navbar and Footer', () => {
      render(<App />);

      const main = screen.getByRole('main');
      expect(main).toBeInTheDocument();
    });

    it('page has proper semantic structure', () => {
      render(<App />);

      // Should have nav, main, footer in order
      const nav = screen.getByRole('navigation');
      const main = screen.getByRole('main');
      const footer = screen.getByRole('contentinfo');

      expect(nav).toBeInTheDocument();
      expect(main).toBeInTheDocument();
      expect(footer).toBeInTheDocument();
    });

    it('Home section exists with proper id', () => {
      render(<App />);

      const homeSection = document.getElementById('home');
      expect(homeSection).toBeInTheDocument();
    });

    it('Features section exists with proper id', () => {
      render(<App />);

      const featuresSection = document.getElementById('features');
      expect(featuresSection).toBeInTheDocument();
    });
  });

  // External links behavior
  describe('external links', () => {
    it('GitHub link in navbar has correct external attributes', () => {
      render(<App />);

      const githubLinks = screen.getAllByRole('link', { name: /github/i });
      // First GitHub link is in navbar
      const navGithubLink = githubLinks[0];

      expect(navGithubLink).toHaveAttribute('target', '_blank');
      expect(navGithubLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('GitHub link in footer has correct external attributes', () => {
      render(<App />);

      // Footer GitHub link
      const footer = screen.getByRole('contentinfo');
      const footerGithubLink = within(footer).getByRole('link', { name: /github/i });

      expect(footerGithubLink).toHaveAttribute('target', '_blank');
      expect(footerGithubLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    it('Documentation link has correct external attributes', () => {
      render(<App />);

      const docLinks = screen.getAllByRole('link', { name: /documentation/i });
      const docLink = docLinks[0];

      expect(docLink).toHaveAttribute('target', '_blank');
      expect(docLink).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });
});
