/**
 * Navigation Component Unit Tests
 * Owner: Scenario 5 - Navigation and GitHub Links
 *
 * Tests for:
 * - Logo/brand name display
 * - Navigation links presence
 * - GitHub link with correct href
 * - Accessibility attributes
 */

import { render, screen, fireEvent } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';
import { Navigation } from '@/components/layout/Navigation';
import { SITE_TITLE, GITHUB_URL, NAVIGATION_LINKS } from '@/utils/constants';

describe('Navigation Component', () => {
  describe('Test Case 1: Logo/brand name display', () => {
    it('displays the product name "MirDB"', () => {
      render(<Navigation />);
      const logo = screen.getByText(SITE_TITLE);
      expect(logo).toBeInTheDocument();
      expect(logo).toHaveTextContent('MirDB');
    });

    it('renders the logo as a link to home', () => {
      render(<Navigation />);
      const logo = screen.getByRole('link', { name: `${SITE_TITLE} home` });
      expect(logo).toBeInTheDocument();
      expect(logo).toHaveAttribute('href', '#hero');
    });

    it('logo has proper styling', () => {
      render(<Navigation />);
      const logo = screen.getByText(SITE_TITLE);
      expect(logo).toBeVisible();
    });
  });

  describe('Test Case 2: Navigation links presence', () => {
    it('displays navigation link for Features', () => {
      render(<Navigation />);
      const featuresLink = screen.getByRole('link', { name: 'Features' });
      expect(featuresLink).toBeInTheDocument();
      expect(featuresLink).toHaveAttribute('href', '#features');
    });

    it('displays navigation link for Usage', () => {
      render(<Navigation />);
      const usageLink = screen.getByRole('link', { name: 'Usage' });
      expect(usageLink).toBeInTheDocument();
      expect(usageLink).toHaveAttribute('href', '#usage');
    });

    it('displays navigation link for Getting Started', () => {
      render(<Navigation />);
      const gettingStartedLink = screen.getByRole('link', { name: 'Getting Started' });
      expect(gettingStartedLink).toBeInTheDocument();
      expect(gettingStartedLink).toHaveAttribute('href', '#getting-started');
    });

    it('renders all internal navigation links from constants', () => {
      render(<Navigation />);
      const internalLinks = NAVIGATION_LINKS.filter(link => !link.isExternal);

      internalLinks.forEach(link => {
        const navLink = screen.getByRole('link', { name: link.label });
        expect(navLink).toBeInTheDocument();
        expect(navLink).toHaveAttribute('href', link.href);
      });
    });
  });

  describe('Test Case 3: GitHub link', () => {
    it('has GitHub link with correct href', () => {
      render(<Navigation />);
      const githubButton = screen.getByRole('link', { name: /github/i });
      expect(githubButton).toBeInTheDocument();
      expect(githubButton).toHaveAttribute('href', GITHUB_URL);
      expect(githubButton).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    });

    it('GitHub link opens in new tab', () => {
      render(<Navigation />);
      const githubButton = screen.getByRole('link', { name: /github/i });
      expect(githubButton).toHaveAttribute('target', '_blank');
    });

    it('GitHub link has security attributes', () => {
      render(<Navigation />);
      const githubButton = screen.getByRole('link', { name: /github/i });
      expect(githubButton).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });

  describe('Accessibility', () => {
    it('has proper nav element with aria-label', () => {
      render(<Navigation />);
      const nav = screen.getByRole('navigation', { name: /main navigation/i });
      expect(nav).toBeInTheDocument();
    });

    it('logo has accessible aria-label', () => {
      render(<Navigation />);
      const logo = screen.getByRole('link', { name: /MirDB home/i });
      expect(logo).toBeInTheDocument();
    });

    it('GitHub button is a link', () => {
      render(<Navigation />);
      const githubButton = screen.getByRole('link', { name: /github/i });
      expect(githubButton).toBeInTheDocument();
    });

    it('mobile menu button has proper aria attributes', () => {
      render(<Navigation />);
      const menuButton = screen.getByLabelText(/open menu/i);
      expect(menuButton).toBeInTheDocument();
      expect(menuButton).toHaveAttribute('aria-expanded', 'false');
      expect(menuButton).toHaveAttribute('aria-controls', 'mobile-navigation');
    });
  });

  describe('Mobile menu interaction', () => {
    it('toggles mobile menu when button is clicked', () => {
      render(<Navigation />);
      const menuButton = screen.getByLabelText(/open menu/i);

      fireEvent.click(menuButton);

      expect(menuButton).toHaveAttribute('aria-expanded', 'true');
      expect(screen.getByLabelText(/close menu/i)).toBeInTheDocument();
    });

    it('mobile navigation is visible when menu is open', () => {
      render(<Navigation />);
      const menuButton = screen.getByLabelText(/open menu/i);

      fireEvent.click(menuButton);

      const mobileNav = document.getElementById('mobile-navigation');
      expect(mobileNav).toBeInTheDocument();
    });

    it('closes mobile menu when a link is clicked', () => {
      render(<Navigation />);
      const menuButton = screen.getByLabelText(/open menu/i);

      fireEvent.click(menuButton);

      const mobileNav = document.getElementById('mobile-navigation');
      expect(mobileNav).toBeInTheDocument();

      const featuresLinks = screen.getAllByRole('link', { name: 'Features' });
      const mobileLink = featuresLinks.find(link => mobileNav?.contains(link));

      if (mobileLink) {
        fireEvent.click(mobileLink);
        expect(document.getElementById('mobile-navigation')).not.toBeInTheDocument();
      }
    });
  });

  describe('Smooth scroll navigation', () => {
    it('calls scrollIntoView when clicking anchor link', () => {
      const scrollIntoViewMock = vi.fn();
      const mockElement = document.createElement('div');
      mockElement.id = 'features';
      mockElement.scrollIntoView = scrollIntoViewMock;
      document.body.appendChild(mockElement);

      render(<Navigation />);
      const featuresLink = screen.getAllByRole('link', { name: 'Features' })[0];

      fireEvent.click(featuresLink);

      expect(scrollIntoViewMock).toHaveBeenCalledWith({ behavior: 'smooth' });

      document.body.removeChild(mockElement);
    });
  });
});
