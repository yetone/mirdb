/**
 * HeroSection Unit Tests
 * Owner: Scenario 1 - Hero Section Rendering
 *
 * Tests hero section rendering including:
 * - Headline visibility and content
 * - Tagline/value proposition display
 * - BackgroundEffect integration
 * - CTA button behavior based on auth state
 */

import { describe, it, expect } from 'vitest';
import { screen, within } from '@testing-library/react';
import { HeroSection } from '../../../src/components/homepage/HeroSection';
import { renderWithProviders } from './test-utils';

describe('HeroSection', () => {
  describe('Test Case 1: Hero section visibility with headline', () => {
    it('renders hero section with headline containing product messaging', () => {
      renderWithProviders(<HeroSection />);

      // Hero section should be visible
      const heroSection = screen.getByRole('region', { name: /hero section/i });
      expect(heroSection).toBeInTheDocument();
      expect(heroSection).toBeVisible();

      // Should contain headline with URL shortening messaging
      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toBeInTheDocument();
      expect(headline.textContent).toMatch(/shorten|url/i);
    });
  });

  describe('Test Case 2: Main headline with value proposition', () => {
    it('displays main headline with clear value proposition text', () => {
      renderWithProviders(<HeroSection />);

      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toBeInTheDocument();

      // Value proposition should communicate benefit
      expect(headline.textContent).toContain('Shorten URLs, Amplify Reach');
    });

    it('headline has proper styling for prominence', () => {
      renderWithProviders(<HeroSection />);

      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toHaveClass('text-4xl');
    });
  });

  describe('Test Case 3: Tagline describes URL shortening benefits', () => {
    it('displays tagline describing URL shortening service benefits', () => {
      renderWithProviders(<HeroSection />);

      // Find tagline/subheadline paragraph
      const tagline = screen.getByText(/transform long.*urls/i);
      expect(tagline).toBeInTheDocument();
      expect(tagline).toBeVisible();
    });

    it('tagline mentions analytics capabilities', () => {
      renderWithProviders(<HeroSection />);

      const tagline = screen.getByText(/analytics/i);
      expect(tagline).toBeInTheDocument();
    });

    it('tagline mentions trackable links benefit', () => {
      renderWithProviders(<HeroSection />);

      const tagline = screen.getByText(/trackable/i);
      expect(tagline).toBeInTheDocument();
    });
  });

  describe('Test Case 4: BackgroundEffect integration', () => {
    it('renders BackgroundEffect within hero section', () => {
      renderWithProviders(<HeroSection />);

      // BackgroundEffect should be present via data-testid
      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toBeInTheDocument();
    });

    it('BackgroundEffect is within the hero section container', () => {
      renderWithProviders(<HeroSection />);

      const heroSection = screen.getByRole('region', { name: /hero section/i });
      const backgroundEffect = within(heroSection).getByTestId('background-effect');
      expect(backgroundEffect).toBeInTheDocument();
    });

    it('BackgroundEffect has correct accessibility attributes', () => {
      renderWithProviders(<HeroSection />);

      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toHaveAttribute('aria-hidden', 'true');
    });
  });

  describe('CTA Buttons - Unauthenticated User', () => {
    it('shows Get Started button for unauthenticated users', () => {
      renderWithProviders(<HeroSection isAuthenticated={false} />);

      const getStartedButton = screen.getByTestId('hero-get-started-button');
      expect(getStartedButton).toBeInTheDocument();
      expect(getStartedButton).toHaveTextContent(/get started/i);
    });

    it('shows Sign In link for unauthenticated users', () => {
      renderWithProviders(<HeroSection isAuthenticated={false} />);

      const signInLink = screen.getByTestId('hero-sign-in-link');
      expect(signInLink).toBeInTheDocument();
      expect(signInLink).toHaveTextContent(/sign in/i);
    });

    it('Get Started button links to /register', () => {
      renderWithProviders(<HeroSection isAuthenticated={false} />);

      const getStartedButton = screen.getByTestId('hero-get-started-button');
      expect(getStartedButton).toHaveAttribute('href', '/register');
    });

    it('Sign In link links to /login', () => {
      renderWithProviders(<HeroSection isAuthenticated={false} />);

      const signInLink = screen.getByTestId('hero-sign-in-link');
      expect(signInLink).toHaveAttribute('href', '/login');
    });
  });

  describe('CTA Buttons - Authenticated User', () => {
    it('shows Dashboard button for authenticated users', () => {
      renderWithProviders(<HeroSection isAuthenticated={true} />);

      const dashboardButton = screen.getByTestId('hero-dashboard-button');
      expect(dashboardButton).toBeInTheDocument();
      expect(dashboardButton).toHaveTextContent(/dashboard/i);
    });

    it('hides Get Started button for authenticated users', () => {
      renderWithProviders(<HeroSection isAuthenticated={true} />);

      const getStartedButton = screen.queryByTestId('hero-get-started-button');
      expect(getStartedButton).not.toBeInTheDocument();
    });

    it('hides Sign In link for authenticated users', () => {
      renderWithProviders(<HeroSection isAuthenticated={true} />);

      const signInLink = screen.queryByTestId('hero-sign-in-link');
      expect(signInLink).not.toBeInTheDocument();
    });

    it('Dashboard button links to /dashboard', () => {
      renderWithProviders(<HeroSection isAuthenticated={true} />);

      const dashboardButton = screen.getByTestId('hero-dashboard-button');
      expect(dashboardButton).toHaveAttribute('href', '/dashboard');
    });
  });

  describe('Accessibility', () => {
    it('hero section has proper aria-label', () => {
      renderWithProviders(<HeroSection />);

      const heroSection = screen.getByRole('region', { name: /hero section/i });
      expect(heroSection).toHaveAttribute('aria-label', 'Hero section');
    });

    it('Sign In link has descriptive aria-label', () => {
      renderWithProviders(<HeroSection isAuthenticated={false} />);

      const signInLink = screen.getByTestId('hero-sign-in-link');
      expect(signInLink).toHaveAttribute('aria-label', 'Sign in to your existing account');
    });
  });
});
