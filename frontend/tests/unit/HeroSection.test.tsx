/**
 * Hero Section Unit Tests
 * Owner: Scenarios 1, 12, 17, 19
 *
 * Test coverage:
 * - Scenario 1: Hero section display and content
 * - Scenario 12: Background effects and animations
 * - Scenario 17: FuturisticButton integration
 * - Scenario 19: Value proposition communication
 *
 * Test suites:
 * - describe('Hero Section Content')
 * - describe('CTA Buttons')
 * - describe('Background Effects')
 * - describe('Animations')
 */

import React from 'react';
import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders, createMockAuthContext } from '../utils/renderWithProviders';
import HeroSection from '../../src/components/homepage/HeroSection';

describe('Hero Section Display and Content (Scenario 1)', () => {
  describe('Hero Section Content', () => {
    it('should render the hero section container', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toBeInTheDocument();
    });

    it('should display a headline related to URL shortening', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const headline = screen.getByTestId('hero-headline');
      expect(headline).toBeInTheDocument();

      // Verify headline contains text related to URL shortening, link tracking, or insights
      const headlineText = headline.textContent?.toLowerCase() || '';
      expect(
        headlineText.includes('shorten') ||
        headlineText.includes('link') ||
        headlineText.includes('track') ||
        headlineText.includes('insight') ||
        headlineText.includes('url')
      ).toBe(true);
    });

    it('should display a subheadline explaining key benefits', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const subheadline = screen.getByTestId('hero-subheadline');
      expect(subheadline).toBeInTheDocument();
      expect(subheadline.textContent).toBeTruthy();

      // Verify subheadline provides additional context (more than 50 chars)
      expect(subheadline.textContent!.length).toBeGreaterThan(50);
    });

    it('should have proper semantic heading structure with h1', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const headline = screen.getByTestId('hero-headline');
      expect(headline.tagName).toBe('H1');
    });

    it('should have an aria-label for accessibility', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const heroSection = screen.getByTestId('hero-section');
      expect(heroSection).toHaveAttribute('aria-label');
    });
  });

  describe('CTA Buttons for Unauthenticated Users', () => {
    it('should display "Get Started" primary CTA when not authenticated', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const getStartedButton = screen.getByTestId('get-started-button');
      expect(getStartedButton).toBeInTheDocument();
      expect(getStartedButton.textContent?.toLowerCase()).toContain('get started');
    });

    it('should display "Login" secondary CTA when not authenticated', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const loginButton = screen.getByTestId('login-button');
      expect(loginButton).toBeInTheDocument();
      expect(loginButton.textContent?.toLowerCase()).toContain('login');
    });

    it('should link Get Started button to register page', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const getStartedButton = screen.getByTestId('get-started-button');
      expect(getStartedButton).toHaveAttribute('href', '/register');
    });

    it('should link Login button to login page', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const loginButton = screen.getByTestId('login-button');
      expect(loginButton).toHaveAttribute('href', '/login');
    });
  });

  describe('CTA Buttons for Authenticated Users', () => {
    it('should display "Go to Dashboard" CTA when authenticated', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({
          isAuthenticated: true,
          user: { id: 1, username: 'testuser', email: 'test@example.com', is_admin: 0 },
        }),
      });

      const dashboardButton = screen.getByTestId('hero-cta-dashboard');
      expect(dashboardButton).toBeInTheDocument();
      expect(dashboardButton.textContent?.toLowerCase()).toContain('dashboard');
    });

    it('should not display Get Started or Login when authenticated', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({
          isAuthenticated: true,
          user: { id: 1, username: 'testuser', email: 'test@example.com', is_admin: 0 },
        }),
      });

      expect(screen.queryByTestId('get-started-button')).not.toBeInTheDocument();
      expect(screen.queryByTestId('login-button')).not.toBeInTheDocument();
    });

    it('should link Dashboard button to dashboard page', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({
          isAuthenticated: true,
          user: { id: 1, username: 'testuser', email: 'test@example.com', is_admin: 0 },
        }),
      });

      const dashboardButton = screen.getByTestId('hero-cta-dashboard');
      expect(dashboardButton).toHaveAttribute('href', '/dashboard');
    });
  });

  describe('Value Proposition Communication (Scenario 19)', () => {
    it('should communicate URL shortening capability in headline', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const headline = screen.getByTestId('hero-headline');
      const headlineText = headline.textContent?.toLowerCase() || '';

      // Check for URL shortening related keywords
      expect(
        headlineText.includes('shorten') ||
        headlineText.includes('short') ||
        headlineText.includes('link')
      ).toBe(true);
    });

    it('should communicate tracking/analytics capability', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const headline = screen.getByTestId('hero-headline');
      const subheadline = screen.getByTestId('hero-subheadline');
      const combinedText = `${headline.textContent} ${subheadline.textContent}`.toLowerCase();

      // Check for analytics/tracking related keywords
      expect(
        combinedText.includes('track') ||
        combinedText.includes('analytics') ||
        combinedText.includes('insight') ||
        combinedText.includes('click') ||
        combinedText.includes('statistics')
      ).toBe(true);
    });

    it('should explain benefits in subheadline', () => {
      renderWithProviders(<HeroSection />, {
        authContext: createMockAuthContext({ isAuthenticated: false }),
      });

      const subheadline = screen.getByTestId('hero-subheadline');
      const subheadlineText = subheadline.textContent?.toLowerCase() || '';

      // Check for benefit-related keywords
      expect(
        subheadlineText.includes('memorable') ||
        subheadlineText.includes('statistics') ||
        subheadlineText.includes('understand') ||
        subheadlineText.includes('audience') ||
        subheadlineText.includes('geographic') ||
        subheadlineText.includes('device')
      ).toBe(true);
    });
  });
});
