/**
 * Unit tests for HeroSection component
 * Scenario 1 - Hero Section Display
 *
 * Tests the hero section displays correctly with:
 * - Headline containing URL shortening messaging
 * - Primary CTA (Get Started) for unauthenticated users
 * - Secondary login link in hero section
 */

import { describe, it, expect, vi } from 'vitest';
import { screen } from '@testing-library/react';
import { HeroSection } from '../../../src/components/homepage/HeroSection';
import { renderWithProviders, mockAuthContext, testFixtures } from './test-utils';

describe('HeroSection', () => {
  describe('Test Case 1: Render HomePage component without authentication', () => {
    it('should display hero section with headline containing URL shortening messaging', () => {
      renderWithProviders(<HeroSection />, { authState: mockAuthContext.unauthenticated });

      // Check for headline with URL shortening message
      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toBeInTheDocument();
      expect(headline).toHaveTextContent(/shorten/i);
      expect(headline).toHaveTextContent(/url/i);
    });

    it('should display the correct headline text', () => {
      renderWithProviders(<HeroSection />, { authState: mockAuthContext.unauthenticated });

      expect(screen.getByText(testFixtures.heroHeadline)).toBeInTheDocument();
    });

    it('should display a subheadline with value proposition', () => {
      renderWithProviders(<HeroSection />, { authState: mockAuthContext.unauthenticated });

      // Check for subheadline text about URL transformation
      expect(
        screen.getByText(/transform long, unwieldy urls into clean, memorable short links/i)
      ).toBeInTheDocument();
    });

    it('should render the hero section element', () => {
      renderWithProviders(<HeroSection />, { authState: mockAuthContext.unauthenticated });

      // Check the section has proper structure
      const heroSection = document.querySelector('.hero');
      expect(heroSection).toBeInTheDocument();
    });
  });

  describe('Test Case 2: Check for Get Started or Create Your First Link button', () => {
    it('should display primary CTA button with correct text for unauthenticated users', () => {
      renderWithProviders(<HeroSection />, { authState: mockAuthContext.unauthenticated });

      // Check for primary CTA button
      const ctaButton = screen.getByTestId('hero-cta');
      expect(ctaButton).toBeInTheDocument();
      expect(ctaButton).toHaveTextContent(/get started/i);
    });

    it('should have the CTA button link to registration page', () => {
      renderWithProviders(<HeroSection />, { authState: mockAuthContext.unauthenticated });

      const ctaButton = screen.getByTestId('hero-cta');
      expect(ctaButton).toHaveAttribute('href', '/register');
    });

    it('should have the CTA button visible and accessible', () => {
      renderWithProviders(<HeroSection />, { authState: mockAuthContext.unauthenticated });

      const ctaButton = screen.getByTestId('hero-cta');
      expect(ctaButton).toBeVisible();
      // Check it's a link (from FuturisticButton with `to` prop)
      expect(ctaButton.tagName.toLowerCase()).toBe('a');
    });
  });

  describe('Test Case 3: Check for Login link or button in hero', () => {
    it('should display secondary login option in hero section', () => {
      renderWithProviders(<HeroSection />, { authState: mockAuthContext.unauthenticated });

      // Check for login link
      const loginLink = screen.getByTestId('hero-login-link');
      expect(loginLink).toBeInTheDocument();
      expect(loginLink).toHaveTextContent(/login/i);
    });

    it('should have the login link pointing to login page', () => {
      renderWithProviders(<HeroSection />, { authState: mockAuthContext.unauthenticated });

      const loginLink = screen.getByTestId('hero-login-link');
      expect(loginLink).toHaveAttribute('href', '/login');
    });

    it('should display "Already have an account?" text near login link', () => {
      renderWithProviders(<HeroSection />, { authState: mockAuthContext.unauthenticated });

      expect(screen.getByText(/already have an account\?/i)).toBeInTheDocument();
    });
  });

  describe('Authenticated User Behavior', () => {
    it('should display "Go to Dashboard" CTA when user is authenticated', () => {
      renderWithProviders(<HeroSection />, { authState: mockAuthContext.authenticated });

      const ctaButton = screen.getByTestId('hero-cta');
      expect(ctaButton).toHaveTextContent(/go to dashboard/i);
    });

    it('should link to dashboard for authenticated users', () => {
      renderWithProviders(<HeroSection />, { authState: mockAuthContext.authenticated });

      const ctaButton = screen.getByTestId('hero-cta');
      expect(ctaButton).toHaveAttribute('href', '/dashboard');
    });

    it('should not show login link when user is authenticated', () => {
      renderWithProviders(<HeroSection />, { authState: mockAuthContext.authenticated });

      expect(screen.queryByTestId('hero-login-link')).not.toBeInTheDocument();
    });

    it('should not show "Already have an account?" text when authenticated', () => {
      renderWithProviders(<HeroSection />, { authState: mockAuthContext.authenticated });

      expect(screen.queryByText(/already have an account\?/i)).not.toBeInTheDocument();
    });
  });

  describe('isAuthenticated prop override', () => {
    it('should respect isAuthenticated prop over context when provided as true', () => {
      renderWithProviders(<HeroSection isAuthenticated={true} />, {
        authState: mockAuthContext.unauthenticated,
      });

      const ctaButton = screen.getByTestId('hero-cta');
      expect(ctaButton).toHaveTextContent(/go to dashboard/i);
    });

    it('should respect isAuthenticated prop over context when provided as false', () => {
      renderWithProviders(<HeroSection isAuthenticated={false} />, {
        authState: mockAuthContext.authenticated,
      });

      const ctaButton = screen.getByTestId('hero-cta');
      expect(ctaButton).toHaveTextContent(/get started/i);
    });
  });

  describe('Accessibility', () => {
    it('should have a heading structure for screen readers', () => {
      renderWithProviders(<HeroSection />, { authState: mockAuthContext.unauthenticated });

      const heading = screen.getByRole('heading', { level: 1 });
      expect(heading).toBeInTheDocument();
    });

    it('should have accessible link elements', () => {
      renderWithProviders(<HeroSection />, { authState: mockAuthContext.unauthenticated });

      const links = screen.getAllByRole('link');
      expect(links.length).toBeGreaterThanOrEqual(2); // CTA and Login links
    });
  });
});
