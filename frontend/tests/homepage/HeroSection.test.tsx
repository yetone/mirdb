/**
 * Hero Section Tests
 * Owner: Scenario 1 - Hero Section Display and Value Proposition
 *
 * Tests for the hero section covering:
 * - Headline with value proposition text
 * - Primary CTA button (Get Started)
 * - Secondary CTA button (Sign In)
 * - BackgroundEffect component rendering
 */
import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import { renderWithProviders } from './testUtils';
import HeroSection from '../../src/components/homepage/HeroSection';
import Home from '../../src/pages/Home';

describe('HeroSection', () => {
  describe('Test Case 1: Hero section displays with headline containing value proposition text', () => {
    it('renders headline with "Shorten URLs" value proposition', () => {
      renderWithProviders(<HeroSection />);

      // Check for the main headline containing "Shorten URLs"
      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline).toBeInTheDocument();
      expect(headline.textContent).toMatch(/Shorten URLs/i);
    });

    it('renders supporting tagline text', () => {
      renderWithProviders(<HeroSection />);

      // Check for supporting description text
      expect(screen.getByText(/Transform your long URLs/i)).toBeInTheDocument();
    });

    it('displays analytics and insights messaging', () => {
      renderWithProviders(<HeroSection />);

      // The headline should mention tracking/analytics
      const headline = screen.getByRole('heading', { level: 1 });
      expect(headline.textContent).toMatch(/Track Clicks/i);
    });
  });

  describe('Test Case 2: Primary CTA button is present and visible', () => {
    it('renders primary CTA button with "Get Started" text', () => {
      renderWithProviders(<HeroSection />);

      const getStartedButton = screen.getByRole('button', { name: /Get Started/i });
      expect(getStartedButton).toBeInTheDocument();
      // Button is in the document and accessible - jsdom doesn't compute visibility
      expect(getStartedButton).not.toHaveAttribute('hidden');
    });

    it('primary CTA links to registration page', () => {
      renderWithProviders(<HeroSection />);

      const getStartedLink = screen.getByRole('link', { name: /Get Started/i });
      expect(getStartedLink).toHaveAttribute('href', '/register');
    });
  });

  describe('Test Case 3: Secondary CTA button is present and visible', () => {
    it('renders secondary CTA button with "Sign In" text', () => {
      renderWithProviders(<HeroSection />);

      const signInButton = screen.getByRole('button', { name: /Sign In/i });
      expect(signInButton).toBeInTheDocument();
      // Button is in the document and accessible - jsdom doesn't compute visibility
      expect(signInButton).not.toHaveAttribute('hidden');
    });

    it('secondary CTA links to login page', () => {
      renderWithProviders(<HeroSection />);

      const signInLink = screen.getByRole('link', { name: /Sign In/i });
      expect(signInLink).toHaveAttribute('href', '/login');
    });
  });

  describe('Test Case 4: BackgroundEffect component is rendered', () => {
    it('renders BackgroundEffect component in the DOM', () => {
      renderWithProviders(<HeroSection />);

      const backgroundEffect = screen.getByTestId('background-effect');
      expect(backgroundEffect).toBeInTheDocument();
    });
  });
});

describe('Home Page with HeroSection', () => {
  it('renders the home page with hero section at "/" route', () => {
    renderWithProviders(<Home />);

    // Hero section should be present
    const headline = screen.getByRole('heading', { level: 1 });
    expect(headline.textContent).toMatch(/Shorten URLs/i);

    // CTAs should be present
    expect(screen.getByRole('button', { name: /Get Started/i })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Sign In/i })).toBeInTheDocument();
  });

  it('includes BackgroundEffect in the home page', () => {
    renderWithProviders(<Home />);

    const backgroundEffect = screen.getByTestId('background-effect');
    expect(backgroundEffect).toBeInTheDocument();
  });
});
