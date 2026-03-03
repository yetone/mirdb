/**
 * HeroSection Component Tests
 * Owner: Scenario 1 - Homepage Hero Section Rendering
 *
 * Test cases:
 * 1. Component renders without errors and displays service name headline
 * 2. Tagline/subheadline explaining value proposition is visible
 * 3. Get Started button is rendered with prominent styling
 * 4. Sign In button/link is rendered with secondary styling
 */

import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { describe, it, expect } from 'vitest';
import { HeroSection } from '../../src/components/HeroSection';

// Helper to render with Router context
const renderWithRouter = (ui: React.ReactElement) => {
  return render(<BrowserRouter>{ui}</BrowserRouter>);
};

describe('HeroSection Component', () => {
  /**
   * Test Case 1: Component renders without errors and displays service name headline
   */
  it('renders without errors and displays service name headline', () => {
    renderWithRouter(<HeroSection />);

    // Check that the component renders
    const heroSection = screen.getByTestId('hero-section');
    expect(heroSection).toBeInTheDocument();

    // Check that the headline is visible
    const headline = screen.getByTestId('hero-headline');
    expect(headline).toBeInTheDocument();
    expect(headline).toHaveTextContent('URL Shortener');
    expect(headline.tagName.toLowerCase()).toBe('h1');
  });

  it('renders with custom service name', () => {
    renderWithRouter(<HeroSection serviceName="Custom Service" />);

    const headline = screen.getByTestId('hero-headline');
    expect(headline).toHaveTextContent('Custom Service');
  });

  /**
   * Test Case 2: Tagline/subheadline explaining value proposition is visible
   */
  it('displays tagline/subheadline explaining value proposition', () => {
    renderWithRouter(<HeroSection />);

    const tagline = screen.getByTestId('hero-tagline');
    expect(tagline).toBeInTheDocument();
    expect(tagline).toHaveTextContent('Shorten URLs, track clicks, analyze your audience');
  });

  it('renders with custom tagline', () => {
    renderWithRouter(<HeroSection tagline="Custom tagline text" />);

    const tagline = screen.getByTestId('hero-tagline');
    expect(tagline).toHaveTextContent('Custom tagline text');
  });

  /**
   * Test Case 3: Get Started button is rendered with prominent styling
   */
  it('renders Get Started button with prominent/primary styling', () => {
    renderWithRouter(<HeroSection />);

    const getStartedButton = screen.getByTestId('get-started-button');
    expect(getStartedButton).toBeInTheDocument();
    expect(getStartedButton).toHaveTextContent('Get Started');

    // Check that it has primary styling (btn-primary class)
    expect(getStartedButton.className).toContain('btn-primary');

    // Check that it links to /register
    const getStartedLink = screen.getByTestId('get-started-link');
    expect(getStartedLink).toHaveAttribute('href', '/register');
  });

  /**
   * Test Case 4: Sign In button/link is rendered with secondary styling
   */
  it('renders Sign In button with secondary/outline styling', () => {
    renderWithRouter(<HeroSection />);

    const signInButton = screen.getByTestId('sign-in-button');
    expect(signInButton).toBeInTheDocument();
    expect(signInButton).toHaveTextContent('Sign In');

    // Check that it has secondary/outline styling (btn-outline class)
    expect(signInButton.className).toContain('btn-outline');

    // Check that it links to /login
    const signInLink = screen.getByTestId('sign-in-link');
    expect(signInLink).toHaveAttribute('href', '/login');
  });

  it('renders both CTA buttons in the same container', () => {
    renderWithRouter(<HeroSection />);

    const ctaContainer = screen.getByTestId('hero-cta-buttons');
    expect(ctaContainer).toBeInTheDocument();

    // Both buttons should be children of the CTA container
    const getStartedLink = screen.getByTestId('get-started-link');
    const signInLink = screen.getByTestId('sign-in-link');

    expect(ctaContainer).toContainElement(getStartedLink);
    expect(ctaContainer).toContainElement(signInLink);
  });

  it('has proper accessibility attributes', () => {
    renderWithRouter(<HeroSection />);

    // Hero section should have role="banner"
    const heroSection = screen.getByTestId('hero-section');
    expect(heroSection).toHaveAttribute('role', 'banner');
    expect(heroSection).toHaveAttribute('aria-label', 'Hero section');

    // Buttons should have aria-labels
    const getStartedButton = screen.getByTestId('get-started-button');
    const signInButton = screen.getByTestId('sign-in-button');

    expect(getStartedButton).toHaveAttribute('aria-label');
    expect(signInButton).toHaveAttribute('aria-label');
  });
});
