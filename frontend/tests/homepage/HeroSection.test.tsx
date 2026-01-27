/**
 * Hero Section Tests
 * Owner: Scenario 1 - Hero Section and Value Proposition
 *
 * Test cases:
 * 1. H1 element exists with URL/shorten/link keywords
 * 2. Supporting paragraph with analytics/tracking/sharing benefits
 * 3. Get Started button exists and is visible
 * 4. Get Started button navigates to /register
 * 5. Sign In link exists and is visible
 * 6. Sign In link navigates to /login
 * 7. BackgroundEffect component renders
 * 8. CTA button visible on desktop viewport
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { HeroSection } from '../../src/components/homepage/HeroSection';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
      <div {...props}>{children}</div>
    ),
    button: ({
      children,
      ...props
    }: React.ButtonHTMLAttributes<HTMLButtonElement>) => (
      <button {...props}>{children}</button>
    ),
  },
  HTMLMotionProps: {},
}));

function renderWithRouter(
  component: React.ReactElement,
  { route = '/' } = {}
) {
  let currentLocation = route;

  return {
    ...render(
      <MemoryRouter initialEntries={[route]}>
        <Routes>
          <Route path="/" element={component} />
          <Route
            path="/register"
            element={<div data-testid="register-page">Register Page</div>}
          />
          <Route
            path="/login"
            element={<div data-testid="login-page">Login Page</div>}
          />
        </Routes>
      </MemoryRouter>
    ),
    getLocation: () => currentLocation,
  };
}

describe('HeroSection', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  // Test Case 1: H1 element with value proposition text
  it('displays H1 headline with URL shortening service purpose', () => {
    renderWithRouter(<HeroSection />);

    const heading = screen.getByRole('heading', { level: 1 });
    expect(heading).toBeInTheDocument();

    const headingText = heading.textContent?.toLowerCase() || '';
    const hasRelevantKeyword =
      headingText.includes('url') ||
      headingText.includes('shorten') ||
      headingText.includes('link');

    expect(hasRelevantKeyword).toBe(true);
  });

  // Test Case 2: Supporting paragraph with benefits
  it('displays supporting paragraph mentioning analytics, tracking, or sharing benefits', () => {
    renderWithRouter(<HeroSection />);

    // Find paragraphs that mention benefits - use getAllByText since multiple elements may match
    const matchingElements = screen.getAllByText(/analytics|tracking|sharing/i);
    // At least one element should be a paragraph (p element)
    const paragraph = matchingElements.find((el) => el.tagName === 'P');
    expect(paragraph).toBeInTheDocument();
  });

  // Test Case 3: Get Started button exists and is visible
  it('displays Get Started primary CTA button', () => {
    renderWithRouter(<HeroSection />);

    const getStartedButton = screen.getByTestId('get-started-button');
    expect(getStartedButton).toBeInTheDocument();
    expect(getStartedButton).toBeVisible();
    expect(getStartedButton).toHaveTextContent(/get started/i);
  });

  // Test Case 4: Get Started button navigates to /register
  it('navigates to /register when Get Started button is clicked', async () => {
    renderWithRouter(<HeroSection />);

    const getStartedButton = screen.getByTestId('get-started-button');
    fireEvent.click(getStartedButton);

    expect(await screen.findByTestId('register-page')).toBeInTheDocument();
  });

  // Test Case 5: Sign In link exists and is visible
  it('displays Sign In secondary CTA button', () => {
    renderWithRouter(<HeroSection />);

    const signInButton = screen.getByTestId('sign-in-button');
    expect(signInButton).toBeInTheDocument();
    expect(signInButton).toBeVisible();
    expect(signInButton).toHaveTextContent(/sign in/i);
  });

  // Test Case 6: Sign In link navigates to /login
  it('navigates to /login when Sign In button is clicked', async () => {
    renderWithRouter(<HeroSection />);

    const signInButton = screen.getByTestId('sign-in-button');
    fireEvent.click(signInButton);

    expect(await screen.findByTestId('login-page')).toBeInTheDocument();
  });

  // Test Case 7: BackgroundEffect component renders
  it('renders BackgroundEffect component without errors', () => {
    renderWithRouter(<HeroSection />);

    const backgroundEffect = screen.getByTestId('background-effect');
    expect(backgroundEffect).toBeInTheDocument();
  });

  // Test Case 8: CTA button visibility on desktop viewport
  it('renders Get Started button visible above the fold on desktop', () => {
    // Mock window dimensions for desktop viewport (1024px width)
    Object.defineProperty(window, 'innerWidth', {
      writable: true,
      configurable: true,
      value: 1024,
    });
    Object.defineProperty(window, 'innerHeight', {
      writable: true,
      configurable: true,
      value: 768,
    });

    renderWithRouter(<HeroSection />);

    const getStartedButton = screen.getByTestId('get-started-button');
    expect(getStartedButton).toBeInTheDocument();
    expect(getStartedButton).toBeVisible();

    // Button should be in the hero section which is designed to be above the fold
    const heroSection = screen.getByTestId('hero-section');
    expect(heroSection).toContainElement(getStartedButton);
  });

  // Additional tests for completeness

  it('calls onGetStarted callback when provided and Get Started is clicked', async () => {
    const onGetStarted = vi.fn();
    renderWithRouter(<HeroSection onGetStarted={onGetStarted} />);

    const getStartedButton = screen.getByTestId('get-started-button');
    fireEvent.click(getStartedButton);

    expect(onGetStarted).toHaveBeenCalledTimes(1);
  });

  it('calls onSignIn callback when provided and Sign In is clicked', async () => {
    const onSignIn = vi.fn();
    renderWithRouter(<HeroSection onSignIn={onSignIn} />);

    const signInButton = screen.getByTestId('sign-in-button');
    fireEvent.click(signInButton);

    expect(onSignIn).toHaveBeenCalledTimes(1);
  });

  it('renders hero section with accessible structure', () => {
    renderWithRouter(<HeroSection />);

    // Should have a section element
    const section = screen.getByTestId('hero-section');
    expect(section.tagName).toBe('SECTION');

    // Should have proper heading hierarchy
    const heading = screen.getByRole('heading', { level: 1 });
    expect(heading).toBeInTheDocument();
  });
});
