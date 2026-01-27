/**
 * Hero Section Component Tests
 * Owner: Scenario 1 - Hero Section Rendering
 *
 * Test cases:
 * 1. Hero section renders with headline containing value proposition text
 * 2. Hero section contains subheadline with supporting text
 * 3. Primary CTA button with text 'Get Started' or 'Create Free Account' is visible
 * 4. Secondary CTA button for Login is visible
 * 5. BackgroundEffect component is rendered in hero section
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '../../test-utils';
import { HeroSection } from '../../../src/components/landing/HeroSection';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <div {...props}>{children}</div>
    ),
    button: ({ children, ...props }: React.PropsWithChildren<Record<string, unknown>>) => (
      <button {...props}>{children}</button>
    ),
  },
}));

// Mock BackgroundEffect component to avoid Three.js issues in tests
vi.mock('../../../src/components/BackgroundEffect', () => ({
  BackgroundEffect: () => (
    <div data-testid="background-effect" aria-hidden="true" />
  ),
  default: () => (
    <div data-testid="background-effect" aria-hidden="true" />
  ),
}));

describe('HeroSection', () => {
  it('renders with headline containing value proposition text', () => {
    render(<HeroSection />);

    // Check for the headline - should contain value proposition about shortening/tracking links
    const headline = screen.getByRole('heading', { level: 1 });
    expect(headline).toBeInTheDocument();
    expect(headline.textContent).toMatch(/shorten|share|track|link/i);
  });

  it('contains subheadline with supporting text', () => {
    render(<HeroSection />);

    // Look for supporting text in a paragraph
    const subheadline = screen.getByText(/create short|memorable|analytics|audience/i);
    expect(subheadline).toBeInTheDocument();
  });

  it('renders primary CTA button with "Get Started" or "Create Free Account" text', () => {
    render(<HeroSection />);

    // Check for primary CTA button
    const primaryCta = screen.getByRole('button', { name: /get started|create free account/i });
    expect(primaryCta).toBeInTheDocument();
    expect(primaryCta).toBeVisible();
  });

  it('renders secondary CTA button for Login', () => {
    render(<HeroSection />);

    // Check for login button
    const loginButton = screen.getByRole('button', { name: /login/i });
    expect(loginButton).toBeInTheDocument();
    expect(loginButton).toBeVisible();
  });

  it('renders BackgroundEffect component in hero section', () => {
    render(<HeroSection />);

    // BackgroundEffect should render with test id
    const backgroundEffect = screen.getByTestId('background-effect');
    expect(backgroundEffect).toBeInTheDocument();
  });

  it('has proper semantic HTML structure with section element', () => {
    render(<HeroSection />);

    // Should have a section element with aria-label
    const section = screen.getByRole('region', { name: /hero section/i });
    expect(section).toBeInTheDocument();
  });

  it('wraps CTA buttons in links for navigation', () => {
    render(<HeroSection />);

    // Check that buttons are wrapped in links
    const registerLink = screen.getByRole('link', { name: /get started|create free account/i });
    expect(registerLink).toHaveAttribute('href', '/register');

    const loginLink = screen.getByRole('link', { name: /login/i });
    expect(loginLink).toHaveAttribute('href', '/login');
  });
});
