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

/**
 * Animation and Visual Effects Tests
 * Owner: Scenario 13 - Animations and Visual Effects
 *
 * Test cases:
 * 1. BackgroundEffect component is rendered and visible on landing page
 * 2. Framer Motion animations are properly configured
 * 3. No animation errors occur during render
 */

describe('HeroSection - Animations and Visual Effects', () => {
  it('renders BackgroundEffect component that is visible in the DOM', () => {
    render(<HeroSection />);

    // BackgroundEffect should render with test id
    const backgroundEffect = screen.getByTestId('background-effect');
    expect(backgroundEffect).toBeInTheDocument();
    expect(backgroundEffect).toBeVisible();
  });

  it('renders motion components for animations', () => {
    const { container } = render(<HeroSection />);

    // Check that the hero section contains animated content
    // With the mock, motion.div renders as regular div
    const heroSection = screen.getByRole('region', { name: /hero section/i });
    expect(heroSection).toBeInTheDocument();

    // Verify the animated container exists
    const contentDiv = container.querySelector('.relative.z-10');
    expect(contentDiv).toBeInTheDocument();
  });

  it('does not produce animation errors in console', () => {
    const consoleSpy = vi.spyOn(console, 'error');

    render(<HeroSection />);

    // Check that no errors were logged during render
    // Filter out React-specific warnings that are not animation related
    const animationErrors = consoleSpy.mock.calls.filter(
      (call) =>
        call.some(
          (arg) =>
            typeof arg === 'string' &&
            (arg.includes('motion') ||
              arg.includes('animation') ||
              arg.includes('framer'))
        )
    );

    expect(animationErrors).toHaveLength(0);

    consoleSpy.mockRestore();
  });

  it('renders hero section with proper z-index layering for BackgroundEffect', () => {
    const { container } = render(<HeroSection />);

    // BackgroundEffect should be behind the content (positioned absolute)
    const backgroundEffect = screen.getByTestId('background-effect');
    expect(backgroundEffect).toBeInTheDocument();

    // Content should have relative z-index to appear above background
    const content = container.querySelector('.relative.z-10');
    expect(content).toBeInTheDocument();
  });
});
