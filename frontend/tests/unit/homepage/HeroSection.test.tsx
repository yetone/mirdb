/**
 * HeroSection Unit Tests
 * Scenario 1 - Hero Section Display
 *
 * Tests for the hero section component verifying:
 * - Component renders without errors
 * - Headline element exists with URL shortening text
 * - Subheadline element exists explaining benefits
 * - Primary CTA button exists with call-to-action text
 * - Secondary CTA link exists for sign in
 * - Uses FuturisticButton component for styling
 * - Navigation triggers correctly on CTA clicks
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import HeroSection from '../../../src/components/homepage/HeroSection';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    h1: ({ children, ...props }: React.HTMLAttributes<HTMLHeadingElement>) => <h1 {...props}>{children}</h1>,
    p: ({ children, ...props }: React.HTMLAttributes<HTMLParagraphElement>) => <p {...props}>{children}</p>,
    div: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => <div {...props}>{children}</div>,
    button: ({ children, ...props }: React.ButtonHTMLAttributes<HTMLButtonElement>) => <button {...props}>{children}</button>,
  },
}));

const renderWithRouter = (component: React.ReactNode) => {
  return render(<BrowserRouter>{component}</BrowserRouter>);
};

describe('HeroSection', () => {
  // Test Case 1: Component renders without errors
  it('renders without errors', () => {
    const { container } = renderWithRouter(<HeroSection />);
    expect(container).toBeInTheDocument();
    expect(screen.getByTestId('hero-section')).toBeInTheDocument();
  });

  // Test Case 2: Check for headline element with text content
  it('displays H1 headline with URL shortening text', () => {
    renderWithRouter(<HeroSection />);

    const headline = screen.getByTestId('hero-headline');
    expect(headline).toBeInTheDocument();
    expect(headline.tagName).toBe('H1');
    expect(headline).toHaveTextContent(/shorten/i);
    expect(headline).toHaveTextContent(/url/i);
  });

  // Test Case 3: Check for subheadline element
  it('displays subheadline explaining service benefits', () => {
    renderWithRouter(<HeroSection />);

    const subheadline = screen.getByTestId('hero-subheadline');
    expect(subheadline).toBeInTheDocument();
    // Subheadline should explain the benefits
    expect(subheadline).toHaveTextContent(/link|analytics|shortened/i);
  });

  // Test Case 4: Check for primary CTA button with "Get Started" or similar text
  it('displays primary CTA button with appropriate text', () => {
    renderWithRouter(<HeroSection />);

    const primaryCTA = screen.getByTestId('hero-primary-cta');
    expect(primaryCTA).toBeInTheDocument();
    expect(primaryCTA).toHaveTextContent(/get started|create account|sign up/i);
  });

  // Test Case 5: Click primary CTA button navigates to /register
  it('primary CTA button links to /register route', async () => {
    renderWithRouter(<HeroSection />);

    const primaryCTA = screen.getByTestId('hero-primary-cta');
    expect(primaryCTA).toHaveAttribute('href', '/register');
  });

  // Test Case 6: Check for secondary CTA link
  it('displays secondary CTA link for existing users to sign in', () => {
    renderWithRouter(<HeroSection />);

    const secondaryCTA = screen.getByTestId('hero-secondary-cta');
    expect(secondaryCTA).toBeInTheDocument();
    expect(secondaryCTA).toHaveTextContent(/sign in|login/i);
  });

  // Test Case 7: Click secondary CTA navigates to /login
  it('secondary CTA link navigates to /login route', async () => {
    renderWithRouter(<HeroSection />);

    const secondaryCTA = screen.getByTestId('hero-secondary-cta');
    expect(secondaryCTA).toHaveAttribute('href', '/login');
  });

  // Test Case 8: Verify hero section uses FuturisticButton component
  it('CTA buttons use FuturisticButton styling', () => {
    renderWithRouter(<HeroSection />);

    const primaryCTA = screen.getByTestId('hero-primary-cta');
    const secondaryCTA = screen.getByTestId('hero-secondary-cta');

    // FuturisticButton applies specific classes
    // Check for some key styling indicators
    expect(primaryCTA).toHaveClass('relative');
    expect(secondaryCTA).toHaveClass('relative');

    // Both should be links (anchor elements rendered by Link component)
    expect(primaryCTA.tagName).toBe('A');
    expect(secondaryCTA.tagName).toBe('A');
  });

  // Additional test: Custom content props work correctly
  it('renders with custom content props', () => {
    const customContent = {
      headline: 'Custom Headline',
      subheadline: 'Custom subheadline text',
      primaryCTA: {
        label: 'Custom Primary',
        href: '/custom-register',
      },
      secondaryCTA: {
        label: 'Custom Secondary',
        href: '/custom-login',
      },
    };

    renderWithRouter(<HeroSection content={customContent} />);

    expect(screen.getByTestId('hero-headline')).toHaveTextContent('Custom Headline');
    expect(screen.getByTestId('hero-subheadline')).toHaveTextContent('Custom subheadline text');
    expect(screen.getByTestId('hero-primary-cta')).toHaveTextContent('Custom Primary');
    expect(screen.getByTestId('hero-secondary-cta')).toHaveTextContent('Custom Secondary');
  });

  // Test: Hero section without secondary CTA
  it('renders without secondary CTA when not provided', () => {
    const contentWithoutSecondary = {
      headline: 'Test Headline',
      subheadline: 'Test subheadline',
      primaryCTA: {
        label: 'Primary Only',
        href: '/register',
      },
    };

    renderWithRouter(<HeroSection content={contentWithoutSecondary} />);

    expect(screen.getByTestId('hero-primary-cta')).toBeInTheDocument();
    expect(screen.queryByTestId('hero-secondary-cta')).not.toBeInTheDocument();
  });
});
