/**
 * CTASection Unit Tests
 * Scenario 4 - Secondary CTA Section
 *
 * Tests for the CTA section component verifying:
 * - Component renders without errors
 * - Headline element exists with encouraging text
 * - Sign-up CTA button exists with appropriate text
 * - CTA button links to /register route
 * - Section has visually distinct styling
 *
 * Requirements: REQ-5, US-2
 */

import { describe, it, expect, vi } from 'vitest';
import { render, screen, within } from '@testing-library/react';
import { BrowserRouter, MemoryRouter } from 'react-router-dom';
import userEvent from '@testing-library/user-event';
import { CTASection } from '../../../src/components/homepage/CTASection';

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
      <div {...props}>{children}</div>
    ),
    button: ({ children, ...props }: React.ButtonHTMLAttributes<HTMLButtonElement>) => (
      <button {...props}>{children}</button>
    ),
  },
}));

const renderWithRouter = (component: React.ReactNode) => {
  return render(<BrowserRouter>{component}</BrowserRouter>);
};

describe('CTASection', () => {
  // Test Case 1: Component renders without errors
  it('renders without errors', () => {
    const { container } = renderWithRouter(<CTASection />);
    expect(container).toBeInTheDocument();
    expect(screen.getByTestId('cta-section')).toBeInTheDocument();
  });

  // Test Case 2: Check for CTA headline text
  it('displays headline with encouraging text', () => {
    renderWithRouter(<CTASection />);

    const headline = screen.getByTestId('cta-headline');
    expect(headline).toBeInTheDocument();
    expect(headline.tagName).toBe('H2');
    // Default text is "Ready to get started?"
    expect(headline).toHaveTextContent(/ready|get started|start/i);
  });

  // Test Case 3: Check for sign-up CTA button
  it('displays sign-up CTA button with appropriate text', () => {
    renderWithRouter(<CTASection />);

    const ctaButtonLink = screen.getByTestId('cta-button');
    expect(ctaButtonLink).toBeInTheDocument();
    // Button should have sign-up related text
    expect(ctaButtonLink).toHaveTextContent(/create|sign up|get started|account/i);
  });

  // Test Case 4: Click CTA button navigates to /register
  it('CTA button links to /register route', () => {
    renderWithRouter(<CTASection />);

    const ctaButtonLink = screen.getByTestId('cta-button');
    expect(ctaButtonLink).toHaveAttribute('href', '/register');
  });

  // Test Case 5: Verify CTA section has visually distinct styling
  it('has visually distinct styling from other sections', () => {
    renderWithRouter(<CTASection />);

    const section = screen.getByTestId('cta-section');
    // Section should have specific styling classes for visual distinction
    expect(section).toHaveClass('py-16');
    expect(section).toHaveClass('relative');

    // Check for the gradient background element
    const gradientBackground = section.querySelector('.bg-gradient-to-b');
    expect(gradientBackground).toBeInTheDocument();
  });

  // Additional test: CTA button uses FuturisticButton styling
  it('CTA button uses FuturisticButton styling', () => {
    renderWithRouter(<CTASection />);

    const ctaButtonLink = screen.getByTestId('cta-button');
    expect(ctaButtonLink.tagName).toBe('A');

    // FuturisticButton is inside the link - check the button has btn class
    const button = within(ctaButtonLink).getByRole('button');
    expect(button).toHaveClass('btn');
  });

  // Additional test: Custom content props work correctly
  it('renders with custom content props', () => {
    const customContent = {
      headline: 'Custom CTA Headline',
      ctaLabel: 'Custom Button Text',
      ctaHref: '/custom-register',
    };

    renderWithRouter(<CTASection content={customContent} />);

    expect(screen.getByTestId('cta-headline')).toHaveTextContent('Custom CTA Headline');
    expect(screen.getByTestId('cta-button')).toHaveTextContent('Custom Button Text');
    expect(screen.getByTestId('cta-button')).toHaveAttribute('href', '/custom-register');
  });

  // Additional test: Description text is present
  it('displays supporting description text', () => {
    renderWithRouter(<CTASection />);

    const description = screen.getByTestId('cta-description');
    expect(description).toBeInTheDocument();
    // Should have encouraging/trust-building text
    expect(description).toHaveTextContent(/trust|join|users|service/i);
  });

  // Additional test: Section is accessible with aria-labelledby
  it('has proper accessibility attributes', () => {
    renderWithRouter(<CTASection />);

    const section = screen.getByTestId('cta-section');
    expect(section).toHaveAttribute('aria-labelledby', 'cta-heading');

    const heading = screen.getByTestId('cta-headline');
    expect(heading).toHaveAttribute('id', 'cta-heading');
  });
});
