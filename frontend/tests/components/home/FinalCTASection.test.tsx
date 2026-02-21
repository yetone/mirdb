/**
 * FinalCTASection component tests.
 * Owner: Scenario 2 - Hero Section Content and CTAs
 *
 * Test coverage:
 * - Final CTA section displays with heading
 * - Sign up button navigates to /register
 * - Sign in button navigates to /login
 * - Buttons have proper accessibility attributes
 */
import { describe, it, expect } from 'vitest';
import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { FinalCTASection } from '@/components/home/FinalCTASection';
import { renderWithProviders } from '../../utils/renderWithProviders';

describe('FinalCTASection', () => {
  it('renders final CTA section with heading', () => {
    renderWithProviders(<FinalCTASection />);

    const section = screen.getByTestId('final-cta-section');
    expect(section).toBeInTheDocument();

    const heading = screen.getByRole('heading', { level: 2 });
    expect(heading).toBeInTheDocument();
    expect(heading.textContent).toMatch(/ready|started|get/i);
  });

  it('renders Sign Up button with link to /register', () => {
    renderWithProviders(<FinalCTASection />);

    const signUpButton = screen.getByTestId('final-cta-signup');
    expect(signUpButton).toBeInTheDocument();
    expect(signUpButton).toHaveAttribute('href', '/register');
  });

  it('renders secondary CTA with link to /login', () => {
    renderWithProviders(<FinalCTASection />);

    const signInButton = screen.getByTestId('final-cta-signin');
    expect(signInButton).toBeInTheDocument();
    expect(signInButton).toHaveAttribute('href', '/login');
  });

  it('CTA buttons have proper ARIA labels', () => {
    renderWithProviders(<FinalCTASection />);

    const signUpButton = screen.getByTestId('final-cta-signup');
    const signInButton = screen.getByTestId('final-cta-signin');

    expect(signUpButton).toHaveAttribute('aria-label');
    expect(signInButton).toHaveAttribute('aria-label');
  });

  it('CTA buttons are keyboard focusable', async () => {
    const user = userEvent.setup();
    renderWithProviders(<FinalCTASection />);

    const signUpButton = screen.getByTestId('final-cta-signup');
    const signInButton = screen.getByTestId('final-cta-signin');

    // Tab to first button
    await user.tab();
    expect(signUpButton).toHaveFocus();

    // Tab to second button
    await user.tab();
    expect(signInButton).toHaveFocus();
  });
});
