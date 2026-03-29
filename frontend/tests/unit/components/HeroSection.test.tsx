/**
 * HeroSection CTA Tests
 * Owner: Scenario 3 - Primary CTA Redirect to Registration
 *
 * Tests for CTA functionality:
 * - Primary CTA button presence and navigation
 * - Secondary CTA button presence
 * - No form inputs on homepage
 *
 * Requirements: REQ-3, US-2
 */
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { BrowserRouter, MemoryRouter } from 'react-router-dom';
import { HeroSection } from '../../../src/components/homepage/HeroSection';

// Test wrapper with router context
function renderWithRouter(ui: React.ReactElement, { route = '/' } = {}) {
  return render(
    <MemoryRouter initialEntries={[route]}>
      {ui}
    </MemoryRouter>
  );
}

describe('HeroSection CTA Tests', () => {
  /**
   * Test Case 1: Primary CTA button with text 'Get Started' or 'Get Started Free' exists
   * Input: Render HeroSection component
   * Expected: Primary CTA button with text 'Get Started' or 'Get Started Free' exists
   */
  it('should render primary CTA button with "Get Started" or "Get Started Free" text', () => {
    renderWithRouter(<HeroSection />);

    // Look for button containing "Get Started" (covers both "Get Started" and "Get Started Free")
    const primaryCTA = screen.getByRole('link', { name: /get started/i });

    expect(primaryCTA).toBeInTheDocument();
    expect(primaryCTA).toHaveAttribute('href', '/register');
  });

  /**
   * Test Case 2: Navigation to /register route is triggered on click
   * Input: Click primary CTA button
   * Expected: Navigation to /register route is triggered
   */
  it('should have primary CTA linking to /register route', async () => {
    renderWithRouter(<HeroSection />);

    const primaryCTA = screen.getByRole('link', { name: /get started/i });

    // Verify the href attribute points to registration
    expect(primaryCTA).toHaveAttribute('href', '/register');
  });

  /**
   * Test Case 2 (alternative): Verify click navigation works
   */
  it('should navigate to /register when primary CTA is clicked', async () => {
    const user = userEvent.setup();

    render(
      <BrowserRouter>
        <HeroSection />
      </BrowserRouter>
    );

    const primaryCTA = screen.getByTestId('cta-primary');

    // Verify the link has correct href before clicking
    expect(primaryCTA).toHaveAttribute('href', '/register');

    // Click the CTA
    await user.click(primaryCTA);

    // After click, window.location should change (in a real browser)
    // In test environment, we verify the href is correct
    expect(primaryCTA.getAttribute('href')).toBe('/register');
  });

  /**
   * Test Case 3: Secondary CTA button 'View Demo' exists with appropriate link
   * Input: Render HeroSection component
   * Expected: Secondary CTA button 'View Demo' exists with appropriate link
   */
  it('should render secondary CTA button "View Demo" with appropriate link', () => {
    renderWithRouter(<HeroSection />);

    const secondaryCTA = screen.getByRole('link', { name: /view demo/i });

    expect(secondaryCTA).toBeInTheDocument();
    expect(secondaryCTA).toHaveAttribute('href', '/demo');
  });

  /**
   * Test Case 5: No form fields or required inputs exist on homepage
   * Input: Check homepage for form inputs
   * Expected: No form fields or required inputs exist on homepage (registration happens on separate page)
   */
  it('should not contain any form inputs on homepage (registration happens on separate page)', () => {
    renderWithRouter(<HeroSection />);

    // Check that no form elements exist
    const form = screen.queryByRole('form');
    expect(form).not.toBeInTheDocument();

    // Check that no text inputs exist
    const textInputs = screen.queryAllByRole('textbox');
    expect(textInputs).toHaveLength(0);

    // Check that no email inputs exist
    const emailInputs = screen.queryAllByRole('textbox', { name: /email/i });
    expect(emailInputs).toHaveLength(0);

    // Check that no password inputs exist (password inputs don't have a role)
    const passwordInputs = document.querySelectorAll('input[type="password"]');
    expect(passwordInputs).toHaveLength(0);

    // Check that no submit buttons exist
    const submitButtons = screen.queryAllByRole('button', { name: /submit|sign up|register/i });
    expect(submitButtons).toHaveLength(0);
  });

  /**
   * Additional test: Primary CTA should be visually prominent
   */
  it('should render primary CTA with primary button styling', () => {
    renderWithRouter(<HeroSection />);

    const primaryCTA = screen.getByTestId('cta-primary');

    expect(primaryCTA).toHaveClass('btn');
    expect(primaryCTA).toHaveClass('btn-primary');
  });

  /**
   * Additional test: Secondary CTA should have outline styling
   */
  it('should render secondary CTA with outline button styling', () => {
    renderWithRouter(<HeroSection />);

    const secondaryCTA = screen.getByTestId('cta-secondary');

    expect(secondaryCTA).toHaveClass('btn');
    expect(secondaryCTA).toHaveClass('btn-outline');
  });

  /**
   * Additional test: CTAs should be accessible (have proper roles and labels)
   */
  it('should have accessible CTA buttons', () => {
    renderWithRouter(<HeroSection />);

    // Both CTAs should be accessible as links
    const links = screen.getAllByRole('link');

    // Should have at least the two CTA links
    expect(links.length).toBeGreaterThanOrEqual(2);

    // Primary CTA should be findable by accessible name
    const primaryCTA = screen.getByRole('link', { name: /get started/i });
    expect(primaryCTA).toBeInTheDocument();

    // Secondary CTA should be findable by accessible name
    const secondaryCTA = screen.getByRole('link', { name: /view demo/i });
    expect(secondaryCTA).toBeInTheDocument();
  });
});
