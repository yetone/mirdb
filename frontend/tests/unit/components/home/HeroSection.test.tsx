/**
 * HeroSection Unit Tests
 * Owner: Scenario 1 - Hero Section Functionality
 *
 * Test coverage:
 * - Renders headline and description
 * - Displays primary CTA button
 * - Displays secondary CTA button
 * - CTAs link to correct routes
 * - Responsive layout classes applied
 * - Gradient text styling applied
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import userEvent from '@testing-library/user-event';
import { HeroSection } from '@/components/home/HeroSection';

// Helper to render with router
const renderWithRouter = (ui: React.ReactElement, { route = '/' } = {}) => {
  return render(
    <MemoryRouter initialEntries={[route]}>
      {ui}
    </MemoryRouter>
  );
};

describe('HeroSection', () => {
  // Test Case 1: Component renders without throwing errors
  it('renders without throwing errors', () => {
    expect(() => renderWithRouter(<HeroSection />)).not.toThrow();
  });

  // Test Case 2: Headline contains product value proposition text
  it('displays headline with product value proposition', () => {
    renderWithRouter(<HeroSection />);

    const headline = screen.getByRole('heading', { level: 1 });
    expect(headline).toBeInTheDocument();
    // Check for URL shortening related text
    expect(headline.textContent?.toLowerCase()).toMatch(/shorten|url/i);
  });

  // Test Case 3: Description text explains benefits of URL shortening service
  it('displays description explaining benefits', () => {
    renderWithRouter(<HeroSection />);

    // Look for paragraph with description about URL shortening benefits
    const description = screen.getByText(/transform.*links|track.*clicks|manage.*links/i);
    expect(description).toBeInTheDocument();
    expect(description.tagName.toLowerCase()).toBe('p');
  });

  // Test Case 4: Get Started button is present with role='button' or as link element
  it('displays Get Started button', () => {
    renderWithRouter(<HeroSection />);

    // The button has role="button" explicitly, so query by button role
    const getStartedButton = screen.getByRole('button', { name: /get started/i });
    expect(getStartedButton).toBeInTheDocument();
    // Verify it's actually an anchor element (link functionality)
    expect(getStartedButton.tagName.toLowerCase()).toBe('a');
  });

  // Test Case 5: Get Started button navigates to /register
  it('Get Started button links to /register', async () => {
    const user = userEvent.setup();

    render(
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route path="/" element={<HeroSection />} />
          <Route path="/register" element={<div data-testid="register-page">Register</div>} />
        </Routes>
      </MemoryRouter>
    );

    const getStartedButton = screen.getByRole('button', { name: /get started/i });
    expect(getStartedButton).toHaveAttribute('href', '/register');

    await user.click(getStartedButton);
    expect(screen.getByTestId('register-page')).toBeInTheDocument();
  });

  // Test Case 6: Log In button is present and visually distinct from primary
  it('displays Log In button that is visually distinct from primary', () => {
    renderWithRouter(<HeroSection />);

    const loginButton = screen.getByRole('button', { name: /log in/i });
    expect(loginButton).toBeInTheDocument();

    // Should have outline style (secondary) vs filled (primary)
    expect(loginButton.className).toMatch(/btn-outline/);
  });

  // Test Case 7: Log In button navigates to /login
  it('Log In button links to /login', async () => {
    const user = userEvent.setup();

    render(
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route path="/" element={<HeroSection />} />
          <Route path="/login" element={<div data-testid="login-page">Login</div>} />
        </Routes>
      </MemoryRouter>
    );

    const loginButton = screen.getByRole('button', { name: /log in/i });
    expect(loginButton).toHaveAttribute('href', '/login');

    await user.click(loginButton);
    expect(screen.getByTestId('login-page')).toBeInTheDocument();
  });

  // Test Case 8: Layout adjusts to stacked arrangement (mobile classes)
  it('has responsive layout classes for mobile (stacked)', () => {
    renderWithRouter(<HeroSection />);

    // Look for the button container with flex-col for mobile
    const buttonContainer = screen.getByRole('button', { name: /get started/i }).parentElement;
    expect(buttonContainer?.className).toMatch(/flex-col/);
  });

  // Test Case 9: Layout displays side-by-side arrangement on desktop
  it('has responsive layout classes for desktop (side-by-side)', () => {
    renderWithRouter(<HeroSection />);

    // Look for sm:flex-row which enables side-by-side on larger screens
    const buttonContainer = screen.getByRole('button', { name: /get started/i }).parentElement;
    expect(buttonContainer?.className).toMatch(/sm:flex-row/);
  });

  // Test Case 10: Headline uses gradient text styling
  it('headline has gradient CSS classes applied', () => {
    renderWithRouter(<HeroSection />);

    const headline = screen.getByRole('heading', { level: 1 });
    // Check for gradient-related Tailwind classes
    expect(headline.className).toMatch(/bg-gradient-to-r/);
    expect(headline.className).toMatch(/from-primary/);
    expect(headline.className).toMatch(/to-secondary/);
    expect(headline.className).toMatch(/bg-clip-text/);
    expect(headline.className).toMatch(/text-transparent/);
  });

  // Additional tests for accessibility and semantic structure
  it('has proper aria-labelledby on section', () => {
    renderWithRouter(<HeroSection />);

    const section = screen.getByRole('region', { name: /shorten/i });
    expect(section).toHaveAttribute('aria-labelledby', 'hero-headline');
  });

  it('buttons have role attribute for accessibility', () => {
    renderWithRouter(<HeroSection />);

    const getStartedButton = screen.getByRole('button', { name: /get started/i });
    const loginButton = screen.getByRole('button', { name: /log in/i });

    expect(getStartedButton).toHaveAttribute('role', 'button');
    expect(loginButton).toHaveAttribute('role', 'button');
  });

  it('primary CTA has prominent btn-primary styling', () => {
    renderWithRouter(<HeroSection />);

    const getStartedButton = screen.getByRole('button', { name: /get started/i });
    expect(getStartedButton.className).toMatch(/btn-primary/);
    expect(getStartedButton.className).toMatch(/btn-lg/);
  });
});
