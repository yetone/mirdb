/**
 * Integration Tests for Landing Page Routing
 * Owner: Scenario 1 - Hero Section Display (Test Case 5)
 *
 * Tests:
 * - Primary CTA navigates to /register page
 */
import { describe, it, expect } from 'vitest';
import { screen, fireEvent, waitFor } from '@testing-library/react';
import { render } from './setup.js';
import App from '../../../src/App';

describe('Landing Page Routing Integration', () => {
  it('navigates to /register when primary CTA is clicked', async () => {
    render(<App />, { initialEntries: ['/'] });

    // Verify we're on the home page
    expect(screen.getByText(/shorten links/i)).toBeInTheDocument();

    // Find and click the Get Started button in the hero section
    const getStartedButtons = screen.getAllByRole('button', { name: /get started/i });
    // Click the one in the hero section (first one should be it)
    const heroGetStarted = getStartedButtons.find(btn =>
      btn.getAttribute('aria-label')?.includes('URL shortening')
    ) || getStartedButtons[0];

    fireEvent.click(heroGetStarted);

    // Wait for navigation to /register
    await waitFor(() => {
      expect(screen.getByText(/create account/i)).toBeInTheDocument();
    });
  });

  it('renders the home page at root URL', () => {
    render(<App />, { initialEntries: ['/'] });

    // Verify hero section is rendered
    expect(screen.getByRole('heading', { level: 1 })).toBeInTheDocument();
    expect(screen.getByText(/shorten links/i)).toBeInTheDocument();
  });

  it('renders the register page at /register URL', () => {
    render(<App />, { initialEntries: ['/register'] });

    expect(screen.getByText(/create account/i)).toBeInTheDocument();
  });

  it('renders the login page at /login URL', () => {
    render(<App />, { initialEntries: ['/login'] });

    expect(screen.getByRole('heading', { name: /login/i })).toBeInTheDocument();
  });
});
