/**
 * Navigation Integration Tests
 * Owner: Scenario 2 - Navigation & Routing (shared with Scenario 1 for CTA navigation test)
 *
 * Test case 4 from Scenario 1:
 * - Click primary CTA button -> navigates to /register route
 */

import { describe, it, expect } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import HeroSection from '../../../src/components/homepage/HeroSection';

describe('HeroSection Navigation Integration', () => {
  // Test Case 4: CTA button navigates to /register route
  it('navigates to /register when primary CTA is clicked', async () => {
    let currentPath = '/';

    const TestApp = () => (
      <MemoryRouter initialEntries={['/']}>
        <Routes>
          <Route
            path="/"
            element={<HeroSection />}
          />
          <Route
            path="/register"
            element={<div data-testid="register-page">Register Page</div>}
          />
        </Routes>
      </MemoryRouter>
    );

    render(<TestApp />);

    // Verify we're on the hero section
    expect(screen.getByTestId('hero-section')).toBeInTheDocument();

    // Click the primary CTA button
    const ctaButton = screen.getByTestId('hero-cta-primary');
    fireEvent.click(ctaButton);

    // Wait for navigation to /register
    await waitFor(() => {
      expect(screen.getByTestId('register-page')).toBeInTheDocument();
    });
  });
});
