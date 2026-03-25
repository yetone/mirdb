/**
 * CTASection Tests
 * Owner: Scenario 15 - Final CTA Section
 *
 * Tests for the final CTA section component:
 * - Section rendering and positioning
 * - Compelling headline display
 * - Navigation behavior for unauthenticated users
 * - Adaptive behavior for authenticated users
 */

import React from 'react';
import { screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, it, expect } from 'vitest';
import { CTASection } from '../../../src/components/homepage/CTASection';
import { Home } from '../../../src/pages/Home';
import { renderWithProviders } from './test-utils';

describe('CTASection', () => {
  describe('Unit Tests', () => {
    it('should render the CTA section', () => {
      renderWithProviders(<CTASection />);

      const ctaSection = screen.getByTestId('cta-section');
      expect(ctaSection).toBeInTheDocument();
    });

    it('should display a compelling headline', () => {
      renderWithProviders(<CTASection />);

      const headline = screen.getByTestId('cta-headline');
      expect(headline).toBeInTheDocument();
      expect(headline).toHaveTextContent('Ready to Supercharge Your Links?');
    });

    it('should have accessible section labeling', () => {
      renderWithProviders(<CTASection />);

      const section = screen.getByRole('region', { name: /call to action/i });
      expect(section).toBeInTheDocument();
    });

    it('should display supporting description text', () => {
      renderWithProviders(<CTASection />);

      expect(screen.getByText(/join thousands of users/i)).toBeInTheDocument();
    });
  });

  describe('CTA Section Position in HomePage', () => {
    it('should render CTA section above footer in HomePage', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
      });

      const ctaSection = screen.getByTestId('cta-section');
      const footer = screen.getByRole('contentinfo');

      expect(ctaSection).toBeInTheDocument();
      expect(footer).toBeInTheDocument();

      // Verify CTA comes before footer in DOM order
      const main = screen.getByRole('main');
      const ctaIndex = Array.from(main.querySelectorAll('[data-testid="cta-section"], footer'))
        .findIndex(el => el.getAttribute('data-testid') === 'cta-section');
      const footerIndex = Array.from(main.querySelectorAll('[data-testid="cta-section"], footer'))
        .findIndex(el => el.tagName.toLowerCase() === 'footer');

      expect(ctaIndex).toBeLessThan(footerIndex);
    });
  });

  describe('Unauthenticated User Navigation', () => {
    it('should show register button for unauthenticated users', () => {
      renderWithProviders(<CTASection isAuthenticated={false} />);

      const registerButton = screen.getByTestId('cta-register-button');
      expect(registerButton).toBeInTheDocument();
      expect(registerButton).toHaveTextContent('Get Started Free');
    });

    it('should navigate to /register when unauthenticated user clicks CTA', async () => {
      const user = userEvent.setup();

      renderWithProviders(<CTASection isAuthenticated={false} />, {
        useMemoryRouter: true,
        initialRoute: '/',
      });

      const registerButton = screen.getByTestId('cta-register-button');
      expect(registerButton).toHaveAttribute('href', '/register');

      await user.click(registerButton);
      // Since FuturisticButton uses Link, the href attribute confirms navigation target
    });

    it('should not show dashboard button for unauthenticated users', () => {
      renderWithProviders(<CTASection isAuthenticated={false} />);

      const dashboardButton = screen.queryByTestId('cta-dashboard-button');
      expect(dashboardButton).not.toBeInTheDocument();
    });
  });

  describe('Authenticated User Adaptation', () => {
    it('should show dashboard button for authenticated users', () => {
      renderWithProviders(<CTASection isAuthenticated={true} />);

      const dashboardButton = screen.getByTestId('cta-dashboard-button');
      expect(dashboardButton).toBeInTheDocument();
      expect(dashboardButton).toHaveTextContent('Go to Dashboard');
    });

    it('should not show register button for authenticated users', () => {
      renderWithProviders(<CTASection isAuthenticated={true} />);

      const registerButton = screen.queryByTestId('cta-register-button');
      expect(registerButton).not.toBeInTheDocument();
    });

    it('should navigate to /dashboard when authenticated user clicks CTA', async () => {
      const user = userEvent.setup();

      renderWithProviders(<CTASection isAuthenticated={true} />, {
        useMemoryRouter: true,
        initialRoute: '/',
      });

      const dashboardButton = screen.getByTestId('cta-dashboard-button');
      expect(dashboardButton).toHaveAttribute('href', '/dashboard');

      await user.click(dashboardButton);
    });
  });

  describe('Integration with Home Page', () => {
    it('should render CTA with correct auth state from HomePage context (unauthenticated)', () => {
      renderWithProviders(<Home />, {
        authOptions: { isAuthenticated: false },
      });

      const registerButton = screen.getByTestId('cta-register-button');
      expect(registerButton).toBeInTheDocument();
    });

    it('should render CTA with correct auth state from HomePage context (authenticated)', () => {
      renderWithProviders(<Home />, {
        authOptions: {
          isAuthenticated: true,
          user: { id: '1', username: 'testuser', email: 'test@example.com' },
        },
      });

      const dashboardButton = screen.getByTestId('cta-dashboard-button');
      expect(dashboardButton).toBeInTheDocument();
    });
  });
});
