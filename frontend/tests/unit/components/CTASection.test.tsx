/**
 * Unit tests for CTASection component
 * Owner: Scenario 10 - Call-to-Action Section
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { CTASection } from '../../../src/components/landing/CTASection';
import { CTA_CONTENT } from '../../../src/constants/landingContent';

// Wrapper component for router context
function renderWithRouter(ui: React.ReactElement) {
  return render(<BrowserRouter>{ui}</BrowserRouter>);
}

describe('CTASection', () => {
  describe('Initial render', () => {
    it('should render section with proper accessibility attributes', () => {
      renderWithRouter(<CTASection />);

      const section = screen.getByTestId('cta-section');
      expect(section).toBeInTheDocument();
      expect(section).toHaveAttribute('aria-labelledby', 'cta-title');
    });

    it('should render headline about unlocking analytics', () => {
      renderWithRouter(<CTASection />);

      const headline = screen.getByTestId('cta-headline');
      expect(headline).toBeInTheDocument();
      expect(headline).toHaveTextContent(CTA_CONTENT.headline);
      expect(headline.textContent?.toLowerCase()).toContain('analytics');
    });

    it('should render description encouraging registration', () => {
      renderWithRouter(<CTASection />);

      const description = screen.getByTestId('cta-description');
      expect(description).toBeInTheDocument();
      expect(description).toHaveTextContent(CTA_CONTENT.description);
    });

    it('should display Sign Up Free button', () => {
      renderWithRouter(<CTASection />);

      const button = screen.getByTestId('cta-signup-button');
      expect(button).toBeInTheDocument();
      expect(button).toHaveTextContent(CTA_CONTENT.buttonText);
    });
  });

  describe('Sign Up button', () => {
    it('should be a link to /register route', () => {
      renderWithRouter(<CTASection />);

      const button = screen.getByTestId('cta-signup-button');
      expect(button).toHaveAttribute('href', '/register');
    });

    it('should have role of link for navigation', () => {
      renderWithRouter(<CTASection />);

      const link = screen.getByRole('link', { name: /sign up free/i });
      expect(link).toBeInTheDocument();
      expect(link).toHaveAttribute('href', '/register');
    });
  });

  describe('Visual styling', () => {
    it('should have visual prominence with background styling', () => {
      renderWithRouter(<CTASection />);

      const section = screen.getByTestId('cta-section');
      // Section should have gradient or prominent background classes
      expect(section.className).toMatch(/bg-|gradient/);
    });

    it('should have proper padding for visual spacing', () => {
      renderWithRouter(<CTASection />);

      const section = screen.getByTestId('cta-section');
      expect(section.className).toMatch(/py-|p-/);
    });
  });

  describe('Accessibility', () => {
    it('should have proper heading hierarchy with h2', () => {
      renderWithRouter(<CTASection />);

      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toHaveTextContent(CTA_CONTENT.headline);
    });

    it('should use semantic section element', () => {
      renderWithRouter(<CTASection />);

      const section = screen.getByTestId('cta-section');
      expect(section.tagName).toBe('SECTION');
    });
  });
});
