/**
 * CTASection Component Unit Tests.
 *
 * Tests for Scenario 7 - Call-to-Action Section:
 * - Renders section with heading, button, and secondary text
 * - Heading displays "Ready to shorten your first link?"
 * - "Create Free Account" button navigates to /register
 * - Section has minimum height of 300px
 */
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { BrowserRouter, MemoryRouter, Routes, Route } from 'react-router-dom';
import { CTASection } from '../../../../src/components/home/CTASection';

// Helper to render with Router context
function renderWithRouter(ui: React.ReactElement) {
  return render(<BrowserRouter>{ui}</BrowserRouter>);
}

describe('CTASection', () => {
  describe('Test Case 1: Section Renders with All Elements', () => {
    it('renders section with heading, primary button, and secondary text', () => {
      renderWithRouter(<CTASection />);

      // Check heading
      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toBeInTheDocument();

      // Check primary button
      const button = screen.getByRole('button', { name: /create free account/i });
      expect(button).toBeInTheDocument();

      // Check secondary text
      const secondaryText = screen.getByText(/start tracking clicks in 30 seconds/i);
      expect(secondaryText).toBeInTheDocument();
    });

    it('section has proper data-testid attribute', () => {
      renderWithRouter(<CTASection />);

      const section = screen.getByTestId('cta-section');
      expect(section).toBeInTheDocument();
    });
  });

  describe('Test Case 2: CTA Heading Text', () => {
    it('heading displays "Ready to shorten your first link?"', () => {
      renderWithRouter(<CTASection />);

      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toHaveTextContent('Ready to shorten your first link?');
    });

    it('heading has proper id for aria-labelledby', () => {
      renderWithRouter(<CTASection />);

      const heading = screen.getByRole('heading', { level: 2 });
      expect(heading).toHaveAttribute('id', 'cta-headline');
    });
  });

  describe('Test Case 3: CTA Button Navigation', () => {
    it('"Create Free Account" button has href to /register', () => {
      renderWithRouter(<CTASection />);

      const button = screen.getByRole('button', { name: /create free account/i });
      expect(button).toHaveAttribute('href', '/register');
    });

    it('button has primary styling', () => {
      renderWithRouter(<CTASection />);

      const button = screen.getByRole('button', { name: /create free account/i });
      expect(button).toHaveClass('btn-primary');
    });

    it('clicking button navigates to /register route', async () => {
      const user = userEvent.setup();

      // Use MemoryRouter to track navigation
      const TestComponent = () => {
        return (
          <MemoryRouter initialEntries={['/']}>
            <Routes>
              <Route path="/" element={<CTASection />} />
              <Route path="/register" element={<div data-testid="register-page">Register Page</div>} />
            </Routes>
          </MemoryRouter>
        );
      };

      render(<TestComponent />);

      // Click the CTA button
      const button = screen.getByRole('button', { name: /create free account/i });
      await user.click(button);

      // Verify navigation occurred
      const registerPage = screen.getByTestId('register-page');
      expect(registerPage).toBeInTheDocument();
    });
  });

  describe('Test Case 4: Section Minimum Height', () => {
    it('section has min-height of 300px', () => {
      renderWithRouter(<CTASection />);

      const section = screen.getByTestId('cta-section');
      expect(section).toHaveStyle({ minHeight: '300px' });
    });

    it('section has cta-section class for CSS styling', () => {
      renderWithRouter(<CTASection />);

      const section = screen.getByTestId('cta-section');
      expect(section).toHaveClass('cta-section');
    });
  });

  describe('Accessibility', () => {
    it('section has proper aria-labelledby attribute', () => {
      renderWithRouter(<CTASection />);

      const section = screen.getByTestId('cta-section');
      expect(section).toHaveAttribute('aria-labelledby', 'cta-headline');
    });

    it('button has role="button" for proper semantics', () => {
      renderWithRouter(<CTASection />);

      const button = screen.getByRole('button', { name: /create free account/i });
      expect(button).toHaveAttribute('role', 'button');
    });
  });

  describe('Secondary Text', () => {
    it('displays "Start tracking clicks in 30 seconds"', () => {
      renderWithRouter(<CTASection />);

      const text = screen.getByText('Start tracking clicks in 30 seconds');
      expect(text).toBeInTheDocument();
    });
  });
});
