/**
 * Unit tests for RegistrationPrompt component
 * Owner: Scenario 11 - Post-Shortening Registration Prompt
 *
 * Tests the registration prompt that appears after anonymous URL shortening.
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { RegistrationPrompt } from '../../../src/components/landing/RegistrationPrompt';
import { REGISTRATION_PROMPT_CONTENT } from '../../../src/constants/landingContent';

// Wrapper component for router context
function renderWithRouter(ui: React.ReactElement) {
  return render(<BrowserRouter>{ui}</BrowserRouter>);
}

describe('RegistrationPrompt', () => {
  describe('Initial render', () => {
    it('should render the registration prompt container', () => {
      renderWithRouter(<RegistrationPrompt />);

      const prompt = screen.getByTestId('registration-prompt');
      expect(prompt).toBeInTheDocument();
    });

    it('should have proper accessibility attributes', () => {
      renderWithRouter(<RegistrationPrompt />);

      const prompt = screen.getByTestId('registration-prompt');
      expect(prompt).toHaveAttribute('role', 'complementary');
      expect(prompt).toHaveAttribute('aria-label', 'Registration prompt');
    });
  });

  describe('Message content', () => {
    it('should display message about analytics benefits', () => {
      renderWithRouter(<RegistrationPrompt />);

      const message = screen.getByTestId('registration-prompt-message');
      expect(message).toBeInTheDocument();
      expect(message).toHaveTextContent(REGISTRATION_PROMPT_CONTENT.message);
    });

    it('should contain text about tracking clicks and analytics', () => {
      renderWithRouter(<RegistrationPrompt />);

      const message = screen.getByTestId('registration-prompt-message');
      // Test case 2: Shows 'Create an account to track clicks and view detailed analytics' or similar
      expect(message.textContent?.toLowerCase()).toContain('track');
      expect(message.textContent?.toLowerCase()).toContain('analytics');
    });
  });

  describe('Sign Up button', () => {
    it('should display Sign Up button', () => {
      renderWithRouter(<RegistrationPrompt />);

      const button = screen.getByTestId('registration-prompt-signup-button');
      expect(button).toBeInTheDocument();
      expect(button).toHaveTextContent(REGISTRATION_PROMPT_CONTENT.buttonText);
    });

    it('should link to /register route', () => {
      renderWithRouter(<RegistrationPrompt />);

      const button = screen.getByTestId('registration-prompt-signup-button');
      expect(button).toHaveAttribute('href', '/register');
    });

    it('should have role of link for navigation', () => {
      renderWithRouter(<RegistrationPrompt />);

      const link = screen.getByRole('link', { name: /sign up/i });
      expect(link).toBeInTheDocument();
      expect(link).toHaveAttribute('href', '/register');
    });
  });

  describe('Visual styling', () => {
    it('should have non-blocking styling with background', () => {
      renderWithRouter(<RegistrationPrompt />);

      const prompt = screen.getByTestId('registration-prompt');
      // Should have styling that makes it visually distinct but not blocking
      expect(prompt.className).toMatch(/bg-|rounded/);
    });

    it('should have proper padding and spacing', () => {
      renderWithRouter(<RegistrationPrompt />);

      const prompt = screen.getByTestId('registration-prompt');
      expect(prompt.className).toMatch(/p-|mt-/);
    });
  });

  describe('Accessibility', () => {
    it('should use semantic complementary role', () => {
      renderWithRouter(<RegistrationPrompt />);

      const prompt = screen.getByRole('complementary');
      expect(prompt).toBeInTheDocument();
    });

    it('should be navigable via keyboard (link is focusable)', () => {
      renderWithRouter(<RegistrationPrompt />);

      const link = screen.getByRole('link', { name: /sign up/i });
      // Links are inherently focusable
      expect(link.tagName).toBe('A');
    });
  });
});
