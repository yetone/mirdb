/**
 * FuturisticButton component tests.
 * Owner: Scenario 14 - FuturisticButton Component Usage
 *
 * Test coverage:
 * - Primary variant styling
 * - Outline variant styling
 * - Hover scale animation (105%)
 * - Focus ring visibility
 * - Loading state with spinner
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { BrowserRouter } from 'react-router-dom';
import { FuturisticButton } from '@/components/common/FuturisticButton';

// Helper to wrap component with BrowserRouter for Link functionality
const renderButton = (ui: React.ReactElement) => {
  return render(<BrowserRouter>{ui}</BrowserRouter>);
};

describe('FuturisticButton', () => {
  describe('Test Case 1: Primary variant styling', () => {
    it('renders with primary background color and styling when variant="primary"', () => {
      renderButton(
        <FuturisticButton variant="primary" data-testid="primary-btn">
          Get Started
        </FuturisticButton>
      );

      const button = screen.getByTestId('primary-btn');
      expect(button).toBeInTheDocument();
      expect(button).toHaveClass('btn-primary');
      expect(button).toHaveClass('btn');
    });

    it('uses primary variant by default when no variant is specified', () => {
      renderButton(
        <FuturisticButton data-testid="default-btn">
          Default Button
        </FuturisticButton>
      );

      const button = screen.getByTestId('default-btn');
      expect(button).toHaveClass('btn-primary');
    });
  });

  describe('Test Case 2: Outline variant styling', () => {
    it('renders with border-based styling and transparent background when variant="outline"', () => {
      renderButton(
        <FuturisticButton variant="outline" data-testid="outline-btn">
          Sign In
        </FuturisticButton>
      );

      const button = screen.getByTestId('outline-btn');
      expect(button).toBeInTheDocument();
      expect(button).toHaveClass('btn-outline');
      expect(button).toHaveClass('btn');
      expect(button).not.toHaveClass('btn-primary');
    });
  });

  describe('Test Case 3: Hover scale animation', () => {
    it('has hover:scale-105 class for 105% scale on hover', () => {
      renderButton(
        <FuturisticButton data-testid="hover-btn">
          Hover Me
        </FuturisticButton>
      );

      const button = screen.getByTestId('hover-btn');
      expect(button).toHaveClass('hover:scale-105');
    });

    it('has transition-transform class for smooth animation', () => {
      renderButton(
        <FuturisticButton data-testid="transition-btn">
          Animated
        </FuturisticButton>
      );

      const button = screen.getByTestId('transition-btn');
      expect(button).toHaveClass('transition-transform');
      expect(button).toHaveClass('duration-200');
    });
  });

  describe('Test Case 4: Focus ring visibility', () => {
    it('has ring-2 focus indicator classes for keyboard focus', () => {
      renderButton(
        <FuturisticButton data-testid="focus-btn">
          Focus Me
        </FuturisticButton>
      );

      const button = screen.getByTestId('focus-btn');
      expect(button).toHaveClass('focus:ring-2');
      expect(button).toHaveClass('focus:ring-offset-2');
    });
  });

  describe('Test Case 5: Loading state', () => {
    it('shows loading spinner when loading=true', () => {
      renderButton(
        <FuturisticButton loading={true} data-testid="loading-btn">
          Loading
        </FuturisticButton>
      );

      const button = screen.getByTestId('loading-btn');
      const spinner = button.querySelector('.loading.loading-spinner');
      expect(spinner).toBeInTheDocument();
    });

    it('is disabled when loading=true', () => {
      renderButton(
        <FuturisticButton loading={true} data-testid="loading-disabled-btn">
          Submitting
        </FuturisticButton>
      );

      const button = screen.getByTestId('loading-disabled-btn');
      expect(button).toBeDisabled();
    });

    it('does not show spinner when loading=false', () => {
      renderButton(
        <FuturisticButton loading={false} data-testid="not-loading-btn">
          Submit
        </FuturisticButton>
      );

      const button = screen.getByTestId('not-loading-btn');
      const spinner = button.querySelector('.loading.loading-spinner');
      expect(spinner).not.toBeInTheDocument();
    });
  });

  describe('Additional functionality', () => {
    it('renders as a Link when "to" prop is provided', () => {
      renderButton(
        <FuturisticButton to="/register" data-testid="link-btn">
          Go to Register
        </FuturisticButton>
      );

      const link = screen.getByTestId('link-btn');
      expect(link).toHaveAttribute('href', '/register');
      expect(link.tagName.toLowerCase()).toBe('a');
    });

    it('renders as a button when no "to" prop is provided', () => {
      renderButton(
        <FuturisticButton data-testid="button-element">
          Click Me
        </FuturisticButton>
      );

      const button = screen.getByTestId('button-element');
      expect(button.tagName.toLowerCase()).toBe('button');
    });

    it('passes through additional props', () => {
      const handleClick = vi.fn();
      renderButton(
        <FuturisticButton
          onClick={handleClick}
          aria-label="Custom Label"
          data-testid="props-btn"
        >
          Click
        </FuturisticButton>
      );

      const button = screen.getByTestId('props-btn');
      expect(button).toHaveAttribute('aria-label', 'Custom Label');
    });

    it('supports custom className', () => {
      renderButton(
        <FuturisticButton className="my-custom-class" data-testid="custom-class-btn">
          Custom
        </FuturisticButton>
      );

      const button = screen.getByTestId('custom-class-btn');
      expect(button).toHaveClass('my-custom-class');
    });

    it('has minimum touch-friendly height of 44px', () => {
      renderButton(
        <FuturisticButton data-testid="touch-btn">
          Touch Friendly
        </FuturisticButton>
      );

      const button = screen.getByTestId('touch-btn');
      expect(button).toHaveClass('min-h-[44px]');
    });

    it('can be disabled explicitly', () => {
      renderButton(
        <FuturisticButton disabled data-testid="disabled-btn">
          Disabled
        </FuturisticButton>
      );

      const button = screen.getByTestId('disabled-btn');
      expect(button).toBeDisabled();
    });
  });
});
