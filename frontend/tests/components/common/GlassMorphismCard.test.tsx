/**
 * GlassMorphismCard component tests.
 * Owner: Scenario 15 - GlassMorphismCard Component Usage
 *
 * Test coverage:
 * - Backdrop blur and transparency classes
 * - Border styling
 * - Hover shadow transition
 * - testId prop rendering
 */

import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { GlassMorphismCard } from '@/components/common/GlassMorphismCard';

describe('GlassMorphismCard', () => {
  describe('Test Case 1: Backdrop blur and transparency', () => {
    it('renders with backdrop-blur-md class for frosted glass effect', () => {
      render(
        <GlassMorphismCard testId="blur-card">
          Card content
        </GlassMorphismCard>
      );

      const card = screen.getByTestId('blur-card');
      expect(card).toBeInTheDocument();
      expect(card).toHaveClass('backdrop-blur-md');
    });

    it('renders with semi-transparent background class', () => {
      render(
        <GlassMorphismCard testId="transparent-card">
          Card content
        </GlassMorphismCard>
      );

      const card = screen.getByTestId('transparent-card');
      expect(card).toHaveClass('bg-base-100/50');
    });

    it('has both backdrop-blur and transparency for glass morphism effect', () => {
      render(
        <GlassMorphismCard testId="glass-card">
          Glass effect
        </GlassMorphismCard>
      );

      const card = screen.getByTestId('glass-card');
      expect(card).toHaveClass('backdrop-blur-md');
      expect(card).toHaveClass('bg-base-100/50');
    });
  });

  describe('Test Case 2: Border styling', () => {
    it('renders with border class', () => {
      render(
        <GlassMorphismCard testId="border-card">
          Card with border
        </GlassMorphismCard>
      );

      const card = screen.getByTestId('border-card');
      expect(card).toHaveClass('border');
    });

    it('renders with border-base-content/10 for subtle border', () => {
      render(
        <GlassMorphismCard testId="subtle-border-card">
          Subtle border
        </GlassMorphismCard>
      );

      const card = screen.getByTestId('subtle-border-card');
      expect(card).toHaveClass('border-base-content/10');
    });

    it('has rounded corners with rounded-xl class', () => {
      render(
        <GlassMorphismCard testId="rounded-card">
          Rounded card
        </GlassMorphismCard>
      );

      const card = screen.getByTestId('rounded-card');
      expect(card).toHaveClass('rounded-xl');
    });
  });

  describe('Test Case 3: Hover shadow transition', () => {
    it('has shadow-lg class for default shadow', () => {
      render(
        <GlassMorphismCard testId="shadow-card">
          Shadowed card
        </GlassMorphismCard>
      );

      const card = screen.getByTestId('shadow-card');
      expect(card).toHaveClass('shadow-lg');
    });

    it('has hover:shadow-xl class for enhanced shadow on hover', () => {
      render(
        <GlassMorphismCard testId="hover-shadow-card">
          Hover shadow
        </GlassMorphismCard>
      );

      const card = screen.getByTestId('hover-shadow-card');
      expect(card).toHaveClass('hover:shadow-xl');
    });

    it('has transition-shadow class for smooth shadow animation', () => {
      render(
        <GlassMorphismCard testId="transition-card">
          Smooth transition
        </GlassMorphismCard>
      );

      const card = screen.getByTestId('transition-card');
      expect(card).toHaveClass('transition-shadow');
    });

    it('has duration-300 class for 300ms transition duration', () => {
      render(
        <GlassMorphismCard testId="duration-card">
          300ms duration
        </GlassMorphismCard>
      );

      const card = screen.getByTestId('duration-card');
      expect(card).toHaveClass('duration-300');
    });
  });

  describe('Test Case 4: testId prop rendering', () => {
    it('renders with data-testid attribute matching testId prop', () => {
      render(
        <GlassMorphismCard testId="custom-test-id">
          Test id card
        </GlassMorphismCard>
      );

      const card = screen.getByTestId('custom-test-id');
      expect(card).toBeInTheDocument();
    });

    it('renders with specific testId value passed via prop', () => {
      const testIdValue = 'feature-card-analytics';
      render(
        <GlassMorphismCard testId={testIdValue}>
          Feature card
        </GlassMorphismCard>
      );

      const card = screen.getByTestId(testIdValue);
      expect(card).toHaveAttribute('data-testid', testIdValue);
    });

    it('renders without data-testid when testId prop is not provided', () => {
      const { container } = render(
        <GlassMorphismCard>
          No test id
        </GlassMorphismCard>
      );

      const card = container.firstChild;
      expect(card).not.toHaveAttribute('data-testid');
    });
  });

  describe('Additional functionality', () => {
    it('renders children content correctly', () => {
      render(
        <GlassMorphismCard testId="content-card">
          <h3>Feature Title</h3>
          <p>Feature description text</p>
        </GlassMorphismCard>
      );

      const card = screen.getByTestId('content-card');
      expect(card).toHaveTextContent('Feature Title');
      expect(card).toHaveTextContent('Feature description text');
    });

    it('applies custom className alongside default classes', () => {
      render(
        <GlassMorphismCard testId="custom-class-card" className="my-custom-class">
          Custom class card
        </GlassMorphismCard>
      );

      const card = screen.getByTestId('custom-class-card');
      expect(card).toHaveClass('my-custom-class');
      expect(card).toHaveClass('backdrop-blur-md');
      expect(card).toHaveClass('bg-base-100/50');
    });

    it('has padding with p-6 class', () => {
      render(
        <GlassMorphismCard testId="padded-card">
          Padded content
        </GlassMorphismCard>
      );

      const card = screen.getByTestId('padded-card');
      expect(card).toHaveClass('p-6');
    });

    it('renders as a div element', () => {
      render(
        <GlassMorphismCard testId="div-card">
          Div element
        </GlassMorphismCard>
      );

      const card = screen.getByTestId('div-card');
      expect(card.tagName.toLowerCase()).toBe('div');
    });

    it('can contain complex nested components', () => {
      render(
        <GlassMorphismCard testId="complex-card">
          <div data-testid="icon-container">
            <span>🔗</span>
          </div>
          <h4 data-testid="card-heading">URL Shortening</h4>
          <p data-testid="card-description">Create short, memorable links</p>
        </GlassMorphismCard>
      );

      expect(screen.getByTestId('icon-container')).toBeInTheDocument();
      expect(screen.getByTestId('card-heading')).toHaveTextContent('URL Shortening');
      expect(screen.getByTestId('card-description')).toBeInTheDocument();
    });
  });
});
