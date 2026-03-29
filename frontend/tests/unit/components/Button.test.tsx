/**
 * Button Component Unit Tests
 * Owner: Scenario 15 - Theme Consistency with Existing App
 *
 * Tests for the Button component:
 * - DaisyUI button classes are applied
 * - Variant styling (primary, secondary, outline, ghost)
 * - Size styling (sm, md, lg)
 * - Accessibility attributes
 * - Loading and disabled states
 *
 * Requirements: NFR-4 - Theme consistency with existing app
 */
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, it, expect, vi } from 'vitest';
import { Button } from '@/components/ui/Button';

describe('Button', () => {
  /**
   * Test Case 3: CTA buttons use FuturisticButton component or consistent styling
   * Input: Render CTA buttons
   * Expected: CTA buttons use FuturisticButton component or consistent styling
   */
  describe('DaisyUI Theme Consistency (NFR-4)', () => {
    it('should apply DaisyUI btn class for theme consistency', () => {
      render(<Button>Click me</Button>);

      const button = screen.getByTestId('ui-button');
      expect(button).toHaveClass('btn');
    });

    it('should apply btn-primary class for primary variant', () => {
      render(<Button variant="primary">Primary</Button>);

      const button = screen.getByTestId('ui-button');
      expect(button).toHaveClass('btn', 'btn-primary');
    });

    it('should apply btn-secondary class for secondary variant', () => {
      render(<Button variant="secondary">Secondary</Button>);

      const button = screen.getByTestId('ui-button');
      expect(button).toHaveClass('btn', 'btn-secondary');
    });

    it('should apply btn-outline class for outline variant', () => {
      render(<Button variant="outline">Outline</Button>);

      const button = screen.getByTestId('ui-button');
      expect(button).toHaveClass('btn', 'btn-outline');
    });

    it('should apply btn-ghost class for ghost variant', () => {
      render(<Button variant="ghost">Ghost</Button>);

      const button = screen.getByTestId('ui-button');
      expect(button).toHaveClass('btn', 'btn-ghost');
    });

    it('should have transition classes for consistent hover effects', () => {
      render(<Button>Click me</Button>);

      const button = screen.getByTestId('ui-button');
      expect(button).toHaveClass('transition-all', 'duration-200');
    });
  });

  describe('Size Variants', () => {
    it('should apply btn-sm class for small size', () => {
      render(<Button size="sm">Small</Button>);

      const button = screen.getByTestId('ui-button');
      expect(button).toHaveClass('btn-sm');
    });

    it('should not add size class for medium (default) size', () => {
      render(<Button size="md">Medium</Button>);

      const button = screen.getByTestId('ui-button');
      expect(button).toHaveClass('btn');
      expect(button).not.toHaveClass('btn-sm');
      expect(button).not.toHaveClass('btn-lg');
    });

    it('should apply btn-lg class for large size', () => {
      render(<Button size="lg">Large</Button>);

      const button = screen.getByTestId('ui-button');
      expect(button).toHaveClass('btn-lg');
    });
  });

  describe('Accessibility (WCAG 2.1 AA)', () => {
    it('should have minimum touch target size (44x44px)', () => {
      render(<Button>Accessible</Button>);

      const button = screen.getByTestId('ui-button');
      expect(button).toHaveClass('min-h-[44px]', 'min-w-[44px]');
    });

    it('should have proper button role by default', () => {
      render(<Button>Click me</Button>);

      const button = screen.getByRole('button', { name: /click me/i });
      expect(button).toBeInTheDocument();
    });

    it('should set type="button" by default to prevent form submission', () => {
      render(<Button>Click me</Button>);

      const button = screen.getByTestId('ui-button');
      expect(button).toHaveAttribute('type', 'button');
    });

    it('should allow type override', () => {
      render(<Button type="submit">Submit</Button>);

      const button = screen.getByTestId('ui-button');
      expect(button).toHaveAttribute('type', 'submit');
    });

    it('should be focusable', () => {
      render(<Button>Focus me</Button>);

      const button = screen.getByTestId('ui-button');
      button.focus();
      expect(button).toHaveFocus();
    });
  });

  describe('States', () => {
    it('should apply disabled state correctly', () => {
      render(<Button disabled>Disabled</Button>);

      const button = screen.getByTestId('ui-button');
      expect(button).toBeDisabled();
      expect(button).toHaveAttribute('aria-disabled', 'true');
    });

    it('should apply loading state correctly', () => {
      render(<Button loading>Loading</Button>);

      const button = screen.getByTestId('ui-button');
      expect(button).toBeDisabled();
      expect(button).toHaveAttribute('aria-busy', 'true');
      expect(button).toHaveClass('loading');
    });

    it('should apply full width when specified', () => {
      render(<Button fullWidth>Full Width</Button>);

      const button = screen.getByTestId('ui-button');
      expect(button).toHaveClass('w-full');
    });
  });

  describe('Custom Props', () => {
    it('should merge custom className', () => {
      render(<Button className="custom-class">Custom</Button>);

      const button = screen.getByTestId('ui-button');
      expect(button).toHaveClass('btn', 'custom-class');
    });

    it('should forward onClick handler', async () => {
      const user = userEvent.setup();
      const handleClick = vi.fn();

      render(<Button onClick={handleClick}>Click me</Button>);

      await user.click(screen.getByTestId('ui-button'));
      expect(handleClick).toHaveBeenCalledTimes(1);
    });

    it('should not call onClick when disabled', async () => {
      const user = userEvent.setup();
      const handleClick = vi.fn();

      render(<Button onClick={handleClick} disabled>Disabled</Button>);

      await user.click(screen.getByTestId('ui-button'));
      expect(handleClick).not.toHaveBeenCalled();
    });

    it('should spread additional props to button element', () => {
      render(<Button data-custom="value">Custom Data</Button>);

      const button = screen.getByTestId('ui-button');
      expect(button).toHaveAttribute('data-custom', 'value');
    });
  });

  describe('Rendering', () => {
    it('should render children correctly', () => {
      render(
        <Button>
          <span data-testid="child-icon">Icon</span>
          Click me
        </Button>
      );

      expect(screen.getByTestId('child-icon')).toBeInTheDocument();
      expect(screen.getByText(/Click me/i)).toBeInTheDocument();
    });

    it('should render as button element', () => {
      render(<Button>Button</Button>);

      const button = screen.getByTestId('ui-button');
      expect(button.tagName).toBe('BUTTON');
    });
  });
});
