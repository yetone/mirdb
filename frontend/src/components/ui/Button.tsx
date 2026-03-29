/**
 * Button Component
 * Owner: Scenario 15 - Theme Consistency with Existing App
 *
 * Reusable button component consistent with existing FuturisticButton:
 * - Primary and secondary variants
 * - Proper hover/focus states
 * - Accessibility attributes
 * - Minimum 44x44px touch target
 *
 * Uses DaisyUI button classes for theme consistency:
 * - btn, btn-primary, btn-secondary, btn-outline, btn-ghost
 * - Supports dark/light mode via DaisyUI theming
 *
 * Requirements: NFR-4
 */
import { ButtonHTMLAttributes, ReactNode, forwardRef } from 'react';
import { clsx } from 'clsx';
import { twMerge } from 'tailwind-merge';

export type ButtonVariant = 'primary' | 'secondary' | 'outline' | 'ghost';
export type ButtonSize = 'sm' | 'md' | 'lg';

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  /** Button variant style */
  variant?: ButtonVariant;
  /** Button size */
  size?: ButtonSize;
  /** Button content */
  children: ReactNode;
  /** Loading state */
  loading?: boolean;
  /** Full width button */
  fullWidth?: boolean;
}

const variantClasses: Record<ButtonVariant, string> = {
  primary: 'btn-primary',
  secondary: 'btn-secondary',
  outline: 'btn-outline',
  ghost: 'btn-ghost',
};

const sizeClasses: Record<ButtonSize, string> = {
  sm: 'btn-sm',
  md: '', // default DaisyUI button size
  lg: 'btn-lg',
};

/**
 * Button component with consistent styling matching the existing app theme.
 * Follows DaisyUI patterns for theme consistency (NFR-4).
 */
export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  (
    {
      variant = 'primary',
      size = 'md',
      children,
      className,
      disabled,
      loading,
      fullWidth,
      type = 'button',
      ...props
    },
    ref
  ) => {
    const buttonClasses = twMerge(
      clsx(
        // Base DaisyUI button class
        'btn',
        // Variant styling
        variantClasses[variant],
        // Size styling
        sizeClasses[size],
        // Loading state
        loading && 'loading',
        // Full width
        fullWidth && 'w-full',
        // Minimum touch target for accessibility (44x44px)
        'min-h-[44px] min-w-[44px]',
        // Transition for hover effects
        'transition-all duration-200'
      ),
      className
    );

    return (
      <button
        ref={ref}
        type={type}
        className={buttonClasses}
        disabled={disabled || loading}
        data-testid="ui-button"
        aria-busy={loading}
        aria-disabled={disabled || loading}
        {...props}
      >
        {children}
      </button>
    );
  }
);

Button.displayName = 'Button';
