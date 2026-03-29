/**
 * Card Component
 * Owner: Scenario 15 - Theme Consistency with Existing App
 *
 * Reusable card component consistent with existing GlassMorphismCard:
 * - Consistent styling with existing app
 * - Hover effects (elevation/border)
 * - Dark/light mode support via DaisyUI theming
 *
 * Uses DaisyUI card classes for theme consistency:
 * - card, card-body, bg-base-200, shadow-md
 * - Supports dark/light mode automatically via DaisyUI theming
 *
 * Requirements: NFR-4
 */
import { HTMLAttributes, ReactNode, forwardRef } from 'react';
import { clsx } from 'clsx';
import { twMerge } from 'tailwind-merge';

export interface CardProps extends HTMLAttributes<HTMLDivElement> {
  /** Card content */
  children: ReactNode;
  /** Enable hover effects (elevation/border highlight) */
  hover?: boolean;
  /** Card variant */
  variant?: 'default' | 'bordered' | 'glass';
  /** Custom class names */
  className?: string;
  /** Padding size */
  padding?: 'none' | 'sm' | 'md' | 'lg';
}

const paddingClasses: Record<NonNullable<CardProps['padding']>, string> = {
  none: '',
  sm: 'p-4',
  md: 'p-6',
  lg: 'p-8',
};

/**
 * Card component with consistent styling matching the existing app theme.
 * Follows DaisyUI patterns for theme consistency (NFR-4).
 * Supports glassmorphism effect via variant="glass".
 */
export const Card = forwardRef<HTMLDivElement, CardProps>(
  (
    {
      children,
      hover = false,
      variant = 'default',
      className,
      padding = 'md',
      ...props
    },
    ref
  ) => {
    const cardClasses = twMerge(
      clsx(
        // Base DaisyUI card class
        'card',
        // Background color using DaisyUI theme variables
        'bg-base-200',
        // Shadow for depth
        'shadow-md',
        // Hover effects when enabled
        hover && [
          'hover:shadow-lg',
          'hover:border-primary',
          'border',
          'border-transparent',
          'transition-all',
          'duration-300',
        ],
        // Variant-specific styles
        variant === 'bordered' && 'border border-base-300',
        variant === 'glass' && [
          'bg-base-200/70',
          'backdrop-blur-md',
          'border',
          'border-base-content/10',
        ],
        // Padding
        paddingClasses[padding]
      ),
      className
    );

    return (
      <div
        ref={ref}
        className={cardClasses}
        data-testid="ui-card"
        {...props}
      >
        {children}
      </div>
    );
  }
);

Card.displayName = 'Card';

/**
 * CardBody component for consistent card content layout.
 * Use this inside Card for proper DaisyUI card structure.
 */
export interface CardBodyProps extends HTMLAttributes<HTMLDivElement> {
  children: ReactNode;
  className?: string;
}

export const CardBody = forwardRef<HTMLDivElement, CardBodyProps>(
  ({ children, className, ...props }, ref) => {
    const bodyClasses = twMerge(
      'card-body',
      className
    );

    return (
      <div
        ref={ref}
        className={bodyClasses}
        data-testid="ui-card-body"
        {...props}
      >
        {children}
      </div>
    );
  }
);

CardBody.displayName = 'CardBody';

/**
 * CardTitle component for consistent card headings.
 */
export interface CardTitleProps extends HTMLAttributes<HTMLHeadingElement> {
  children: ReactNode;
  className?: string;
  as?: 'h1' | 'h2' | 'h3' | 'h4' | 'h5' | 'h6';
}

export const CardTitle = forwardRef<HTMLHeadingElement, CardTitleProps>(
  ({ children, className, as: Component = 'h3', ...props }, ref) => {
    const titleClasses = twMerge(
      'card-title',
      'text-base-content',
      className
    );

    return (
      <Component
        ref={ref}
        className={titleClasses}
        data-testid="ui-card-title"
        {...props}
      >
        {children}
      </Component>
    );
  }
);

CardTitle.displayName = 'CardTitle';
