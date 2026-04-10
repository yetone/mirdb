import React from 'react';
import type { ButtonProps } from '../../types';
import './Button.css';

/**
 * Button Component
 * Owner: Scenario 1 - Hero Section and Value Proposition
 *
 * Reusable button component with multiple variants and sizes.
 * Supports both link-style navigation and onClick handlers.
 * Includes proper hover and focus states for accessibility.
 */
export const Button: React.FC<ButtonProps> = ({
  children,
  href,
  onClick,
  variant = 'primary',
  size = 'md',
  className = '',
}) => {
  const baseClass = 'btn';
  const variantClass = `btn--${variant}`;
  const sizeClass = `btn--${size}`;
  const combinedClass = `${baseClass} ${variantClass} ${sizeClass} ${className}`.trim();

  if (href) {
    return (
      <a href={href} className={combinedClass} data-testid="cta-button">
        {children}
      </a>
    );
  }

  return (
    <button type="button" onClick={onClick} className={combinedClass} data-testid="cta-button">
      {children}
    </button>
  );
};

export default Button;
