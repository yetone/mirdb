/**
 * Button Component
 * Owner: First Builder (Shared)
 *
 * Reusable button component with:
 * - Primary and secondary variants
 * - Size variants
 * - Link button variant
 * - Hover and focus states
 */

import React from 'react';

export interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'primary' | 'secondary' | 'ghost';
  size?: 'sm' | 'md' | 'lg';
  href?: string;
  isExternal?: boolean;
  children: React.ReactNode;
}

const buttonStyles: React.CSSProperties = {
  display: 'inline-flex',
  alignItems: 'center',
  justifyContent: 'center',
  gap: '0.5rem',
  fontWeight: 600,
  borderRadius: 'var(--radius-md)',
  cursor: 'pointer',
  transition: 'all 0.2s ease',
  border: 'none',
  textDecoration: 'none',
};

const variantStyles: Record<string, React.CSSProperties> = {
  primary: {
    backgroundColor: 'var(--accent-primary)',
    color: '#ffffff',
  },
  secondary: {
    backgroundColor: 'transparent',
    color: 'var(--accent-primary)',
    border: '2px solid var(--accent-primary)',
  },
  ghost: {
    backgroundColor: 'transparent',
    color: 'var(--text-primary)',
  },
};

const sizeStyles: Record<string, React.CSSProperties> = {
  sm: {
    padding: '0.5rem 1rem',
    fontSize: 'var(--font-size-sm)',
  },
  md: {
    padding: '0.75rem 1.5rem',
    fontSize: 'var(--font-size-base)',
  },
  lg: {
    padding: '1rem 2rem',
    fontSize: 'var(--font-size-lg)',
  },
};

export const Button: React.FC<ButtonProps> = ({
  variant = 'primary',
  size = 'md',
  href,
  isExternal,
  children,
  style,
  ...props
}) => {
  const combinedStyles = {
    ...buttonStyles,
    ...variantStyles[variant],
    ...sizeStyles[size],
    ...style,
  };

  if (href) {
    return (
      <a
        href={href}
        style={combinedStyles}
        target={isExternal ? '_blank' : undefined}
        rel={isExternal ? 'noopener noreferrer' : undefined}
      >
        {children}
      </a>
    );
  }

  return (
    <button style={combinedStyles} {...props}>
      {children}
    </button>
  );
};

export default Button;
