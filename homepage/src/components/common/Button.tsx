/**
 * Reusable Button component.
 * Owner: First Builder (Shared)
 */
import React from 'react';
import Link from 'next/link';
import { ButtonProps } from '@/types';
import { cn, isExternalUrl } from '@/utils/helpers';

const variantStyles = {
  primary: 'bg-blue-600 hover:bg-blue-700 text-white focus:ring-blue-500',
  secondary: 'bg-gray-100 text-gray-900 hover:bg-gray-200 focus:ring-gray-500',
  outline: 'border-2 border-blue-600 text-blue-600 hover:bg-blue-50 focus:ring-blue-500',
};

const sizeStyles = {
  sm: 'px-3 py-1.5 text-sm',
  md: 'px-5 py-2.5 text-base',
  lg: 'px-8 py-3.5 text-lg',
};

export function Button({
  children,
  variant = 'primary',
  size = 'md',
  href,
  onClick,
  external,
  className,
  'data-testid': testId,
}: ButtonProps) {
  const baseStyles = cn(
    'inline-flex items-center justify-center font-semibold rounded-lg transition-colors duration-200 focus:outline-none focus:ring-2 focus:ring-offset-2',
    variantStyles[variant],
    sizeStyles[size],
    className
  );

  if (href) {
    const isExternal = external ?? isExternalUrl(href);

    if (isExternal) {
      return (
        <a
          href={href}
          className={baseStyles}
          target="_blank"
          rel="noopener noreferrer"
          data-testid={testId}
        >
          {children}
        </a>
      );
    }

    return (
      <Link href={href} className={baseStyles} data-testid={testId}>
        {children}
      </Link>
    );
  }

  return (
    <button
      type="button"
      onClick={onClick}
      className={baseStyles}
      data-testid={testId}
    >
      {children}
    </button>
  );
}
