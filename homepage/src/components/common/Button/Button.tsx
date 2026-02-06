import { forwardRef } from 'react';
import { Link } from 'react-router-dom';
import type { ButtonProps } from '../../../types';
import styles from './Button.module.css';

/**
 * Reusable Button component with primary and secondary variants.
 * Can render as a button or a link (anchor/router link).
 */
export const Button = forwardRef<
  HTMLButtonElement | HTMLAnchorElement,
  ButtonProps
>(function Button(
  { variant = 'primary', href, onClick, children, className = '', ...props },
  ref
) {
  const classNames = [
    styles.button,
    styles[variant],
    className,
  ].filter(Boolean).join(' ');

  // If href is provided, render as a link
  if (href) {
    // External link or anchor
    if (href.startsWith('http') || href.startsWith('#')) {
      return (
        <a
          ref={ref as React.Ref<HTMLAnchorElement>}
          href={href}
          className={classNames}
          onClick={onClick}
          {...props}
        >
          {children}
        </a>
      );
    }

    // Internal route
    return (
      <Link
        ref={ref as React.Ref<HTMLAnchorElement>}
        to={href}
        className={classNames}
        onClick={onClick}
        {...props}
      >
        {children}
      </Link>
    );
  }

  // Otherwise, render as a button
  return (
    <button
      ref={ref as React.Ref<HTMLButtonElement>}
      type="button"
      className={classNames}
      onClick={onClick}
      {...props}
    >
      {children}
    </button>
  );
});
