import React from 'react';
import { ButtonProps } from '@/types';
import styles from './Button.module.css';

export function Button({
  children,
  variant = 'primary',
  size = 'md',
  href,
  onClick,
  external,
  className,
  'data-testid': dataTestId,
}: ButtonProps) {
  const classNames = [
    styles.button,
    styles[variant],
    styles[size],
    className,
  ].filter(Boolean).join(' ');

  if (href) {
    return (
      <a
        href={href}
        className={classNames}
        target={external ? '_blank' : undefined}
        rel={external ? 'noopener noreferrer' : undefined}
        data-testid={dataTestId}
      >
        {children}
      </a>
    );
  }

  return (
    <button className={classNames} onClick={onClick} type="button" data-testid={dataTestId}>
      {children}
    </button>
  );
}
