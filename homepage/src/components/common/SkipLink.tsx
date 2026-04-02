/**
 * Skip Link component for keyboard navigation.
 * Owner: Scenario 12 - Accessibility Compliance
 *
 * Provides a skip link that becomes visible on focus,
 * allowing keyboard users to skip navigation and jump
 * directly to main content.
 */
import React from 'react';
import styles from './SkipLink.module.css';

export interface SkipLinkProps {
  href?: string;
  children?: React.ReactNode;
}

export function SkipLink({
  href = '#main-content',
  children = 'Skip to main content',
}: SkipLinkProps) {
  const handleClick = (e: React.MouseEvent<HTMLAnchorElement>) => {
    e.preventDefault();
    const target = document.querySelector(href);
    if (target) {
      // Set tabindex to allow focus on non-focusable elements
      if (!target.hasAttribute('tabindex')) {
        target.setAttribute('tabindex', '-1');
      }
      (target as HTMLElement).focus();
    }
  };

  return (
    <a
      href={href}
      className={styles.skipLink}
      data-testid="skip-link"
      onClick={handleClick}
    >
      {children}
    </a>
  );
}
