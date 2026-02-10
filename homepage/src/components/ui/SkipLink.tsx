/**
 * Skip Link Component
 * Owner: Scenario 8 - Accessibility Compliance
 *
 * Accessibility skip link for keyboard users:
 * - Hidden until focused
 * - Links to main content
 * - Visible focus state
 */

import React from 'react';

export interface SkipLinkProps {
  /** The ID of the main content element to skip to */
  targetId?: string;
  /** Custom label text for the skip link */
  label?: string;
}

const skipLinkStyles: React.CSSProperties = {
  position: 'absolute',
  left: '-9999px',
  top: 'auto',
  width: '1px',
  height: '1px',
  overflow: 'hidden',
  zIndex: 9999,
};

const skipLinkFocusStyles: React.CSSProperties = {
  position: 'fixed',
  top: 'var(--spacing-md)',
  left: 'var(--spacing-md)',
  width: 'auto',
  height: 'auto',
  overflow: 'visible',
  padding: 'var(--spacing-md) var(--spacing-lg)',
  backgroundColor: 'var(--accent-primary)',
  color: '#ffffff',
  fontWeight: 600,
  fontSize: 'var(--font-size-base)',
  textDecoration: 'none',
  borderRadius: 'var(--radius-md)',
  boxShadow: 'var(--shadow-lg)',
  outline: '3px solid var(--accent-secondary)',
  outlineOffset: '2px',
};

export const SkipLink: React.FC<SkipLinkProps> = ({
  targetId = 'main-content',
  label = 'Skip to main content',
}) => {
  const [isFocused, setIsFocused] = React.useState(false);

  const handleFocus = () => setIsFocused(true);
  const handleBlur = () => setIsFocused(false);

  const combinedStyles: React.CSSProperties = isFocused
    ? skipLinkFocusStyles
    : skipLinkStyles;

  return (
    <a
      href={`#${targetId}`}
      className="skip-link"
      style={combinedStyles}
      onFocus={handleFocus}
      onBlur={handleBlur}
      data-testid="skip-link"
    >
      {label}
    </a>
  );
};

export default SkipLink;
