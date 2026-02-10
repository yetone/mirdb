/**
 * Header Component
 * Owner: Scenario 5 - Navigation and GitHub Links
 *
 * Sticky header containing:
 * - Navigation component
 * - Theme toggle
 * - Responsive behavior
 */

import React from 'react';
import { Navigation } from './Navigation';
import { ThemeToggle } from '../ui/ThemeToggle';

const headerStyles: React.CSSProperties = {
  position: 'sticky',
  top: 0,
  left: 0,
  right: 0,
  zIndex: 1000,
  backgroundColor: 'var(--bg-primary)',
  borderBottom: '1px solid var(--border-color)',
  boxShadow: 'var(--shadow-sm)',
  transition: 'background-color 0.3s ease, box-shadow 0.3s ease',
};

const headerContainerStyles: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  padding: 'var(--spacing-md) var(--spacing-lg)',
  minHeight: '64px',
  position: 'relative',
  maxWidth: 'var(--container-max-width)',
  margin: '0 auto',
  width: '100%',
};

const themeToggleContainerStyles: React.CSSProperties = {
  marginLeft: 'auto',
  paddingLeft: 'var(--spacing-md)',
};

export const Header: React.FC = () => {
  return (
    <header style={headerStyles} role="banner" aria-label="Site header">
      <div style={headerContainerStyles}>
        <Navigation />
        <div style={themeToggleContainerStyles}>
          <ThemeToggle />
        </div>
      </div>
    </header>
  );
};

export default Header;
