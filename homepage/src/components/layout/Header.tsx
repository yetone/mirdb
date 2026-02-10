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
  padding: 'var(--spacing-md) 0',
  minHeight: '64px',
  position: 'relative',
};

export const Header: React.FC = () => {
  return (
    <header style={headerStyles} role="banner" aria-label="Site header">
      <div style={headerContainerStyles}>
        <Navigation />
        {/* Theme toggle will be added by Scenario 6 */}
      </div>
    </header>
  );
};

export default Header;
