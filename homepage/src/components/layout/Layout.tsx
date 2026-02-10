/**
 * Main Layout Component
 * Owner: First Builder (Shared)
 *
 * Wrapper layout component containing:
 * - Skip link
 * - Header with navigation
 * - Main content area
 * - Footer
 */

import React from 'react';
import { Header } from './Header';

interface LayoutProps {
  children: React.ReactNode;
}

const layoutStyles: React.CSSProperties = {
  minHeight: '100vh',
  display: 'flex',
  flexDirection: 'column',
};

const mainStyles: React.CSSProperties = {
  flex: 1,
  width: '100%',
  maxWidth: 'var(--container-max-width)',
  margin: '0 auto',
  padding: 'var(--container-padding)',
};

export const Layout: React.FC<LayoutProps> = ({ children }) => {
  return (
    <div style={layoutStyles}>
      {/* Skip link will be added by Scenario 8 */}
      <Header />
      <main id="main-content" style={mainStyles}>
        {children}
      </main>
      {/* Footer will be added by Scenario 10 */}
    </div>
  );
};

export default Layout;
