/**
 * Main layout wrapper component
 *
 * Owner: Scenario 1 - Hero Section and Branding Display
 *
 * Features:
 * - Combines Header, main content area, and Footer
 * - Provides consistent page structure
 */

import { ReactNode } from 'react';
import Header from './Header';
import Footer from './Footer';
import './Layout.css';

interface LayoutProps {
  children: ReactNode;
}

export default function Layout({ children }: LayoutProps) {
  return (
    <div className="layout">
      <Header />
      <main className="layout-main" role="main">
        {children}
      </main>
      <Footer />
    </div>
  );
}
