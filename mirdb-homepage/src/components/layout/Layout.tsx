/**
 * Main layout wrapper component.
 */

import type { ReactNode } from 'react';
import { ThemeToggle } from '../ui/ThemeToggle';

interface LayoutProps {
  children: ReactNode;
}

export function Layout({ children }: LayoutProps) {
  return (
    <div className="min-h-screen flex flex-col">
      {/* Theme toggle positioned in top-right corner */}
      <div className="fixed top-4 right-4 z-50">
        <ThemeToggle />
      </div>
      <main className="flex-grow">
        {children}
      </main>
    </div>
  );
}
