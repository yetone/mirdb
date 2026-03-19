/**
 * Header component.
 * Owner: Scenario 8 - Navigation Header
 *
 * Features:
 * - Fixed/sticky positioning
 * - MirDB logo
 * - Navigation links
 * - Theme toggle (placeholder for Scenario 11)
 * - Mobile hamburger menu
 *
 * Requirements: REQ-13
 */

'use client';

import { Database } from 'lucide-react';
import { Navigation } from './Navigation';
import { ThemeToggle } from '@/components/ui/ThemeToggle';
import { cn } from '@/lib/utils';

export interface HeaderProps {
  className?: string;
}

export function Header({ className }: HeaderProps) {
  return (
    <header
      className={cn(
        'fixed top-0 left-0 right-0 z-50',
        'bg-white/80 dark:bg-gray-900/80 backdrop-blur-md',
        'border-b border-gray-200 dark:border-gray-800',
        'h-20',
        className
      )}
    >
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-full">
        <div className="flex items-center justify-between h-full">
          {/* Logo */}
          <a
            href="/"
            className="flex items-center gap-2 text-xl font-bold text-gray-900 dark:text-white"
            aria-label="MirDB Home"
          >
            <Database className="h-8 w-8 text-orange-500" aria-hidden="true" />
            <span>MirDB</span>
          </a>

          {/* Navigation */}
          <Navigation />

          {/* Theme Toggle - Implemented by Scenario 11 */}
          <div className="hidden md:flex items-center">
            <ThemeToggle />
          </div>
        </div>
      </div>
    </header>
  );
}
