'use client';

import { NAV_LINKS } from '@/lib/constants';
import { cn } from '@/lib/utils';

export default function Header() {
  return (
    <header
      className={cn(
        'fixed top-0 left-0 right-0 z-50',
        'bg-[var(--background)]/80 backdrop-blur-md',
        'border-b border-[var(--border)]'
      )}
      data-testid="header"
    >
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-16">
          {/* Logo / Brand */}
          <a
            href="#"
            className="text-xl font-bold text-[var(--foreground)] flex items-center gap-2"
            data-testid="header-logo"
          >
            <span className="text-brand-500">MirDB</span>
          </a>

          {/* Desktop Navigation - Horizontal links */}
          <nav
            className="hidden md:flex items-center gap-8"
            data-testid="desktop-nav"
            aria-label="Main navigation"
          >
            {NAV_LINKS.map((link) => (
              <a
                key={link.label}
                href={link.href}
                className={cn(
                  'text-sm font-medium text-[var(--muted-foreground)]',
                  'hover:text-[var(--foreground)] transition-colors'
                )}
              >
                {link.label}
              </a>
            ))}
          </nav>

          {/* Mobile hamburger - hidden on desktop */}
          <button
            type="button"
            className="md:hidden p-2 text-[var(--foreground)]"
            aria-label="Open menu"
            data-testid="mobile-menu-button"
          >
            <svg
              className="w-6 h-6"
              fill="none"
              stroke="currentColor"
              viewBox="0 0 24 24"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M4 6h16M4 12h16M4 18h16"
              />
            </svg>
          </button>
        </div>
      </div>
    </header>
  );
}
