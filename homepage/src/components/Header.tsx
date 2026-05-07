'use client';

import { useState, useCallback, useEffect } from 'react';
import { NAV_LINKS } from '@/lib/constants';
import { cn } from '@/lib/utils';

export default function Header() {
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  const toggleMobileMenu = useCallback(() => {
    setMobileMenuOpen((prev) => !prev);
  }, []);

  const closeMobileMenu = useCallback(() => {
    setMobileMenuOpen(false);
  }, []);

  // Close menu on Escape key
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && mobileMenuOpen) {
        setMobileMenuOpen(false);
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [mobileMenuOpen]);

  // Prevent body scroll when mobile menu is open
  useEffect(() => {
    if (mobileMenuOpen) {
      document.body.style.overflow = 'hidden';
    } else {
      document.body.style.overflow = '';
    }
    return () => {
      document.body.style.overflow = '';
    };
  }, [mobileMenuOpen]);

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
            aria-label={mobileMenuOpen ? 'Close menu' : 'Open menu'}
            aria-expanded={mobileMenuOpen}
            aria-controls="mobile-nav-overlay"
            data-testid="mobile-menu-button"
            onClick={toggleMobileMenu}
          >
            {mobileMenuOpen ? (
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
                  d="M6 18L18 6M6 6l12 12"
                />
              </svg>
            ) : (
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
            )}
          </button>
        </div>
      </div>

      {/* Mobile Navigation Overlay */}
      {mobileMenuOpen && (
        <div
          id="mobile-nav-overlay"
          className={cn(
            'md:hidden',
            'fixed top-16 left-0 right-0 z-40',
            'h-[calc(100vh-4rem)]',
            'bg-[var(--background)]/95 backdrop-blur-lg'
          )}
          data-testid="mobile-nav-overlay"
          role="dialog"
          aria-label="Mobile navigation menu"
          aria-modal="true"
        >
          <nav
            className="flex flex-col p-6 gap-2"
            aria-label="Mobile navigation"
            data-testid="mobile-nav"
          >
            {NAV_LINKS.map((link) => (
              <a
                key={link.label}
                href={link.href}
                className={cn(
                  'px-4 py-3 rounded-lg',
                  'text-lg font-medium text-[var(--foreground)]',
                  'hover:bg-[var(--card)] transition-colors'
                )}
                onClick={closeMobileMenu}
                data-testid="mobile-nav-link"
              >
                {link.label}
              </a>
            ))}
          </nav>
        </div>
      )}
    </header>
  );
}
