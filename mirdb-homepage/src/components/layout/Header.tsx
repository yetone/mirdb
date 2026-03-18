/**
 * Header Component with Sticky Navigation.
 * Owner: Scenario 9 - Navigation and Footer
 *
 * Features:
 * - Sticky positioning
 * - Logo/brand
 * - Navigation links
 * - Theme toggle slot
 * - Mobile hamburger menu
 */

import { useState } from 'react';
import { Navigation } from './Navigation';
import { NAV_ITEMS } from '../../utils/constants';
import { useSmoothScroll } from '../../hooks/useSmoothScroll';

export interface HeaderProps {
  className?: string;
}

export function Header({ className = '' }: HeaderProps) {
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);
  const { scrollTo } = useSmoothScroll();

  const handleMobileNavClick = (href: string) => {
    if (href.startsWith('#')) {
      scrollTo(href);
    }
    setIsMobileMenuOpen(false);
  };

  return (
    <header
      className={`sticky top-0 z-50 bg-gray-900/95 backdrop-blur-sm border-b border-gray-800 ${className}`}
      data-testid="header"
    >
      <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-16">
          {/* Logo/Brand */}
          <a
            href="#"
            className="flex items-center gap-2 text-white font-bold text-xl"
            data-testid="header-logo"
          >
            <span className="text-blue-400">Mir</span>
            <span>DB</span>
          </a>

          {/* Desktop Navigation */}
          <div className="hidden md:block">
            <Navigation items={NAV_ITEMS} />
          </div>

          {/* Mobile Menu Button */}
          <button
            type="button"
            className="md:hidden p-2 text-gray-400 hover:text-white"
            onClick={() => setIsMobileMenuOpen(!isMobileMenuOpen)}
            aria-expanded={isMobileMenuOpen}
            aria-controls="mobile-menu"
            aria-label={isMobileMenuOpen ? 'Close menu' : 'Open menu'}
            data-testid="mobile-menu-button"
          >
            {isMobileMenuOpen ? (
              <svg
                className="w-6 h-6"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
                aria-hidden="true"
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
                aria-hidden="true"
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

        {/* Mobile Navigation Menu */}
        {isMobileMenuOpen && (
          <nav
            id="mobile-menu"
            className="md:hidden py-4 border-t border-gray-800"
            aria-label="Mobile navigation"
            data-testid="mobile-navigation"
          >
            <ul className="flex flex-col gap-4">
              {NAV_ITEMS.map((item) => {
                const isExternal = !item.href.startsWith('#');
                return (
                  <li key={item.label}>
                    <a
                      href={item.href}
                      onClick={(e) => {
                        if (item.href.startsWith('#')) {
                          e.preventDefault();
                          handleMobileNavClick(item.href);
                        }
                      }}
                      className="block text-gray-300 hover:text-white transition-colors font-medium py-2"
                      data-testid={`mobile-nav-link-${item.label.toLowerCase().replace(/\s+/g, '-')}`}
                      {...(isExternal && {
                        target: '_blank',
                        rel: 'noopener noreferrer',
                      })}
                    >
                      {item.label}
                    </a>
                  </li>
                );
              })}
            </ul>
          </nav>
        )}
      </div>
    </header>
  );
}
