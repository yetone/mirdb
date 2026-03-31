/**
 * Mobile Menu Component
 * Owner: Scenario 4 - Navigation and Header
 *
 * Slide-in mobile navigation menu.
 *
 * Requirements: REQ-6, Story 6 (Mobile Experience)
 *
 * Expected props:
 * - isOpen: boolean
 * - onClose: () => void
 * - items: NavItem[]
 */

import { useEffect, useCallback } from 'react';
import { X } from 'lucide-react';
import { NavItem } from '../../types';
import { cn } from '../../utils';

export interface MobileMenuProps {
  isOpen: boolean;
  onClose: () => void;
  items: NavItem[];
  activeSection?: string;
  onNavClick?: (sectionId: string) => void;
}

export function MobileMenu({ isOpen, onClose, items, activeSection, onNavClick }: MobileMenuProps) {
  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    if (e.key === 'Escape' && isOpen) {
      onClose();
    }
  }, [isOpen, onClose]);

  useEffect(() => {
    document.addEventListener('keydown', handleKeyDown);
    return () => {
      document.removeEventListener('keydown', handleKeyDown);
    };
  }, [handleKeyDown]);

  useEffect(() => {
    if (isOpen) {
      document.body.style.overflow = 'hidden';
    } else {
      document.body.style.overflow = '';
    }
    return () => {
      document.body.style.overflow = '';
    };
  }, [isOpen]);

  const handleNavClick = (e: React.MouseEvent<HTMLAnchorElement>, item: NavItem) => {
    e.preventDefault();
    const sectionId = item.href.replace('#', '');

    if (onNavClick) {
      onNavClick(sectionId);
    } else {
      const element = document.getElementById(sectionId);
      if (element) {
        element.scrollIntoView({ behavior: 'smooth' });
      }
    }
    onClose();
  };

  return (
    <>
      {/* Backdrop */}
      <div
        className={cn(
          'fixed inset-0 bg-black/50 z-40 transition-opacity md:hidden',
          isOpen ? 'opacity-100' : 'opacity-0 pointer-events-none'
        )}
        onClick={onClose}
        aria-hidden="true"
        data-testid="mobile-menu-backdrop"
      />

      {/* Menu panel */}
      <div
        className={cn(
          'fixed top-0 right-0 h-full w-64 bg-white dark:bg-gray-900 shadow-xl z-50 transform transition-transform duration-300 ease-in-out md:hidden',
          isOpen ? 'translate-x-0' : 'translate-x-full'
        )}
        role="dialog"
        aria-modal="true"
        aria-label="Mobile navigation menu"
        data-testid="mobile-menu"
      >
        <div className="flex items-center justify-between p-4 border-b border-gray-200 dark:border-gray-700">
          <span className="text-lg font-semibold text-gray-900 dark:text-white">Menu</span>
          <button
            onClick={onClose}
            className="p-2 rounded-md text-gray-500 hover:text-gray-700 hover:bg-gray-100 dark:text-gray-400 dark:hover:text-gray-200 dark:hover:bg-gray-800 transition-colors"
            aria-label="Close menu"
            data-testid="mobile-menu-close"
          >
            <X className="w-5 h-5" aria-hidden="true" />
          </button>
        </div>

        <nav className="p-4" aria-label="Mobile navigation">
          <ul className="space-y-2">
            {items.map((item) => {
              const isActive = activeSection === item.id;
              return (
                <li key={item.id}>
                  <a
                    href={item.href}
                    onClick={(e) => handleNavClick(e, item)}
                    className={cn(
                      'block px-4 py-3 text-base font-medium rounded-lg transition-colors min-h-[44px] flex items-center',
                      isActive
                        ? 'text-primary-600 bg-primary-50 dark:text-primary-400 dark:bg-primary-900/20'
                        : 'text-gray-700 hover:text-primary-600 hover:bg-gray-100 dark:text-gray-300 dark:hover:text-primary-400 dark:hover:bg-gray-800'
                    )}
                    aria-current={isActive ? 'page' : undefined}
                    data-testid={`mobile-nav-link-${item.id}`}
                  >
                    {item.label}
                  </a>
                </li>
              );
            })}
          </ul>
        </nav>
      </div>
    </>
  );
}
