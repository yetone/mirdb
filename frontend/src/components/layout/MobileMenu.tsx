/**
 * Mobile Menu Component.
 * Owner: Scenario 9 - Mobile Responsive Design
 *
 * Slide-in navigation menu for mobile viewports (< 768px):
 * - Triggered by hamburger menu in HomeNavbar
 * - Contains: Login, Register links
 * - Theme toggle
 * - Slide-in animation from right
 * - Overlay backdrop
 *
 * Requirements: US-4
 */
import { useEffect, useRef } from 'react';
import { Link } from 'react-router-dom';
import { X } from 'lucide-react';
import { ThemeToggle } from '../ThemeToggle';

interface MobileMenuProps {
  isOpen: boolean;
  onClose: () => void;
}

export function MobileMenu({ isOpen, onClose }: MobileMenuProps) {
  const menuRef = useRef<HTMLDivElement>(null);
  const closeButtonRef = useRef<HTMLButtonElement>(null);

  // Focus management: focus close button when menu opens
  useEffect(() => {
    if (isOpen && closeButtonRef.current) {
      closeButtonRef.current.focus();
    }
  }, [isOpen]);

  // Handle escape key to close menu
  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && isOpen) {
        onClose();
      }
    };

    document.addEventListener('keydown', handleEscape);
    return () => document.removeEventListener('keydown', handleEscape);
  }, [isOpen, onClose]);

  // Prevent body scroll when menu is open
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

  // Handle click outside to close
  const handleBackdropClick = (e: React.MouseEvent) => {
    if (e.target === e.currentTarget) {
      onClose();
    }
  };

  // Don't render anything if closed (to avoid duplicate elements in DOM)
  if (!isOpen) {
    return null;
  }

  return (
    <>
      {/* Backdrop overlay */}
      <div
        className="fixed inset-0 bg-black/50 z-40 transition-opacity duration-300 opacity-100"
        onClick={handleBackdropClick}
        aria-hidden="false"
        data-testid="mobile-menu-backdrop"
      />

      {/* Slide-in menu panel */}
      <div
        ref={menuRef}
        role="dialog"
        aria-modal="true"
        aria-label="Mobile navigation menu"
        aria-hidden="false"
        className="fixed top-0 right-0 h-full w-64 bg-base-100 shadow-xl z-50 transform transition-transform duration-300 ease-out translate-x-0"
        data-testid="mobile-menu"
      >
        {/* Header with close button */}
        <div className="flex items-center justify-between p-4 border-b border-base-200">
          <span className="text-lg font-semibold">Menu</span>
          <button
            ref={closeButtonRef}
            onClick={onClose}
            className="btn btn-ghost btn-circle btn-sm"
            aria-label="Close menu"
            data-testid="mobile-menu-close"
          >
            <X className="h-5 w-5" aria-hidden="true" />
          </button>
        </div>

        {/* Navigation links */}
        <nav className="flex flex-col p-4 gap-2" aria-label="Mobile navigation">
          <Link
            to="/login"
            onClick={onClose}
            className="btn btn-ghost justify-start h-12 min-h-[44px] text-base"
            data-testid="mobile-menu-login"
          >
            Login
          </Link>
          <Link
            to="/register"
            onClick={onClose}
            className="btn btn-primary justify-start h-12 min-h-[44px] text-base"
            data-testid="mobile-menu-register"
          >
            Register
          </Link>
        </nav>

        {/* Theme toggle section */}
        <div className="p-4 border-t border-base-200" data-testid="mobile-menu-theme-section">
          <div className="flex items-center justify-between">
            <span className="text-sm text-base-content/80">Theme</span>
            <ThemeToggle />
          </div>
        </div>
      </div>
    </>
  );
}
