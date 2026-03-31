/**
 * Header Component
 * Owner: Scenario 4 - Navigation and Header
 *
 * Sticky header with:
 * - Logo
 * - Desktop navigation links
 * - Mobile hamburger menu trigger
 * - Theme toggle placeholder
 *
 * Requirements: REQ-8
 */

import { useState, useEffect, useCallback } from 'react';
import { Menu } from 'lucide-react';
import { Navigation } from './Navigation';
import { MobileMenu } from './MobileMenu';
import { NAV_ITEMS } from '../../constants/navigation';
import { cn } from '../../utils';

export interface HeaderProps {
  className?: string;
}

export function Header({ className }: HeaderProps) {
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);
  const [activeSection, setActiveSection] = useState<string>('hero');
  const [isScrolled, setIsScrolled] = useState(false);

  const handleScroll = useCallback(() => {
    setIsScrolled(window.scrollY > 10);

    // Find the current active section based on scroll position
    const sections = NAV_ITEMS.map((item) => item.id);
    let currentSection = 'hero';

    for (const sectionId of sections) {
      const element = document.getElementById(sectionId);
      if (element) {
        const rect = element.getBoundingClientRect();
        // Consider section active when its top is within 100px of viewport top
        if (rect.top <= 100 && rect.bottom > 100) {
          currentSection = sectionId;
        }
      }
    }

    setActiveSection(currentSection);
  }, []);

  useEffect(() => {
    window.addEventListener('scroll', handleScroll, { passive: true });
    handleScroll(); // Initial check
    return () => {
      window.removeEventListener('scroll', handleScroll);
    };
  }, [handleScroll]);

  const handleNavClick = (sectionId: string) => {
    const element = document.getElementById(sectionId);
    if (element) {
      element.scrollIntoView({ behavior: 'smooth' });
    }
    setActiveSection(sectionId);
  };

  const openMobileMenu = () => {
    setIsMobileMenuOpen(true);
  };

  const closeMobileMenu = () => {
    setIsMobileMenuOpen(false);
  };

  return (
    <>
      <header
        className={cn(
          'fixed top-0 left-0 right-0 z-30 transition-all duration-200',
          isScrolled
            ? 'bg-white/95 dark:bg-gray-900/95 backdrop-blur-sm shadow-sm'
            : 'bg-transparent',
          className
        )}
        data-testid="header"
      >
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            {/* Logo */}
            <a
              href="#hero"
              className="flex items-center space-x-2"
              onClick={(e) => {
                e.preventDefault();
                handleNavClick('hero');
              }}
              data-testid="header-logo"
            >
              <img
                src="/assets/logo.gif"
                alt="MirDB"
                className="h-8 w-auto"
              />
              <span className="text-xl font-bold text-gray-900 dark:text-white">
                MirDB
              </span>
            </a>

            {/* Desktop Navigation */}
            <Navigation
              items={NAV_ITEMS}
              activeSection={activeSection}
              onNavClick={handleNavClick}
            />

            {/* Mobile menu button */}
            <button
              onClick={openMobileMenu}
              className="md:hidden p-2 rounded-md text-gray-500 hover:text-gray-700 hover:bg-gray-100 dark:text-gray-400 dark:hover:text-gray-200 dark:hover:bg-gray-800 transition-colors min-w-[44px] min-h-[44px] flex items-center justify-center"
              aria-label="Open menu"
              aria-expanded={isMobileMenuOpen}
              data-testid="mobile-menu-button"
            >
              <Menu className="w-6 h-6" aria-hidden="true" />
            </button>
          </div>
        </div>
      </header>

      {/* Mobile Menu */}
      <MobileMenu
        isOpen={isMobileMenuOpen}
        onClose={closeMobileMenu}
        items={NAV_ITEMS}
        activeSection={activeSection}
        onNavClick={handleNavClick}
      />
    </>
  );
}
