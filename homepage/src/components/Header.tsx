'use client';

import { useState, useCallback, useEffect, useMemo } from 'react';
import { NAV_LINKS } from '@/lib/constants';
import { cn } from '@/lib/utils';
import { useScrollSpy } from '@/hooks/useScrollSpy';

const HEADER_HEIGHT_PX = 64;

function getSectionIdFromHref(href: string): string | null {
  if (!href.startsWith('#') || href === '#') {
    return null;
  }
  return href.slice(1);
}

function isInternalAnchor(href: string): boolean {
  return href.startsWith('#') && href.length > 1;
}

export default function Header() {
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [isScrolled, setIsScrolled] = useState(false);

  const sectionIds = useMemo(
    () =>
      NAV_LINKS.map((link) => getSectionIdFromHref(link.href)).filter(
        (id): id is string => id !== null
      ),
    []
  );

  const { activeId } = useScrollSpy(sectionIds);

  const toggleMobileMenu = useCallback(() => {
    setMobileMenuOpen((prev) => !prev);
  }, []);

  const closeMobileMenu = useCallback(() => {
    setMobileMenuOpen(false);
  }, []);

  // Track scroll position for sticky header background variant.
  useEffect(() => {
    const handleScroll = () => {
      setIsScrolled(window.scrollY > 8);
    };
    handleScroll();
    window.addEventListener('scroll', handleScroll, { passive: true });
    return () => window.removeEventListener('scroll', handleScroll);
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

  // On initial mount, if URL has a hash, smooth-scroll to that section.
  useEffect(() => {
    if (typeof window === 'undefined') return;
    const hash = window.location.hash;
    if (!hash || hash === '#') return;
    const id = hash.slice(1);
    const target = document.getElementById(id);
    if (!target) return;
    // Defer until layout is ready so the scroll lands accurately.
    requestAnimationFrame(() => {
      const top =
        target.getBoundingClientRect().top + window.scrollY - HEADER_HEIGHT_PX;
      window.scrollTo({ top, behavior: 'smooth' });
    });
  }, []);

  const handleNavClick = useCallback(
    (e: React.MouseEvent<HTMLAnchorElement>, href: string) => {
      if (!isInternalAnchor(href)) {
        // External link - close mobile menu and let default navigation occur.
        closeMobileMenu();
        return;
      }
      e.preventDefault();
      const id = href.slice(1);
      const target = document.getElementById(id);
      if (!target) return;

      // Close the mobile menu first so the overlay doesn't intercept scroll.
      closeMobileMenu();

      const top =
        target.getBoundingClientRect().top + window.scrollY - HEADER_HEIGHT_PX;
      window.scrollTo({ top, behavior: 'smooth' });

      // Update URL hash without triggering a page reload or scroll jump.
      if (window.history && typeof window.history.pushState === 'function') {
        window.history.pushState(null, '', href);
      }
    },
    [closeMobileMenu]
  );

  return (
    <header
      className={cn(
        'fixed top-0 left-0 right-0 z-50 transition-colors duration-200',
        isScrolled
          ? 'bg-[var(--background)]/90 backdrop-blur-md border-b border-[var(--border)] shadow-sm'
          : 'bg-[var(--background)]/80 backdrop-blur-md border-b border-[var(--border)]'
      )}
      data-testid="header"
      data-scrolled={isScrolled ? 'true' : 'false'}
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
            {NAV_LINKS.map((link) => {
              const sectionId = getSectionIdFromHref(link.href);
              const isActive = sectionId !== null && sectionId === activeId;
              const external = !isInternalAnchor(link.href);
              return (
                <a
                  key={link.label}
                  href={link.href}
                  onClick={(e) => handleNavClick(e, link.href)}
                  className={cn(
                    'text-sm font-medium transition-colors',
                    isActive
                      ? 'text-[var(--foreground)] font-semibold'
                      : 'text-[var(--muted-foreground)] hover:text-[var(--foreground)]'
                  )}
                  aria-current={isActive ? 'true' : undefined}
                  data-testid={`desktop-nav-link-${link.label
                    .toLowerCase()
                    .replace(/\s+/g, '-')}`}
                  data-active={isActive ? 'true' : 'false'}
                  {...(external
                    ? { target: '_blank', rel: 'noopener noreferrer' }
                    : {})}
                >
                  {link.label}
                </a>
              );
            })}
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
            {NAV_LINKS.map((link) => {
              const sectionId = getSectionIdFromHref(link.href);
              const isActive = sectionId !== null && sectionId === activeId;
              const external = !isInternalAnchor(link.href);
              return (
                <a
                  key={link.label}
                  href={link.href}
                  onClick={(e) => handleNavClick(e, link.href)}
                  className={cn(
                    'px-4 py-3 rounded-lg',
                    'text-lg font-medium transition-colors',
                    isActive
                      ? 'bg-[var(--card)] text-[var(--foreground)] font-semibold'
                      : 'text-[var(--foreground)] hover:bg-[var(--card)]'
                  )}
                  aria-current={isActive ? 'true' : undefined}
                  data-testid="mobile-nav-link"
                  data-active={isActive ? 'true' : 'false'}
                  {...(external
                    ? { target: '_blank', rel: 'noopener noreferrer' }
                    : {})}
                >
                  {link.label}
                </a>
              );
            })}
          </nav>
        </div>
      )}
    </header>
  );
}
