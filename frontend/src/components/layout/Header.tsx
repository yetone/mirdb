/**
 * Header component.
 * Owner: Scenario 13 - Responsive Layout
 *
 * Responsive header with MirDB branding, navigation links,
 * theme toggle, and mobile hamburger menu trigger.
 */

import { useState, useCallback, useEffect } from 'react';
import { GITHUB_REPO_URL, DOCS_URL } from '../../utils/constants';
import ThemeToggle from '../theme/ThemeToggle';

export interface HeaderProps {
  onMenuToggle?: (open: boolean) => void;
}

export default function Header({ onMenuToggle }: HeaderProps) {
  const [menuOpen, setMenuOpen] = useState(false);

  // Close mobile menu on Escape key
  useEffect(() => {
    if (!menuOpen) return;

    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        setMenuOpen(false);
        onMenuToggle?.(false);
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [menuOpen, onMenuToggle]);

  const toggleMenu = useCallback(() => {
    setMenuOpen((prev) => {
      const next = !prev;
      onMenuToggle?.(next);
      return next;
    });
  }, [onMenuToggle]);

  const closeMenu = useCallback(() => {
    setMenuOpen(false);
    onMenuToggle?.(false);
  }, [onMenuToggle]);

  return (
    <header className="layout-header" data-testid="layout-header">
      <div className="layout-header__inner">
        {/* Brand */}
        <a href="/" className="layout-header__brand" data-testid="header-brand">
          <img
            src="/mirdb-logo.svg"
            alt=""
            width={32}
            height={32}
            aria-hidden="true"
            className="layout-header__logo"
          />
          <span className="layout-header__title">MirDB</span>
        </a>

        {/* Desktop Navigation */}
        <nav
          className="layout-header__nav"
          aria-label="Main navigation"
          data-testid="header-desktop-nav"
        >
          <a
            href={GITHUB_REPO_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="layout-header__nav-link"
            data-testid="header-nav-github"
          >
            GitHub
          </a>
          <a
            href={DOCS_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="layout-header__nav-link"
            data-testid="header-nav-docs"
          >
            Documentation
          </a>
          <ThemeToggle className="layout-header__theme-toggle" />
        </nav>

        {/* Mobile Menu Button */}
        <button
          type="button"
          className="layout-header__menu-btn"
          aria-label={menuOpen ? 'Close menu' : 'Open menu'}
          aria-expanded={menuOpen}
          aria-controls="mobile-nav"
          data-testid="header-menu-button"
          onClick={toggleMenu}
        >
          <span className="layout-header__menu-icon" aria-hidden="true">
            <span
              className={`layout-header__menu-bar${menuOpen ? ' layout-header__menu-bar--open' : ''}`}
            />
            <span
              className={`layout-header__menu-bar${menuOpen ? ' layout-header__menu-bar--open' : ''}`}
            />
            <span
              className={`layout-header__menu-bar${menuOpen ? ' layout-header__menu-bar--open' : ''}`}
            />
          </span>
        </button>
      </div>

      {/* Mobile Navigation */}
      {menuOpen && (
        <nav
          id="mobile-nav"
          className="layout-header__mobile-nav"
          aria-label="Mobile navigation"
          data-testid="header-mobile-nav"
        >
          <a
            href={GITHUB_REPO_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="layout-header__mobile-nav-link"
            data-testid="header-mobile-nav-github"
            onClick={closeMenu}
          >
            GitHub
          </a>
          <a
            href={DOCS_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="layout-header__mobile-nav-link"
            data-testid="header-mobile-nav-docs"
            onClick={closeMenu}
          >
            Documentation
          </a>
          <div className="layout-header__mobile-theme-toggle">
            <ThemeToggle />
          </div>
        </nav>
      )}
    </header>
  );
}
