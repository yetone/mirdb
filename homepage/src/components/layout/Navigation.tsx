/**
 * Navigation Component
 * Owner: Scenario 5 - Navigation and GitHub Links
 *
 * Provides navigation with:
 * - Logo/brand name
 * - Anchor links to sections (Features, Usage, Getting Started)
 * - GitHub link
 * - Mobile hamburger menu
 * - Smooth scroll behavior
 */

import React, { useState } from 'react';
import { SITE_TITLE, NAVIGATION_LINKS, GITHUB_URL } from '@/utils/constants';
import { NavLink } from '@/types';
import { Button } from '@/components/ui/Button';

const navStyles: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  width: '100%',
  maxWidth: 'var(--container-max-width)',
  margin: '0 auto',
  padding: '0 var(--container-padding)',
};

const logoStyles: React.CSSProperties = {
  fontSize: 'var(--font-size-xl)',
  fontWeight: 700,
  color: 'var(--accent-primary)',
  textDecoration: 'none',
};

const navLinksContainerStyles: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: 'var(--spacing-lg)',
};

const navLinkStyles: React.CSSProperties = {
  color: 'var(--text-primary)',
  textDecoration: 'none',
  fontSize: 'var(--font-size-base)',
  fontWeight: 500,
  transition: 'color 0.2s ease',
  cursor: 'pointer',
};

const mobileMenuButtonStyles: React.CSSProperties = {
  display: 'none',
  background: 'none',
  border: 'none',
  cursor: 'pointer',
  padding: 'var(--spacing-sm)',
  color: 'var(--text-primary)',
};

const mobileNavStyles: React.CSSProperties = {
  position: 'absolute',
  top: '100%',
  left: 0,
  right: 0,
  backgroundColor: 'var(--bg-primary)',
  borderTop: '1px solid var(--border-color)',
  boxShadow: 'var(--shadow-md)',
  padding: 'var(--spacing-md)',
  display: 'flex',
  flexDirection: 'column',
  gap: 'var(--spacing-md)',
  zIndex: 100,
};

const handleSmoothScroll = (e: React.MouseEvent<HTMLAnchorElement>, href: string) => {
  if (!href.startsWith('#')) return;

  e.preventDefault();
  const targetId = href.substring(1);
  const targetElement = document.getElementById(targetId);

  if (targetElement) {
    targetElement.scrollIntoView({ behavior: 'smooth' });
  }
};

export const Navigation: React.FC = () => {
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);

  const toggleMobileMenu = () => {
    setIsMobileMenuOpen(!isMobileMenuOpen);
  };

  const renderNavLink = (link: NavLink, isMobile = false) => {
    if (link.isExternal) {
      return (
        <a
          key={link.label}
          href={link.href}
          style={navLinkStyles}
          target="_blank"
          rel="noopener noreferrer"
          aria-label={`${link.label} (opens in new tab)`}
        >
          {link.label}
        </a>
      );
    }

    return (
      <a
        key={link.label}
        href={link.href}
        style={navLinkStyles}
        onClick={(e) => {
          handleSmoothScroll(e, link.href);
          if (isMobile) setIsMobileMenuOpen(false);
        }}
      >
        {link.label}
      </a>
    );
  };

  const internalLinks = NAVIGATION_LINKS.filter(link => !link.isExternal);

  return (
    <nav style={navStyles} aria-label="Main navigation">
      <a href="#hero" style={logoStyles} aria-label={`${SITE_TITLE} home`}>
        {SITE_TITLE}
      </a>

      <div
        style={navLinksContainerStyles}
        className="nav-links-desktop"
      >
        {internalLinks.map(link => renderNavLink(link))}
        <Button
          href={GITHUB_URL}
          isExternal
          variant="primary"
          size="sm"
          aria-label="View MirDB on GitHub (opens in new tab)"
        >
          GitHub
        </Button>
      </div>

      <button
        style={mobileMenuButtonStyles}
        className="mobile-menu-button"
        onClick={toggleMobileMenu}
        aria-expanded={isMobileMenuOpen}
        aria-controls="mobile-navigation"
        aria-label={isMobileMenuOpen ? 'Close menu' : 'Open menu'}
      >
        <svg
          width="24"
          height="24"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          aria-hidden="true"
        >
          {isMobileMenuOpen ? (
            <>
              <line x1="18" y1="6" x2="6" y2="18" />
              <line x1="6" y1="6" x2="18" y2="18" />
            </>
          ) : (
            <>
              <line x1="3" y1="12" x2="21" y2="12" />
              <line x1="3" y1="6" x2="21" y2="6" />
              <line x1="3" y1="18" x2="21" y2="18" />
            </>
          )}
        </svg>
      </button>

      {isMobileMenuOpen && (
        <div
          id="mobile-navigation"
          style={mobileNavStyles}
          className="mobile-nav"
        >
          {internalLinks.map(link => renderNavLink(link, true))}
          <Button
            href={GITHUB_URL}
            isExternal
            variant="primary"
            size="sm"
            aria-label="View MirDB on GitHub (opens in new tab)"
          >
            GitHub
          </Button>
        </div>
      )}
    </nav>
  );
};

export default Navigation;
