import React, { useEffect } from 'react';
import { NAV_ITEMS } from '@/utils/constants';
import styles from './MobileMenu.module.css';

interface MobileMenuProps {
  isOpen: boolean;
  onClose: () => void;
  onNavClick: (e: React.MouseEvent<HTMLAnchorElement>, href: string, external?: boolean) => void;
}

export function MobileMenu({ isOpen, onClose, onNavClick }: MobileMenuProps) {
  // Close menu on escape key
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

  const handleNavItemClick = (e: React.MouseEvent<HTMLAnchorElement>, href: string, external?: boolean) => {
    onNavClick(e, href, external);
    if (!external) {
      onClose();
    }
  };

  return (
    <>
      {/* Backdrop */}
      <div
        className={`${styles.backdrop} ${isOpen ? styles.visible : ''}`}
        onClick={onClose}
        aria-hidden="true"
      />

      {/* Menu panel */}
      <div
        id="mobile-menu"
        className={`${styles.menu} ${isOpen ? styles.open : ''}`}
        aria-hidden={!isOpen}
        role="dialog"
        aria-label="Mobile navigation menu"
      >
        <nav aria-label="Mobile navigation">
          <ul className={styles.navList} role="menu">
            {NAV_ITEMS.map((item) => (
              <li key={item.label} role="none">
                <a
                  href={item.href}
                  className={styles.navLink}
                  role="menuitem"
                  target={item.external ? '_blank' : undefined}
                  rel={item.external ? 'noopener noreferrer' : undefined}
                  onClick={(e) => handleNavItemClick(e, item.href, item.external)}
                  tabIndex={isOpen ? 0 : -1}
                  data-testid={`mobile-nav-link-${item.label.toLowerCase()}`}
                >
                  {item.label}
                  {item.external && (
                    <svg
                      className={styles.externalIcon}
                      width="14"
                      height="14"
                      viewBox="0 0 12 12"
                      fill="none"
                      xmlns="http://www.w3.org/2000/svg"
                      aria-hidden="true"
                    >
                      <path
                        d="M10.5 1.5L1.5 10.5M10.5 1.5H4.5M10.5 1.5V7.5"
                        stroke="currentColor"
                        strokeWidth="1.5"
                        strokeLinecap="round"
                        strokeLinejoin="round"
                      />
                    </svg>
                  )}
                </a>
              </li>
            ))}
          </ul>
        </nav>
      </div>
    </>
  );
}
