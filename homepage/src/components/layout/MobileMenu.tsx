/**
 * Mobile menu component with hamburger toggle.
 * Owner: Scenario 1 - Header and Navigation
 *
 * Requirements:
 * - REQ-13: Responsive navigation for mobile
 * - US-7: Accessible via mobile menu
 */

import type { NavItem } from '../../types'
import './MobileMenu.css'

interface MobileMenuProps {
  items: NavItem[]
  isOpen: boolean
  onClose: () => void
}

export function MobileMenu({ items, isOpen, onClose }: MobileMenuProps) {
  return (
    <>
      {isOpen && (
        <div
          className="mobile-menu__overlay"
          onClick={onClose}
          aria-hidden="true"
        />
      )}
      <div
        className={`mobile-menu ${isOpen ? 'mobile-menu--open' : ''}`}
        role="dialog"
        aria-label="Mobile navigation menu"
        aria-hidden={!isOpen}
      >
        <nav className="mobile-menu__nav">
          <ul className="mobile-menu__list">
            {items.map((item) => (
              <li key={item.label} className="mobile-menu__item">
                <a
                  href={item.href}
                  className="mobile-menu__link"
                  target={item.external ? '_blank' : undefined}
                  rel={item.external ? 'noopener noreferrer' : undefined}
                  onClick={onClose}
                >
                  {item.label}
                </a>
              </li>
            ))}
          </ul>
        </nav>
      </div>
    </>
  )
}

interface HamburgerButtonProps {
  isOpen: boolean
  onClick: () => void
}

export function HamburgerButton({ isOpen, onClick }: HamburgerButtonProps) {
  return (
    <button
      className={`hamburger-button ${isOpen ? 'hamburger-button--open' : ''}`}
      onClick={onClick}
      aria-label={isOpen ? 'Close menu' : 'Open menu'}
      aria-expanded={isOpen}
    >
      <span className="hamburger-button__line" />
      <span className="hamburger-button__line" />
      <span className="hamburger-button__line" />
    </button>
  )
}
