/**
 * Navigation component for desktop view.
 * Owner: Scenario 1 - Header and Navigation
 *
 * Requirements:
 * - REQ-2: Links to Documentation, Examples, GitHub, About
 * - NFR-6: External links with rel="noopener noreferrer"
 */

import type { NavItem } from '../../types'
import './Navigation.css'

interface NavigationProps {
  items: NavItem[]
}

export function Navigation({ items }: NavigationProps) {
  return (
    <nav className="navigation" aria-label="Main navigation">
      <ul className="navigation__list">
        {items.map((item) => (
          <li key={item.label} className="navigation__item">
            <a
              href={item.href}
              className="navigation__link"
              target={item.external ? '_blank' : undefined}
              rel={item.external ? 'noopener noreferrer' : undefined}
            >
              {item.label}
            </a>
          </li>
        ))}
      </ul>
    </nav>
  )
}
