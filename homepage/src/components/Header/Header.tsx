/**
 * Header/Navigation Component
 * Owner: Scenario 1 - Hero Section Display
 *
 * Features:
 * - Navigation links to sections (Features, Usage, GitHub)
 * - Responsive menu for mobile
 * - Sticky positioning (optional)
 */

import { PRODUCT_NAME, GITHUB_URL } from '../../utils/constants'
import './Header.css'

export function Header() {
  return (
    <header className="header" role="banner">
      <div className="header-container">
        <a href="/" className="header-brand">
          {PRODUCT_NAME}
        </a>
        <nav className="header-nav" aria-label="Main navigation">
          <ul className="header-nav-list">
            <li>
              <a href="#features" className="header-nav-link">
                Features
              </a>
            </li>
            <li>
              <a href="#installation" className="header-nav-link">
                Installation
              </a>
            </li>
            <li>
              <a href="#usage" className="header-nav-link">
                Usage
              </a>
            </li>
            <li>
              <a
                href={GITHUB_URL}
                className="header-nav-link header-nav-link--github"
                target="_blank"
                rel="noopener noreferrer"
              >
                GitHub
              </a>
            </li>
          </ul>
        </nav>
      </div>
    </header>
  )
}
