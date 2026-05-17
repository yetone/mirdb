import { Link } from 'react-router-dom';
import { BRAND, ROUTES, NAV_LINKS } from '../../utils/constants';
import ThemeToggle from '../shared/ThemeToggle';

/**
 * Top navigation bar for the public homepage.
 * Owner: Scenario 2 - Call-to-Action Buttons and Navigation Routing.
 *
 * Renders the MirDB brand link to '/', inline desktop nav links
 * (Login, Register), and the ThemeToggle component.
 */
export default function HomeNavbar() {
  return (
    <header className="navbar bg-base-100 shadow-sm px-4">
      <div className="flex-1">
        <Link
          to={ROUTES.HOME}
          className="btn btn-ghost normal-case text-xl"
          data-testid="navbar-brand"
          aria-label={BRAND.name}
        >
          {BRAND.name}
        </Link>
      </div>
      <nav className="flex-none gap-2" aria-label="Main navigation">
        <ul className="menu menu-horizontal px-1 hidden sm:flex gap-2">
          {NAV_LINKS.map((link) => (
            <li key={link.to}>
              <Link
                to={link.to}
                aria-label={link.ariaLabel}
                data-testid={`navbar-link-${link.label.toLowerCase()}`}
              >
                {link.label}
              </Link>
            </li>
          ))}
        </ul>
        <ThemeToggle />
      </nav>
    </header>
  );
}
