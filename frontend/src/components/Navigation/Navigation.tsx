import { Link } from 'react-router-dom'
import './Navigation.css'

export interface NavLink {
  label: string
  href: string
}

export const NAV_LINKS: NavLink[] = [
  { label: 'Home', href: '/' },
  { label: 'Features', href: '/features' },
  { label: 'About', href: '/about' },
  { label: 'Contact', href: '/contact' },
]

export const Navigation: React.FC = () => {
  return (
    <header className="navigation-header">
      <nav className="navigation" aria-label="Main navigation">
        <Link to="/" className="navigation-logo">
          Homepage Enhancement
        </Link>
        <ul className="navigation-links">
          {NAV_LINKS.map((link) => (
            <li key={link.href}>
              <Link to={link.href} className="navigation-link">
                {link.label}
              </Link>
            </li>
          ))}
        </ul>
      </nav>
    </header>
  )
}
