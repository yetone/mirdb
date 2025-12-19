import React from 'react';
import { GitHubLink } from './GitHubLink';

export interface NavItem {
  id: string;
  label: string;
  href: string;
}

export const navItems: NavItem[] = [
  { id: 'features', label: 'Features', href: '#features' },
  { id: 'quick-start', label: 'Quick Start', href: '#quick-start' },
  { id: 'commands', label: 'Commands', href: '#commands' },
  { id: 'configuration', label: 'Configuration', href: '#configuration' },
];

export const StickyNav: React.FC = () => {
  const handleNavClick = (e: React.MouseEvent<HTMLAnchorElement>, href: string) => {
    e.preventDefault();
    const targetId = href.replace('#', '');
    const targetElement = document.getElementById(targetId);
    if (targetElement) {
      targetElement.scrollIntoView({ behavior: 'smooth' });
    }
  };

  return (
    <nav className="sticky-nav" data-testid="sticky-nav">
      <div className="sticky-nav-container">
        <a href="#" className="nav-logo" onClick={(e) => { e.preventDefault(); window.scrollTo({ top: 0, behavior: 'smooth' }); }}>
          MirDB
        </a>
        <div className="nav-links">
          {navItems.map((item) => (
            <a
              key={item.id}
              href={item.href}
              className="nav-link"
              data-testid={`nav-${item.id}`}
              onClick={(e) => handleNavClick(e, item.href)}
            >
              {item.label}
            </a>
          ))}
          <GitHubLink />
        </div>
      </div>
    </nav>
  );
};

export default StickyNav;
