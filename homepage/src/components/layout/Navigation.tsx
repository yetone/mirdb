/**
 * Navigation Component
 * Owner: Scenario 4 - Navigation and Header
 *
 * Desktop navigation with smooth scroll links.
 *
 * Expected props:
 * - items: NavItem[]
 * - activeSection?: string
 */

import { NavItem } from '../../types';
import { cn } from '../../utils';

export interface NavigationProps {
  items: NavItem[];
  activeSection?: string;
  onNavClick?: (sectionId: string) => void;
  className?: string;
}

export function Navigation({ items, activeSection, onNavClick, className }: NavigationProps) {
  const handleClick = (e: React.MouseEvent<HTMLAnchorElement>, item: NavItem) => {
    e.preventDefault();
    const sectionId = item.href.replace('#', '');

    if (onNavClick) {
      onNavClick(sectionId);
    } else {
      const element = document.getElementById(sectionId);
      if (element) {
        element.scrollIntoView({ behavior: 'smooth' });
      }
    }
  };

  return (
    <nav
      className={cn('hidden md:flex items-center space-x-1', className)}
      aria-label="Main navigation"
      data-testid="desktop-navigation"
    >
      {items.map((item) => {
        const isActive = activeSection === item.id;
        return (
          <a
            key={item.id}
            href={item.href}
            onClick={(e) => handleClick(e, item)}
            className={cn(
              'px-3 py-2 text-sm font-medium rounded-md transition-colors',
              isActive
                ? 'text-primary-600 bg-primary-50 dark:text-primary-400 dark:bg-primary-900/20'
                : 'text-gray-700 hover:text-primary-600 hover:bg-gray-100 dark:text-gray-300 dark:hover:text-primary-400 dark:hover:bg-gray-800'
            )}
            aria-current={isActive ? 'page' : undefined}
            data-testid={`nav-link-${item.id}`}
          >
            {item.label}
          </a>
        );
      })}
    </nav>
  );
}
