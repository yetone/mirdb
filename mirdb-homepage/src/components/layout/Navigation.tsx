/**
 * Navigation Component.
 * Owner: Scenario 9 - Navigation and Footer
 *
 * Features:
 * - Navigation links with smooth scroll
 * - Mobile-responsive menu
 * - Active section highlighting
 */

import { useSmoothScroll } from '../../hooks/useSmoothScroll';
import type { NavItem } from '../../types';

export interface NavigationProps {
  items: NavItem[];
  className?: string;
}

export function Navigation({ items, className = '' }: NavigationProps) {
  const { scrollTo } = useSmoothScroll();

  const handleClick = (e: React.MouseEvent<HTMLAnchorElement>, href: string) => {
    // Only handle anchor links (starting with #)
    if (href.startsWith('#')) {
      e.preventDefault();
      scrollTo(href);
    }
    // External links will work normally
  };

  return (
    <nav className={className} aria-label="Main navigation" data-testid="navigation">
      <ul className="flex items-center gap-6">
        {items.map((item) => {
          const isExternal = !item.href.startsWith('#');
          return (
            <li key={item.label}>
              <a
                href={item.href}
                onClick={(e) => handleClick(e, item.href)}
                className="text-gray-300 hover:text-white transition-colors font-medium"
                data-testid={`nav-link-${item.label.toLowerCase().replace(/\s+/g, '-')}`}
                {...(isExternal && {
                  target: '_blank',
                  rel: 'noopener noreferrer',
                })}
              >
                {item.label}
              </a>
            </li>
          );
        })}
      </ul>
    </nav>
  );
}
