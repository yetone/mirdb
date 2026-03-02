/**
 * Skip to Content Link Component.
 * Owner: Scenario 11 - Keyboard Navigation Accessibility
 *
 * Accessibility feature for keyboard users:
 * - Hidden until focused
 * - First focusable element on page
 * - Skips to main content when activated
 *
 * Requirements: NFR-3, US-6
 */

interface SkipLinkProps {
  /** Target element ID to skip to (without #) */
  targetId?: string;
  /** Custom text for the skip link */
  children?: React.ReactNode;
  /** Additional CSS classes */
  className?: string;
}

export function SkipLink({
  targetId = 'main-content',
  children = 'Skip to content',
  className = '',
}: SkipLinkProps) {
  const focusTarget = () => {
    const target = document.getElementById(targetId);
    if (target) {
      // Set tabindex temporarily to make the element focusable
      target.setAttribute('tabindex', '-1');
      target.focus();
      // Scroll into view for visual users (check if method exists for jsdom compatibility)
      if (typeof target.scrollIntoView === 'function') {
        target.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
    }
  };

  const handleClick = (e: React.MouseEvent<HTMLAnchorElement>) => {
    e.preventDefault();
    focusTarget();
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLAnchorElement>) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      focusTarget();
    }
  };

  return (
    <a
      href={`#${targetId}`}
      onClick={handleClick}
      onKeyDown={handleKeyDown}
      className={`
        skip-link
        fixed top-0 left-0 z-[9999]
        px-4 py-2
        bg-primary text-primary-content
        font-semibold
        rounded-br-lg
        transform -translate-y-full
        focus:translate-y-0
        transition-transform duration-200
        focus:outline-none focus:ring-2 focus:ring-primary focus:ring-offset-2
        ${className}
      `.trim().replace(/\s+/g, ' ')}
      data-testid="skip-link"
    >
      {children}
    </a>
  );
}
