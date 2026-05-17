import { MouseEvent } from 'react';

/**
 * Skip-to-content link.
 * Owner: Scenario 7 - Accessibility and Keyboard Navigation.
 *
 * Renders an anchor that targets '#main-content' on the page <main>.
 * Visually hidden until focused, then revealed in the top-left corner so
 * keyboard users can bypass the navigation. Required by WCAG 2.4.1.
 */

export interface SkipToContentProps {
  targetId?: string;
  label?: string;
}

const DEFAULT_TARGET = 'main-content';
const DEFAULT_LABEL = 'Skip to main content';

export default function SkipToContent({
  targetId = DEFAULT_TARGET,
  label = DEFAULT_LABEL,
}: SkipToContentProps = {}) {
  const handleClick = (event: MouseEvent<HTMLAnchorElement>) => {
    const target = document.getElementById(targetId);
    if (target) {
      event.preventDefault();
      if (!target.hasAttribute('tabindex')) {
        target.setAttribute('tabindex', '-1');
      }
      target.focus();
      if (typeof target.scrollIntoView === 'function') {
        target.scrollIntoView({ behavior: 'auto', block: 'start' });
      }
    }
  };

  return (
    <a
      href={`#${targetId}`}
      onClick={handleClick}
      data-testid="skip-to-content"
      className="skip-to-content sr-only focus:not-sr-only focus-visible:not-sr-only absolute left-2 top-2 z-[9999] rounded-md bg-primary px-4 py-2 font-medium text-primary-content focus:outline focus:outline-2 focus:outline-offset-2"
    >
      {label}
    </a>
  );
}
