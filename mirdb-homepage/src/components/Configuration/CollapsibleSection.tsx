/**
 * Collapsible Section Component (React Island).
 * Owner: Scenario 5 - Configuration Reference Section
 *
 * Features:
 * - Expand/collapse toggle
 * - Smooth animation
 * - Keyboard accessible
 * - ARIA attributes for screen readers
 */
import { useState, useRef, useEffect, type ReactNode } from 'react';

interface CollapsibleSectionProps {
  title: string;
  children: ReactNode;
  defaultOpen?: boolean;
  testId?: string;
}

export function CollapsibleSection({
  title,
  children,
  defaultOpen = false,
  testId,
}: CollapsibleSectionProps) {
  const [isOpen, setIsOpen] = useState(defaultOpen);
  const contentRef = useRef<HTMLDivElement>(null);
  const [contentHeight, setContentHeight] = useState<number | undefined>(
    defaultOpen ? undefined : 0
  );

  useEffect(() => {
    if (contentRef.current) {
      setContentHeight(isOpen ? contentRef.current.scrollHeight : 0);
    }
  }, [isOpen]);

  const handleToggle = () => {
    setIsOpen(!isOpen);
  };

  const handleKeyDown = (event: React.KeyboardEvent) => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      handleToggle();
    }
  };

  const sectionId = `section-${title.toLowerCase().replace(/\s+/g, '-')}`;
  const contentId = `${sectionId}-content`;

  return (
    <div
      className="border border-border rounded-lg overflow-hidden mb-4"
      data-testid={testId || `collapsible-${sectionId}`}
    >
      <button
        type="button"
        onClick={handleToggle}
        onKeyDown={handleKeyDown}
        aria-expanded={isOpen}
        aria-controls={contentId}
        className="w-full flex items-center justify-between px-4 py-3 bg-surface hover:bg-opacity-80 text-text-primary font-mono text-left transition-colors duration-200 focus:outline-none focus-visible:ring-2 focus-visible:ring-accent"
        data-testid={`${testId || sectionId}-toggle`}
      >
        <span className="font-medium">{title}</span>
        <span
          className={`transform transition-transform duration-200 ${
            isOpen ? 'rotate-180' : 'rotate-0'
          }`}
          aria-hidden="true"
        >
          <ChevronIcon />
        </span>
      </button>
      <div
        id={contentId}
        ref={contentRef}
        role="region"
        aria-labelledby={sectionId}
        style={{ maxHeight: contentHeight !== undefined ? `${contentHeight}px` : 'none' }}
        className="overflow-hidden transition-[max-height] duration-300 ease-in-out"
        data-testid={`${testId || sectionId}-content`}
      >
        <div className="px-4 py-3 bg-background">{children}</div>
      </div>
    </div>
  );
}

function ChevronIcon() {
  return (
    <svg
      width="20"
      height="20"
      viewBox="0 0 20 20"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      className="text-text-secondary"
    >
      <path
        d="M5 7.5L10 12.5L15 7.5"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

export default CollapsibleSection;
