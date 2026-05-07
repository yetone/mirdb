'use client';

import { useEffect, useState } from 'react';

interface UseScrollSpyOptions {
  rootMargin?: string;
  threshold?: number | number[];
}

export function useScrollSpy(
  sectionIds: string[],
  options: UseScrollSpyOptions = {}
): { activeId: string } {
  const [activeId, setActiveId] = useState<string>('');

  useEffect(() => {
    if (typeof window === 'undefined' || !sectionIds.length) {
      return;
    }

    const elements = sectionIds
      .map((id) => document.getElementById(id))
      .filter((el): el is HTMLElement => el !== null);

    if (elements.length === 0) {
      return;
    }

    const visibility = new Map<string, number>();

    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          visibility.set(entry.target.id, entry.intersectionRatio);
        }

        let bestId = '';
        let bestRatio = 0;
        for (const id of sectionIds) {
          const ratio = visibility.get(id) ?? 0;
          if (ratio > bestRatio) {
            bestRatio = ratio;
            bestId = id;
          }
        }
        if (bestId) {
          setActiveId(bestId);
        }
      },
      {
        rootMargin: options.rootMargin ?? '-80px 0px -40% 0px',
        threshold: options.threshold ?? [0, 0.1, 0.25, 0.5, 0.75, 1],
      }
    );

    elements.forEach((el) => observer.observe(el));

    return () => observer.disconnect();
  }, [sectionIds, options.rootMargin, options.threshold]);

  return { activeId };
}
