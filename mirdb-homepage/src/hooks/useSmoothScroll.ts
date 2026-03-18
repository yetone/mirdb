/**
 * Smooth Scroll Hook.
 * Owner: Scenario 9 - Navigation and Footer
 *
 * Features:
 * - Smooth scroll to anchor links
 * - Offset for sticky header
 *
 * Returns:
 * - scrollTo: (id: string) => void
 */

export interface SmoothScrollReturn {
  scrollTo: (id: string) => void;
}

export function useSmoothScroll(offset: number = 80): SmoothScrollReturn {
  const scrollTo = (id: string) => {
    // Remove the '#' if present
    const targetId = id.startsWith('#') ? id.slice(1) : id;
    const element = document.getElementById(targetId);

    if (element) {
      const elementPosition = element.getBoundingClientRect().top;
      const offsetPosition = elementPosition + window.scrollY - offset;

      window.scrollTo({
        top: offsetPosition,
        behavior: 'smooth',
      });
    }
  };

  return { scrollTo };
}
