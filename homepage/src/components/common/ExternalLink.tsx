/**
 * External Link Component
 * Owner: Scenario 4 - GitHub Repository Links
 *
 * Renders external links with proper security attributes:
 * - target="_blank"
 * - rel="noopener noreferrer"
 */

import type { ExternalLinkProps } from '../../types';

/**
 * ExternalLink component renders anchor tags for external URLs
 * with proper security attributes to prevent tabnapping attacks.
 *
 * @param href - The URL to link to
 * @param children - Link content (text or React elements)
 * @param className - Optional CSS class name
 */
export function ExternalLink({ href, children, className }: ExternalLinkProps) {
  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className={className}
    >
      {children}
    </a>
  );
}

export default ExternalLink;
