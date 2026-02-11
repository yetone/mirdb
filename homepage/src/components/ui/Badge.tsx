/**
 * Badge component for displaying build status.
 * Owner: Scenario 6 - Build Status Badge
 *
 * Requirements:
 * - REQ-8: CircleCI build status badge
 * - US-6: Click to open CI build page
 */

import { classNames } from '../../utils'
import './Badge.css'

export interface BadgeProps {
  src: string
  alt: string
  href: string
  className?: string
}

export function Badge({ src, alt, href, className }: BadgeProps) {
  return (
    <a
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      className={classNames('badge', className)}
    >
      <img src={src} alt={alt} className="badge__image" />
    </a>
  )
}
