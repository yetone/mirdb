/**
 * Badge Component
 * Owner: Scenario 6 - Footer and Badges
 *
 * Expected exports:
 * - Badge: React.FC<BadgeProps> - Status badge component
 * - BadgeProps: interface
 *
 * Features:
 * - CI status badge display
 * - Link to CI service
 * - Alt text for accessibility
 */

export interface BadgeProps {
  src: string
  alt: string
  href?: string
}

export function Badge({ src, alt, href }: BadgeProps) {
  const img = <img src={src} alt={alt} />
  if (href) {
    return <a href={href}>{img}</a>
  }
  return img
}
