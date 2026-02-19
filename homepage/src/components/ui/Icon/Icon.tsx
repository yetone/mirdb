/**
 * Icon Component
 * Renders SVG icons from predefined paths
 */

import { iconPaths } from './icons'

interface IconProps {
  name: string
  size?: number
  className?: string
  'aria-hidden'?: boolean
}

export function Icon({ name, size = 24, className, 'aria-hidden': ariaHidden = true }: IconProps) {
  const path = iconPaths[name]

  if (!path) {
    console.warn(`Icon "${name}" not found`)
    return null
  }

  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="currentColor"
      className={className}
      aria-hidden={ariaHidden}
    >
      <path d={path} />
    </svg>
  )
}
