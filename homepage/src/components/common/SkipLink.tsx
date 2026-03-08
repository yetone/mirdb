import React from 'react'

interface SkipLinkProps {
  href?: string
  children?: React.ReactNode
}

export const SkipLink: React.FC<SkipLinkProps> = ({
  href = '#main-content',
  children = 'Skip to main content',
}) => {
  return (
    <a href={href} className="skip-link">
      {children}
    </a>
  )
}
