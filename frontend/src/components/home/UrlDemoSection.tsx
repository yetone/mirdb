/**
 * Interactive URL shortening demo section.
 * Owner: Scenario 5 - Interactive URL Shortening Demo
 *
 * Allows visitors to test URL shortening without registration.
 * This is a stub - implementation will be done by Scenario 5.
 */

import React from 'react'

export interface UrlDemoSectionProps {
  onRegisterPrompt?: () => void
}

export function UrlDemoSection({ onRegisterPrompt: _onRegisterPrompt }: UrlDemoSectionProps) {
  return (
    <section id="demo" className="py-16" aria-labelledby="demo-heading">
      <h2 id="demo-heading" className="sr-only">Try URL Shortening</h2>
      {/* Implementation by Scenario 5 */}
    </section>
  )
}
