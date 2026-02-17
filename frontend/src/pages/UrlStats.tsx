/**
 * URL Stats page component.
 * Stub implementation for routing.
 */

import React from 'react'
import { useParams } from 'react-router-dom'

export function UrlStats() {
  const { shortCode } = useParams<{ shortCode: string }>()

  return (
    <div className="p-8">
      <h1 className="text-2xl font-bold">URL Stats: {shortCode}</h1>
      <p>URL statistics would go here.</p>
    </div>
  )
}

export default UrlStats
