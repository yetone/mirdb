/**
 * URL analytics page component.
 *
 * Stub implementation - full implementation by another scenario.
 */

import React from 'react'
import { useParams } from 'react-router-dom'

export function UrlStats() {
  const { shortCode } = useParams<{ shortCode: string }>()

  return (
    <div className="container mx-auto p-6">
      <h1 className="text-3xl font-bold">URL Statistics</h1>
      <p className="text-base-content/70 mt-2">Stats for {shortCode} coming soon</p>
    </div>
  )
}

export default UrlStats
