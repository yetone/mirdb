/**
 * Admin route guard component.
 *
 * Restricts access to admin-only routes.
 */

import React from 'react'
import { Navigate, Outlet } from 'react-router-dom'
import { useAuth } from '../contexts/AuthContext'

export function AdminRoute() {
  const { user, isLoading } = useAuth()

  if (isLoading) {
    return <div className="flex items-center justify-center h-screen">Loading...</div>
  }

  if (!user?.is_admin) {
    return <Navigate to="/dashboard" replace />
  }

  return <Outlet />
}
