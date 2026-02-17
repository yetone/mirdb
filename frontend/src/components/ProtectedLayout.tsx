/**
 * Protected layout wrapper for authenticated routes.
 *
 * Redirects unauthenticated users to login.
 */

import React from 'react'
import { Navigate, Outlet } from 'react-router-dom'
import { useAuth } from '../contexts/AuthContext'
import { Navbar } from './layout/Navbar'

export function ProtectedLayout() {
  const { isAuthenticated, isLoading } = useAuth()

  if (isLoading) {
    return <div className="flex items-center justify-center h-screen">Loading...</div>
  }

  if (!isAuthenticated) {
    return <Navigate to="/login" replace />
  }

  return (
    <>
      <Navbar />
      <main>
        <Outlet />
      </main>
    </>
  )
}
