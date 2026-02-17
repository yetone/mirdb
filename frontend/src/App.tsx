/**
 * Main application component with routing.
 * Owner: Scenario 16 - Route Configuration
 *
 * Configures React Router routes:
 * - / : Home (public)
 * - /login : Login (public)
 * - /register : Register (public)
 * - /dashboard : Dashboard (protected)
 * - /stats/:shortCode : URL stats (protected)
 * - /settings : Admin settings (admin only)
 */

import React from 'react'
import { Routes, Route } from 'react-router-dom'
import { Home } from './pages/Home'
import { Login } from './pages/Login'
import { Register } from './pages/Register'
import { Dashboard } from './pages/Dashboard'
import { UrlStats } from './pages/UrlStats'
import { Settings } from './pages/Settings'
import { ProtectedLayout } from './components/ProtectedLayout'
import { AdminRoute } from './components/AdminRoute'

function App() {
  return (
    <Routes>
      {/* Public routes */}
      <Route path="/" element={<Home />} />
      <Route path="/login" element={<Login />} />
      <Route path="/register" element={<Register />} />

      {/* Protected routes */}
      <Route element={<ProtectedLayout />}>
        <Route path="/dashboard" element={<Dashboard />} />
        <Route path="/stats/:shortCode" element={<UrlStats />} />

        {/* Admin routes */}
        <Route element={<AdminRoute />}>
          <Route path="/settings" element={<Settings />} />
        </Route>
      </Route>
    </Routes>
  )
}

export default App
