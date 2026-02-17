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
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ThemeProvider } from './contexts/ThemeContext'
import { AuthProvider } from './contexts/AuthContext'
import Home from './pages/Home'
import Login from './pages/Login'
import Register from './pages/Register'
import Dashboard from './pages/Dashboard'
import UrlStats from './pages/UrlStats'
import Settings from './pages/Settings'

const queryClient = new QueryClient()

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider>
        <AuthProvider>
          <BrowserRouter>
            <Routes>
              <Route path="/" element={<Home />} />
              <Route path="/login" element={<Login />} />
              <Route path="/register" element={<Register />} />
              <Route path="/dashboard" element={<Dashboard />} />
              <Route path="/stats/:shortCode" element={<UrlStats />} />
              <Route path="/settings" element={<Settings />} />
            </Routes>
          </BrowserRouter>
        </AuthProvider>
      </ThemeProvider>
    </QueryClientProvider>
  )
}

export default App
