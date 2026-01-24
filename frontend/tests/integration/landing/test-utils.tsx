/**
 * Integration Test Utilities for Landing Page Tests
 *
 * Provides common test setup, mocks, and utilities for integration tests.
 */

import React, { ReactElement } from 'react'
import { render, RenderOptions } from '@testing-library/react'
import { MemoryRouter, Routes, Route } from 'react-router-dom'
import { ThemeProvider } from '../../../src/contexts/ThemeContext'
import { AuthProvider } from '../../../src/contexts/AuthContext'
import Home from '../../../src/pages/Home'
import Login from '../../../src/pages/Login'
import Register from '../../../src/pages/Register'

// Custom render options for integration tests
interface IntegrationRenderOptions extends Omit<RenderOptions, 'wrapper'> {
  initialRoute?: string
}

// Test App that provides full routing context with all providers
function TestApp({
  initialRoute = '/',
}: {
  initialRoute?: string
}) {
  return (
    <ThemeProvider>
      <AuthProvider>
        <MemoryRouter initialEntries={[initialRoute]}>
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/login" element={<Login />} />
            <Route path="/register" element={<Register />} />
          </Routes>
        </MemoryRouter>
      </AuthProvider>
    </ThemeProvider>
  )
}

// Render with full app context for integration tests
export function renderWithRouter(options?: IntegrationRenderOptions) {
  const { initialRoute = '/', ...renderOptions } = options || {}

  return render(<TestApp initialRoute={initialRoute} />, renderOptions)
}

// Re-export everything from React Testing Library
export * from '@testing-library/react'
export { default as userEvent } from '@testing-library/user-event'
