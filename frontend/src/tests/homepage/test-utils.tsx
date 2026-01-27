/**
 * Shared test utilities for homepage tests.
 *
 * Provides:
 * - Custom render function with providers (Router, Auth, Theme)
 * - Mock data for testing
 * - Common test helpers
 */

import React from 'react'
import { render, RenderOptions } from '@testing-library/react'
import { BrowserRouter, MemoryRouter } from 'react-router-dom'
import { vi } from 'vitest'

interface WrapperProps {
  children: React.ReactNode
}

interface CustomRenderOptions extends Omit<RenderOptions, 'wrapper'> {
  initialRoute?: string
  useMemoryRouter?: boolean
}

const AllProviders: React.FC<WrapperProps> = ({ children }) => {
  return <BrowserRouter>{children}</BrowserRouter>
}

export const renderWithRouter = (
  ui: React.ReactElement,
  options: CustomRenderOptions = {}
) => {
  const { initialRoute = '/', useMemoryRouter = false, ...renderOptions } = options

  if (useMemoryRouter) {
    return render(
      <MemoryRouter initialEntries={[initialRoute]}>{ui}</MemoryRouter>,
      renderOptions
    )
  }

  return render(ui, { wrapper: AllProviders, ...renderOptions })
}

const customRender = (
  ui: React.ReactElement,
  options?: Omit<RenderOptions, 'wrapper'>
) => render(ui, { wrapper: AllProviders, ...options })

export const mockNavigate = vi.fn()

export * from '@testing-library/react'
export { customRender as render }
