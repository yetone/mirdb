// Mock localStorage for JSDOM
const localStorageMock = {
  getItem: () => null,
  setItem: () => {},
  removeItem: () => {},
  clear: () => {},
}
Object.defineProperty(window, 'localStorage', {
  value: localStorageMock,
})

import '@testing-library/jest-dom'
import { cleanup } from '@testing-library/react'
import { afterEach } from 'vitest'
import React from 'react'
import { render, RenderOptions } from '@testing-library/react'
import { MemoryRouter, MemoryRouterProps } from 'react-router-dom'
import { ThemeProvider } from '@/contexts/ThemeContext'
import { AuthProvider } from '@/contexts/AuthContext'

afterEach(() => {
  cleanup()
})

interface AllTheProvidersProps {
  children: React.ReactNode
  initialEntries?: MemoryRouterProps['initialEntries']
}

const AllTheProviders = ({ children, initialEntries = ['/'] }: AllTheProvidersProps) => {
  return React.createElement(
    MemoryRouter,
    { initialEntries },
    React.createElement(
      ThemeProvider,
      null,
      React.createElement(AuthProvider, null, children)
    )
  )
}

interface CustomRenderOptions extends Omit<RenderOptions, 'wrapper'> {
  initialEntries?: MemoryRouterProps['initialEntries']
}

const customRender = (
  ui: React.ReactElement,
  { initialEntries, ...options }: CustomRenderOptions = {}
) => {
  const wrapper = ({ children }: { children: React.ReactNode }) =>
    React.createElement(
      MemoryRouter,
      { initialEntries: initialEntries || ['/'] },
      React.createElement(
        ThemeProvider,
        null,
        React.createElement(AuthProvider, null, children)
      )
    )

  return render(ui, { wrapper, ...options })
}

export * from '@testing-library/react'
export { customRender as render }
