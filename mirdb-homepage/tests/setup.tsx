import '@testing-library/jest-dom'
import { ReactElement } from 'react'
import { render, RenderOptions } from '@testing-library/react'
import { ThemeProvider } from '../src/context'

// Custom render function that wraps with providers
interface CustomRenderOptions extends Omit<RenderOptions, 'wrapper'> {
  withProviders?: boolean
}

function AllProviders({ children }: { children: React.ReactNode }) {
  return <ThemeProvider>{children}</ThemeProvider>
}

export function customRender(
  ui: ReactElement,
  options: CustomRenderOptions = {}
) {
  const { withProviders = false, ...renderOptions } = options

  if (withProviders) {
    return render(ui, { wrapper: AllProviders, ...renderOptions })
  }

  return render(ui, renderOptions)
}

// Re-export everything from testing-library
export * from '@testing-library/react'

// Override render with custom render
export { customRender as renderWithProviders }
