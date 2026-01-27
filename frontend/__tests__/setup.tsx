import '@testing-library/jest-dom'
import { vi } from 'vitest'

// Mock framer-motion to avoid animation issues in tests
vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => {
      const { initial, animate, whileHover, whileTap, whileInView, viewport, transition, ...validProps } = props as any
      return <div {...validProps}>{children}</div>
    },
    span: ({ children, ...props }: React.HTMLAttributes<HTMLSpanElement>) => {
      const { initial, animate, whileHover, whileTap, whileInView, viewport, transition, ...validProps } = props as any
      return <span {...validProps}>{children}</span>
    },
    button: ({ children, ...props }: React.ButtonHTMLAttributes<HTMLButtonElement>) => {
      const { initial, animate, whileHover, whileTap, whileInView, viewport, transition, ...validProps } = props as any
      return <button {...validProps}>{children}</button>
    },
  },
  AnimatePresence: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}))
