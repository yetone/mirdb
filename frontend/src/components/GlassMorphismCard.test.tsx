import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import GlassMorphismCard from './GlassMorphismCard'

vi.mock('framer-motion', () => ({
  motion: {
    div: ({ children, ...props }: { children: React.ReactNode; [key: string]: unknown }) => (
      <div {...props}>{children}</div>
    ),
  },
}))

describe('GlassMorphismCard', () => {
  it('renders children content', () => {
    render(
      <GlassMorphismCard>
        <span>Test Content</span>
      </GlassMorphismCard>
    )

    expect(screen.getByText('Test Content')).toBeInTheDocument()
  })

  it('applies glassmorphism styling classes', () => {
    render(
      <GlassMorphismCard>
        <span>Test</span>
      </GlassMorphismCard>
    )

    const card = screen.getByTestId('glassmorphism-card')
    expect(card).toHaveClass('backdrop-blur-md')
    expect(card).toHaveClass('bg-base-200/30')
    expect(card).toHaveClass('border')
    expect(card).toHaveClass('rounded-2xl')
    expect(card).toHaveClass('shadow-xl')
    expect(card).toHaveClass('p-6')
  })

  it('accepts custom className prop', () => {
    render(
      <GlassMorphismCard className="custom-class">
        <span>Test</span>
      </GlassMorphismCard>
    )

    const card = screen.getByTestId('glassmorphism-card')
    expect(card).toHaveClass('custom-class')
  })

  it('has data-testid attribute for testing', () => {
    render(
      <GlassMorphismCard>
        <span>Test</span>
      </GlassMorphismCard>
    )

    expect(screen.getByTestId('glassmorphism-card')).toBeInTheDocument()
  })

  it('renders complex children structures', () => {
    render(
      <GlassMorphismCard>
        <div>
          <h3>Title</h3>
          <p>Description</p>
        </div>
      </GlassMorphismCard>
    )

    expect(screen.getByText('Title')).toBeInTheDocument()
    expect(screen.getByText('Description')).toBeInTheDocument()
  })
})
