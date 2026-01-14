import { useContext } from 'react'
import { ThemeContext, type Theme } from '../contexts/ThemeContext'

interface BackgroundEffectProps {
  'data-testid'?: string
}

// List of dark themes for fallback calculation
const DARK_THEMES: Theme[] = ['dark', 'cyberpunk', 'synthwave']

/**
 * BackgroundEffect - A component that renders subtle, animated background visual effects
 * Features gradient orbs and noise texture that adapt to the current theme
 * Positioned behind all content with negative z-index (z-[-1])
 *
 * NFR-4 compliant: Maintains consistent design language with existing UI components
 * REQ-9 compliant: Integrates with existing theme system (dark mode support)
 *
 * Note: This component gracefully handles cases where ThemeProvider is not present
 * by falling back to light theme, making it safe to use in tests without full context setup.
 */
const BackgroundEffect = ({ 'data-testid': testId = 'background-effect' }: BackgroundEffectProps) => {
  // Use context directly with fallback for cases when ThemeProvider is not available
  const themeContext = useContext(ThemeContext)

  // Fallback to light theme if no ThemeProvider is present
  const theme: Theme = themeContext?.theme ?? 'light'
  const isDarkMode = themeContext?.isDarkMode ?? DARK_THEMES.includes(theme)

  // Theme-specific colors for gradient orbs
  const getOrbColors = () => {
    switch (theme) {
      case 'cyberpunk':
        return {
          primary: 'from-cyan-500/20 to-cyan-500/5',
          secondary: 'from-yellow-400/15 to-yellow-400/5',
          accent: 'from-pink-500/10 to-pink-500/5',
        }
      case 'synthwave':
        return {
          primary: 'from-purple-500/20 to-purple-500/5',
          secondary: 'from-pink-500/15 to-pink-500/5',
          accent: 'from-blue-400/10 to-blue-400/5',
        }
      case 'dark':
        return {
          primary: 'from-blue-600/15 to-blue-600/5',
          secondary: 'from-purple-600/10 to-purple-600/5',
          accent: 'from-indigo-500/10 to-indigo-500/5',
        }
      default: // light
        return {
          primary: 'from-blue-400/20 to-blue-400/5',
          secondary: 'from-purple-400/15 to-purple-400/5',
          accent: 'from-pink-300/10 to-pink-300/5',
        }
    }
  }

  const orbColors = getOrbColors()

  return (
    <div
      data-testid={testId}
      data-theme-mode={isDarkMode ? 'dark' : 'light'}
      data-theme={theme}
      className="fixed inset-0 z-[-1] overflow-hidden pointer-events-none"
      aria-hidden="true"
    >
      {/* Primary gradient orb - top left */}
      <div
        data-testid="background-orb-primary"
        className={`
          absolute
          -top-40 -left-40
          w-96 h-96
          rounded-full
          bg-gradient-radial ${orbColors.primary}
          blur-3xl
          animate-pulse-slow
        `.replace(/\s+/g, ' ').trim()}
      />

      {/* Secondary gradient orb - bottom right */}
      <div
        data-testid="background-orb-secondary"
        className={`
          absolute
          -bottom-40 -right-40
          w-80 h-80
          rounded-full
          bg-gradient-radial ${orbColors.secondary}
          blur-3xl
          animate-pulse-slower
        `.replace(/\s+/g, ' ').trim()}
      />

      {/* Accent gradient orb - center */}
      <div
        data-testid="background-orb-accent"
        className={`
          absolute
          top-1/2 left-1/2
          -translate-x-1/2 -translate-y-1/2
          w-64 h-64
          rounded-full
          bg-gradient-radial ${orbColors.accent}
          blur-3xl
          animate-float
        `.replace(/\s+/g, ' ').trim()}
      />

      {/* Subtle noise texture overlay */}
      <div
        data-testid="background-noise"
        className={`
          absolute inset-0
          opacity-${isDarkMode ? '30' : '20'}
          mix-blend-soft-light
        `.replace(/\s+/g, ' ').trim()}
        style={{
          backgroundImage: `url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.65' numOctaves='3' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)'/%3E%3C/svg%3E")`,
          backgroundRepeat: 'repeat',
        }}
      />
    </div>
  )
}

export default BackgroundEffect
