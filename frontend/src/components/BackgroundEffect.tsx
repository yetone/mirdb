import { useTheme } from '../contexts/ThemeContext'

interface BackgroundEffectProps {
  'data-testid'?: string
}

const themeColors = {
  light: {
    primary: 'from-blue-400/30 to-purple-400/30',
    secondary: 'from-pink-400/30 to-orange-400/30',
    accent: 'from-green-400/30 to-teal-400/30',
  },
  dark: {
    primary: 'from-blue-600/40 to-purple-600/40',
    secondary: 'from-pink-600/40 to-orange-600/40',
    accent: 'from-green-600/40 to-teal-600/40',
  },
  cyberpunk: {
    primary: 'from-yellow-400/40 to-pink-500/40',
    secondary: 'from-cyan-400/40 to-blue-500/40',
    accent: 'from-lime-400/40 to-emerald-500/40',
  },
  synthwave: {
    primary: 'from-purple-500/40 to-pink-500/40',
    secondary: 'from-blue-500/40 to-cyan-500/40',
    accent: 'from-rose-500/40 to-orange-500/40',
  },
}

export default function BackgroundEffect({ 'data-testid': testId }: BackgroundEffectProps) {
  const { theme } = useTheme()
  const colors = themeColors[theme] || themeColors.light

  return (
    <div
      data-testid={testId}
      className="fixed inset-0 -z-10 overflow-hidden pointer-events-none"
      aria-hidden="true"
    >
      {/* Primary gradient orb */}
      <div
        className={`absolute top-1/4 left-1/4 w-96 h-96 bg-gradient-to-br ${colors.primary} rounded-full blur-3xl animate-pulse`}
      />
      {/* Secondary gradient orb */}
      <div
        className={`absolute top-1/2 right-1/4 w-80 h-80 bg-gradient-to-br ${colors.secondary} rounded-full blur-3xl animate-pulse`}
        style={{ animationDelay: '1s' }}
      />
      {/* Accent gradient orb */}
      <div
        className={`absolute bottom-1/4 left-1/2 w-72 h-72 bg-gradient-to-br ${colors.accent} rounded-full blur-3xl animate-pulse`}
        style={{ animationDelay: '2s' }}
      />
      {/* Noise texture overlay */}
      <svg className="absolute inset-0 w-full h-full opacity-[0.03]">
        <filter id="noise">
          <feTurbulence type="fractalNoise" baseFrequency="0.8" numOctaves="4" stitchTiles="stitch" />
        </filter>
        <rect width="100%" height="100%" filter="url(#noise)" />
      </svg>
    </div>
  )
}
