import { useTheme } from '../contexts/ThemeContext'

export function ThemeToggle() {
  const { theme, setTheme } = useTheme()

  const themes = ['light', 'dark', 'cyberpunk', 'synthwave'] as const

  return (
    <div className="dropdown dropdown-end">
      <div tabIndex={0} role="button" className="btn btn-ghost m-1">
        Theme
        <svg className="w-4 h-4 ml-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </div>
      <ul tabIndex={0} className="dropdown-content z-[1] menu p-2 shadow bg-base-200 rounded-box w-52">
        {themes.map((t) => (
          <li key={t}>
            <button
              className={theme === t ? 'active' : ''}
              onClick={() => setTheme(t)}
            >
              {t.charAt(0).toUpperCase() + t.slice(1)}
            </button>
          </li>
        ))}
      </ul>
    </div>
  )
}
