/**
 * Custom type definitions for the application.
 */

export interface User {
  id: number
  username: string
  email: string
  is_admin: boolean
}

export interface AuthContextType {
  user: User | null
  isAuthenticated: boolean
  isLoading: boolean
  login: (username: string, password: string) => Promise<void>
  logout: () => void
  register: (username: string, email: string, password: string) => Promise<void>
}

export interface ThemeContextType {
  theme: string
  setTheme: (theme: string) => void
}

export interface Url {
  id: number
  original_url: string
  short_code: string
  created_at: string
  click_count: number
  user_id: number
}

export interface UrlStats {
  url: Url
  clicks: Click[]
  total_clicks: number
  unique_visitors: number
  countries: Record<string, number>
  referrers: Record<string, number>
}

export interface Click {
  id: number
  url_id: number
  clicked_at: string
  ip_address: string
  user_agent: string
  referrer: string | null
  country: string | null
  city: string | null
}
