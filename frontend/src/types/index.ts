export interface User {
  id: number;
  email: string;
  is_admin: boolean;
}

export interface AuthState {
  user: User | null;
  token: string | null;
  isAuthenticated: boolean;
  login: (email: string, password: string) => Promise<void>;
  logout: () => void;
  register: (email: string, password: string) => Promise<void>;
}

export type Theme = 'light' | 'dark' | 'cyberpunk' | 'synthwave' | 'retro' | 'valentine';

export interface ThemeState {
  theme: Theme;
  setTheme: (theme: Theme) => void;
}

export interface Feature {
  id: string;
  title: string;
  description: string;
  icon: React.ReactNode;
}
