/// <reference types="vite/client" />

export interface User {
  id: number;
  username: string;
  email: string;
  is_admin: number;
}

export interface URL {
  id: number;
  original_url: string;
  short_code: string;
  user_id: number;
  created_at: string;
  clicks: number;
}

export interface LoginResponse {
  access_token: string;
  token_type: string;
}

export type Theme = 'light' | 'dark' | 'cyberpunk' | 'synthwave' | 'retro' | 'valentine' | 'night';
