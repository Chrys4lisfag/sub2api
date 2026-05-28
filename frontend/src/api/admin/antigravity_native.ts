/**
 * Admin Antigravity Native API endpoints
 *
 * Mirrors the legacy `antigravity` admin OAuth flow but talks to the
 * `/admin/antigravity-native/oauth/*` endpoints exposed by the backend.
 *
 * The wire shape is identical (same field names, same response schema)
 * because the native backend reuses the legacy DTOs to keep the admin UI
 * minimally diverged. Only the URL prefix differs.
 */

import { apiClient } from '../client'

export interface AntigravityNativeAuthUrlResponse {
  auth_url: string
  session_id: string
  state: string
}

export interface AntigravityNativeAuthUrlRequest {
  proxy_id?: number
}

export interface AntigravityNativeExchangeCodeRequest {
  session_id: string
  state: string
  code: string
  proxy_id?: number
}

export interface AntigravityNativeTokenInfo {
  access_token?: string
  refresh_token?: string
  token_type?: string
  expires_at?: number | string
  expires_in?: number
  project_id?: string
  email?: string
  // Native-only fields populated by agymimic's Tokens struct.
  installation_id?: string
  instance_label?: string
  connection_id?: string
  tier_id?: string
  [key: string]: unknown
}

export async function generateAuthUrl(
  payload: AntigravityNativeAuthUrlRequest
): Promise<AntigravityNativeAuthUrlResponse> {
  const { data } = await apiClient.post<AntigravityNativeAuthUrlResponse>(
    '/admin/antigravity-native/oauth/auth-url',
    payload
  )
  return data
}

export async function exchangeCode(
  payload: AntigravityNativeExchangeCodeRequest
): Promise<AntigravityNativeTokenInfo> {
  const { data } = await apiClient.post<AntigravityNativeTokenInfo>(
    '/admin/antigravity-native/oauth/exchange-code',
    payload
  )
  return data
}

export async function refreshAntigravityNativeToken(
  refreshToken: string,
  proxyId?: number | null
): Promise<AntigravityNativeTokenInfo> {
  const payload: Record<string, any> = { refresh_token: refreshToken }
  if (proxyId) payload.proxy_id = proxyId

  const { data } = await apiClient.post<AntigravityNativeTokenInfo>(
    '/admin/antigravity-native/oauth/refresh-token',
    payload
  )
  return data
}

export default { generateAuthUrl, exchangeCode, refreshAntigravityNativeToken }
