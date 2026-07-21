/**
 * Admin Browser-Login API endpoints
 * Drives the standalone browser2webfront stealth-browser stream so an admin
 * can perform an interactive OAuth login inside a streamed browser.
 */

import { apiClient } from '../client'

export interface BrowserLoginSession {
  session_id: string
  vnc_token: string
  vnc_path: string
  profile_id: string
  vnc_url: string
}

export interface BrowserLoginResult {
  callback_url: string | null
  code: string | null
  current_url: string | null
}

/** Start the single browser session (409 upstream if one is already active). */
export async function startSession(payload: {
  proxy_id?: number | null
  profile_id?: string
}): Promise<BrowserLoginSession> {
  const { data } = await apiClient.post<BrowserLoginSession>('/admin/browser-login/session', payload, {
    timeout: 130_000
  })
  return data
}

/** Drive the streamed browser's active tab to a URL (e.g. the OAuth consent page). */
export async function navigate(
  sessionId: string,
  payload: { url: string }
): Promise<{ ok: boolean; warning?: string }> {
  const { data } = await apiClient.post<{ ok: boolean; warning?: string }>('/admin/browser-login/navigate', payload, {
    headers: { 'X-Browser-Session-ID': sessionId },
    timeout: 70_000
  })
  return data
}

/** Poll for the captured OAuth callback code + current URL. */
export async function getResult(sessionId: string): Promise<BrowserLoginResult> {
  const { data } = await apiClient.get<BrowserLoginResult>('/admin/browser-login/result', {
    headers: { 'X-Browser-Session-ID': sessionId }
  })
  return data
}

/** Tear the session down (the profile dir is kept for later re-login). */
export async function stopSession(sessionId: string): Promise<{ ok: boolean }> {
  const { data } = await apiClient.delete<{ ok: boolean }>('/admin/browser-login/session', {
    headers: { 'X-Browser-Session-ID': sessionId }
  })
  return data
}

export const browserLoginAPI = {
  startSession,
  navigate,
  getResult,
  stopSession
}

export default browserLoginAPI
