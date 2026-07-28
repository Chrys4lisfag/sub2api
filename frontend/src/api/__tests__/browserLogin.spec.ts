import { beforeEach, describe, expect, it, vi } from 'vitest'

const { get, post } = vi.hoisted(() => ({
  get: vi.fn(),
  post: vi.fn()
}))

vi.mock('@/api/client', () => ({
  apiClient: { get, post }
}))

import {
  getGoogleAutologinStatus,
  runGoogleAutologin,
  type GoogleAutologinRequest,
  type GoogleAutologinState
} from '@/api/admin/browserLogin'

const idleState: GoogleAutologinState = 'idle'

beforeEach(() => {
  get.mockReset()
  post.mockReset()
})

describe('browser-login Google autologin API', () => {
  it('posts credentials and optional account ID using the active session header', async () => {
    const payload: GoogleAutologinRequest = {
      account_id: 42,
      login: 'admin@example.com',
      password: 'secret-password',
      two_factor_import_code: 'IMPORT-CODE'
    }
    post.mockResolvedValue({ data: { status: 'running', message: 'started' } })

    await expect(runGoogleAutologin('session-a', payload)).resolves.toEqual({
      status: 'running',
      message: 'started'
    })
    expect(post).toHaveBeenCalledWith('/admin/browser-login/google-autologin', payload, {
      headers: { 'X-Browser-Session-ID': 'session-a' }
    })
  })

  it('gets sanitized job status using the active session header', async () => {
    get.mockResolvedValue({ data: { status: 'succeeded', message: 'complete' } })

    await expect(getGoogleAutologinStatus('session-a')).resolves.toEqual({
      status: 'succeeded',
      message: 'complete'
    })
    expect(get).toHaveBeenCalledWith('/admin/browser-login/google-autologin', {
      headers: { 'X-Browser-Session-ID': 'session-a' }
    })
    expect(idleState).toBe('idle')
  })
})
