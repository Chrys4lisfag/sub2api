import { beforeEach, describe, expect, it, vi } from 'vitest'

const api = vi.hoisted(() => ({
  generateAuthUrl: vi.fn(),
  exchangeCode: vi.fn(),
  refreshToken: vi.fn()
}))

vi.mock('@/api/admin', () => ({
  adminAPI: {
    antigravityNative: {
      generateAuthUrl: api.generateAuthUrl,
      exchangeCode: api.exchangeCode,
      refreshAntigravityNativeToken: api.refreshToken
    }
  }
}))

vi.mock('@/stores/app', () => ({
  useAppStore: () => ({ showError: vi.fn() })
}))

vi.mock('vue-i18n', () => ({
  useI18n: () => ({ t: (key: string) => key })
}))

import { useAntigravityNativeOAuth } from '../useAntigravityNativeOAuth'

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  const promise = new Promise<T>((res) => {
    resolve = res
  })
  return { promise, resolve }
}

interface AuthURLResponse {
  auth_url: string
  session_id: string
  state: string
}

const response = (suffix: string): AuthURLResponse => ({
  auth_url: `https://oauth.example/${suffix}`,
  session_id: `session-${suffix}`,
  state: `state-${suffix}`
})

describe('useAntigravityNativeOAuth operation ownership', () => {
  beforeEach(() => vi.clearAllMocks())

  it('does not restore stale OAuth state after reset', async () => {
    const pending = deferred<AuthURLResponse>()
    api.generateAuthUrl.mockReturnValue(pending.promise)
    const oauth = useAntigravityNativeOAuth()

    const result = oauth.generateAuthUrl(null)
    oauth.resetState()
    pending.resolve(response('old'))

    await expect(result).resolves.toBe(false)
    expect(oauth.authUrl.value).toBe('')
    expect(oauth.sessionId.value).toBe('')
    expect(oauth.state.value).toBe('')
  })

  it('keeps the newest result when requests finish out of order', async () => {
    const oldRequest = deferred<AuthURLResponse>()
    const newRequest = deferred<AuthURLResponse>()
    api.generateAuthUrl
      .mockReturnValueOnce(oldRequest.promise)
      .mockReturnValueOnce(newRequest.promise)
    const oauth = useAntigravityNativeOAuth()

    const oldResult = oauth.generateAuthUrl(null)
    const newResult = oauth.generateAuthUrl(null)
    newRequest.resolve(response('new'))
    await expect(newResult).resolves.toBe(true)
    oldRequest.resolve(response('old'))
    await expect(oldResult).resolves.toBe(false)

    expect(oauth.authUrl.value).toBe('https://oauth.example/new')
    expect(oauth.sessionId.value).toBe('session-new')
    expect(oauth.state.value).toBe('state-new')
  })
})
