import { defineComponent } from 'vue'
import type * as Vue from 'vue'
import { flushPromises, mount } from '@vue/test-utils'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const api = vi.hoisted(() => ({
  startSession: vi.fn(),
  getResult: vi.fn(),
  stopSession: vi.fn(),
  runGoogleAutologin: vi.fn(),
  getGoogleAutologinStatus: vi.fn(),
  cancelGoogleAutologin: vi.fn(),
  navigate: vi.fn()
}))

const oauth = vi.hoisted(() => ({
  generateAuthUrl: vi.fn(),
  exchangeAuthCode: vi.fn(),
  buildCredentials: vi.fn(),
  resetState: vi.fn()
}))

vi.mock('@/api/admin/browserLogin', () => ({
  default: api
}))

vi.mock('@/composables/useAntigravityNativeOAuth', async () => {
  const { ref } = await vi.importActual<typeof Vue>('vue')
  return {
    useAntigravityNativeOAuth: () => ({
      authUrl: ref(''),
      sessionId: ref(''),
      state: ref(''),
      generateAuthUrl: oauth.generateAuthUrl,
      exchangeAuthCode: oauth.exchangeAuthCode,
      buildCredentials: oauth.buildCredentials,
      resetState: oauth.resetState
    })
  }
})

vi.mock('@/stores/app', () => ({
  useAppStore: () => ({ showError: vi.fn(), showSuccess: vi.fn() })
}))

import BrowserLoginModal from '../BrowserLoginModal.vue'

const BaseDialogStub = defineComponent({
  props: { show: Boolean },
  template: '<div v-if="show"><slot /><slot name="footer" /></div>'
})

const session = {
  session_id: 'session-a',
  vnc_token: 'password',
  vnc_path: '/websockify',
  profile_id: 'profile-a',
  vnc_url: 'http://browser.example:6080'
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

type ModalOverrides = Partial<{
  profileId: string
  mode: 'login' | 'launch'
  accountName: string
  accountId: number
  proxyId: number | null
  initialGoogleLogin: string
  initialGooglePassword: string
  initialGoogle2faImportCode: string
}>

function mountModal(props: ModalOverrides = {}) {
  return mount(BrowserLoginModal, {
    props: { show: false, proxyId: null, ...props },
    global: { stubs: { BaseDialog: BaseDialogStub } }
  })
}

describe('BrowserLoginModal lifecycle', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    api.startSession.mockResolvedValue(session)
    api.getResult.mockResolvedValue({ callback_url: null, code: null, current_url: 'about:blank' })
    api.stopSession.mockResolvedValue({ ok: true })
    api.runGoogleAutologin.mockResolvedValue({ status: 'running' })
    api.getGoogleAutologinStatus.mockResolvedValue({ status: 'succeeded' })
    api.cancelGoogleAutologin.mockResolvedValue({ status: 'canceled' })
    oauth.generateAuthUrl.mockResolvedValue(false)
    oauth.exchangeAuthCode.mockResolvedValue(null)
    oauth.buildCredentials.mockReturnValue({})
  })

  it('scopes polling and cleanup to the started session', async () => {
    const wrapper = mountModal()
    await wrapper.setProps({ show: true })
    await flushPromises()

    expect(api.startSession).toHaveBeenCalledOnce()
    await wrapper.setProps({ show: false })
    await flushPromises()
    expect(api.stopSession).toHaveBeenCalledWith('session-a')

    wrapper.unmount()
  })

  it('restarts for a changed account identity and rejects a stale start', async () => {
    const pendingSecondStart = deferred<typeof session>()
    const sessionB = {
      ...session,
      session_id: 'session-b',
      profile_id: 'profile-b',
      vnc_token: 'password-b'
    }
    const sessionC = {
      ...session,
      session_id: 'session-c',
      profile_id: 'profile-c',
      vnc_token: 'password-c'
    }
    api.startSession
      .mockResolvedValueOnce(session)
      .mockReturnValueOnce(pendingSecondStart.promise)
      .mockResolvedValueOnce(sessionC)
    const wrapper = mountModal({
      mode: 'launch',
      accountId: 1,
      proxyId: 11,
      profileId: 'profile-a'
    })

    await wrapper.setProps({ show: true })
    await flushPromises()
    await wrapper.setProps({ accountId: 2, proxyId: 22, profileId: 'profile-b' })
    await flushPromises()

    expect(api.stopSession).toHaveBeenCalledWith('session-a')
    expect(api.startSession).toHaveBeenNthCalledWith(2, {
      account_id: 2,
      proxy_id: 22,
      profile_id: 'profile-b'
    })

    await wrapper.setProps({ accountId: 3, proxyId: 33, profileId: 'profile-c' })
    await flushPromises()
    expect(api.startSession).toHaveBeenNthCalledWith(3, {
      account_id: 3,
      proxy_id: 33,
      profile_id: 'profile-c'
    })
    expect(wrapper.get('iframe').attributes('src')).toContain('password=password-c')

    pendingSecondStart.resolve(sessionB)
    await flushPromises()
    expect(api.stopSession).toHaveBeenCalledWith('session-b')
    expect(wrapper.get('iframe').attributes('src')).toContain('password=password-c')
    expect(wrapper.get('iframe').attributes('src')).not.toContain('password=password-b')

    wrapper.unmount()
  })

  it('closes a session that finishes starting after the modal was closed', async () => {
    const { promise, resolve: finishStart } = deferred<typeof session>()
    api.startSession.mockReturnValue(promise)
    const wrapper = mountModal()

    await wrapper.setProps({ show: true })
    await wrapper.setProps({ show: false })
    finishStart(session)
    await flushPromises()

    expect(api.stopSession).toHaveBeenCalledWith('session-a')
    wrapper.unmount()
  })

  it('retries until an abandoned upstream session becomes reclaimable', async () => {
    vi.useFakeTimers()
    api.startSession
      .mockRejectedValueOnce({ message: 'session busy' })
      .mockResolvedValueOnce(session)
    const wrapper = mountModal()

    await wrapper.setProps({ show: true })
    await flushPromises()
    expect(api.startSession).toHaveBeenCalledOnce()

    await vi.advanceTimersByTimeAsync(2500)
    await flushPromises()
    expect(api.startSession).toHaveBeenCalledTimes(2)

    wrapper.unmount()
    vi.useRealTimers()
  })

  it('ignores a late result from a closed session', async () => {
    vi.useFakeTimers()
    const nextSession = { ...session, session_id: 'session-b' }
    const pendingResult = deferred<{
      callback_url: string | null
      code: string | null
      current_url: string | null
    }>()
    api.startSession
      .mockResolvedValueOnce(session)
      .mockResolvedValueOnce(nextSession)
    api.getResult.mockReturnValueOnce(pendingResult.promise)
    const wrapper = mountModal()

    await wrapper.setProps({ show: true })
    await flushPromises()
    vi.advanceTimersByTime(1500)
    await Promise.resolve()
    await wrapper.setProps({ show: false })
    await wrapper.setProps({ show: true })
    await flushPromises()

    pendingResult.resolve({ callback_url: null, code: 'old-code', current_url: 'http://localhost' })
    await flushPromises()
    expect((wrapper.find('input.font-mono').element as HTMLInputElement).value).toBe('')

    wrapper.unmount()
    vi.useRealTimers()
  })

  it('resets a stale OAuth-open operation when reopened', async () => {
    const pendingGenerate = deferred<boolean>()
    oauth.generateAuthUrl.mockReturnValue(pendingGenerate.promise)
    api.startSession
      .mockResolvedValueOnce(session)
      .mockResolvedValueOnce({ ...session, session_id: 'session-b' })
    const wrapper = mountModal()
    await wrapper.setProps({ show: true })
    await flushPromises()

    await wrapper.get('button.btn-primary').trigger('click')
    await wrapper.setProps({ show: false })
    await wrapper.setProps({ show: true })
    await flushPromises()

    expect(wrapper.get('button.btn-primary').text()).toBe('Open OAuth link')
    expect(wrapper.get('button.btn-primary').attributes('disabled')).toBeUndefined()
    pendingGenerate.resolve(true)
    await flushPromises()
    expect(api.navigate).not.toHaveBeenCalled()
    wrapper.unmount()
  })

  it('does not authorize after cancel during final session cleanup', async () => {
    const pendingStop = deferred<{ ok: boolean }>()
    oauth.exchangeAuthCode.mockResolvedValue({ access_token: 'token' })
    oauth.buildCredentials.mockReturnValue({ access_token: 'token' })
    api.stopSession.mockReturnValue(pendingStop.promise)
    const wrapper = mountModal()
    await wrapper.setProps({ show: true })
    await flushPromises()
    await wrapper.get('input.font-mono').setValue('oauth-code')

    const complete = wrapper.findAll('button').find((button) => button.text() === 'Complete')
    await complete!.trigger('click')
    await flushPromises()
    const cancel = wrapper.findAll('button').find((button) => button.text() === 'Cancel')
    await cancel!.trigger('click')
    pendingStop.resolve({ ok: true })
    await flushPromises()

    expect(wrapper.emitted('authorized')).toBeUndefined()
    expect(wrapper.emitted('close')).toHaveLength(1)
    wrapper.unmount()
  })

  it('prefills and reveals saved Google credentials', async () => {
    const wrapper = mountModal({
      initialGoogleLogin: 'saved@example.com',
      initialGooglePassword: 'saved-password',
      initialGoogle2faImportCode: 'JBSWY3DPEHPK3PXP'
    })

    await wrapper.setProps({ show: true })
    await flushPromises()

    const login = wrapper.get('#browser-google-login')
    const password = wrapper.get('#browser-google-password')
    const importCode = wrapper.get('#browser-google-2fa')
    expect((login.element as HTMLInputElement).value).toBe('saved@example.com')
    expect((password.element as HTMLInputElement).value).toBe('saved-password')
    expect(password.attributes('type')).toBe('password')
    expect((importCode.element as HTMLInputElement).value).toBe('JBSWY3DPEHPK3PXP')
    expect(importCode.attributes('type')).toBe('password')

    await wrapper.get('[aria-label="Show Google password"]').trigger('click')
    await wrapper.get('[aria-label="Show 2FA import code"]').trigger('click')
    expect(password.attributes('type')).toBe('text')
    expect(importCode.attributes('type')).toBe('text')
    wrapper.unmount()
  })

  it('posts the account-scoped autologin body and polls sanitized status', async () => {
    vi.useFakeTimers()
    const wrapper = mountModal({ accountId: 42 })
    await wrapper.setProps({ show: true })
    await flushPromises()
    await wrapper.get('#browser-google-login').setValue('admin@example.com')
    await wrapper.get('#browser-google-password').setValue('secret-password')
    await wrapper.get('#browser-google-2fa').setValue('IMPORT-CODE')

    const run = wrapper.findAll('button').find((button) => button.text() === 'Run Google activation')
    await run!.trigger('click')
    await flushPromises()

    expect(api.runGoogleAutologin).toHaveBeenCalledWith(
      'session-a',
      {
        account_id: 42,
        login: 'admin@example.com',
        password: 'secret-password',
        two_factor_import_code: 'IMPORT-CODE'
      },
      { signal: expect.any(AbortSignal) }
    )
    expect(wrapper.get('[data-testid="google-autologin-status"]').text()).toBe(
      'Google activation running.'
    )

    await vi.advanceTimersByTimeAsync(1500)
    await flushPromises()
    expect(api.getGoogleAutologinStatus).toHaveBeenCalledWith(
      'session-a',
      { signal: expect.any(AbortSignal) }
    )
    expect(wrapper.get('[data-testid="google-autologin-status"]').text()).toBe(
      'Google activation succeeded.'
    )
    expect(wrapper.text()).not.toContain('secret-password')
    expect(wrapper.text()).not.toContain('IMPORT-CODE')
    wrapper.unmount()
    vi.useRealTimers()
  })

  it('cancels activation, aborts polling, and ignores a late status response', async () => {
    vi.useFakeTimers()
    const pendingStatus = deferred<{ status: 'succeeded' }>()
    api.getGoogleAutologinStatus.mockReturnValue(pendingStatus.promise)
    const wrapper = mountModal()
    await wrapper.setProps({ show: true })
    await flushPromises()
    await wrapper.get('#browser-google-login').setValue('admin@example.com')
    await wrapper.get('#browser-google-password').setValue('secret-password')

    const run = wrapper.findAll('button').find((button) => button.text() === 'Run Google activation')
    await run!.trigger('click')
    await flushPromises()
    await vi.advanceTimersByTimeAsync(1500)

    const pollOptions = api.getGoogleAutologinStatus.mock.calls[0][1]
    expect(pollOptions.signal.aborted).toBe(false)
    const cancel = wrapper
      .findAll('button')
      .find((button) => button.text() === 'Cancel Google activation')
    await cancel!.trigger('click')
    await flushPromises()

    expect(api.cancelGoogleAutologin).toHaveBeenCalledWith(
      'session-a',
      { signal: expect.any(AbortSignal) }
    )
    expect(pollOptions.signal.aborted).toBe(true)
    expect(api.stopSession).not.toHaveBeenCalled()
    expect(wrapper.get('[data-testid="google-autologin-status"]').text()).toBe(
      'Google activation canceled.'
    )

    pendingStatus.resolve({ status: 'succeeded' })
    await flushPromises()
    await vi.advanceTimersByTimeAsync(3000)
    expect(api.getGoogleAutologinStatus).toHaveBeenCalledOnce()
    expect(wrapper.get('[data-testid="google-autologin-status"]').text()).toBe(
      'Google activation canceled.'
    )

    wrapper.unmount()
    vi.useRealTimers()
  })

  it('merges Google fields into credentials emitted after OAuth completion', async () => {
    oauth.exchangeAuthCode.mockResolvedValue({ access_token: 'token' })
    oauth.buildCredentials.mockReturnValue({ access_token: 'token', project_id: 'project' })
    const wrapper = mountModal({
      initialGoogleLogin: 'admin@example.com',
      initialGooglePassword: 'secret-password',
      initialGoogle2faImportCode: 'IMPORT-CODE'
    })
    await wrapper.setProps({ show: true })
    await flushPromises()
    await wrapper.get('input.font-mono').setValue('oauth-code')

    const complete = wrapper.findAll('button').find((button) => button.text() === 'Complete')
    await complete!.trigger('click')
    await flushPromises()

    expect(wrapper.emitted('authorized')?.[0]?.[0]).toEqual({
      credentials: {
        access_token: 'token',
        project_id: 'project',
        google_login: 'admin@example.com',
        google_password: 'secret-password',
        google_2fa_import_code: 'IMPORT-CODE'
      },
      profileId: 'profile-a'
    })
    wrapper.unmount()
  })

  it('sends a session-scoped keepalive delete on pagehide', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response())
    const wrapper = mountModal()

    await wrapper.setProps({ show: true })
    await flushPromises()

    window.dispatchEvent(new Event('pagehide'))
    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/admin/browser-login/session',
      expect.objectContaining({
        method: 'DELETE',
        keepalive: true,
        headers: expect.objectContaining({ 'X-Browser-Session-ID': 'session-a' })
      })
    )

    wrapper.unmount()
    fetchMock.mockRestore()
  })
})
