import { defineComponent } from 'vue'
import type * as Vue from 'vue'
import { flushPromises, mount } from '@vue/test-utils'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const api = vi.hoisted(() => ({
  startSession: vi.fn(),
  getResult: vi.fn(),
  stopSession: vi.fn(),
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

function mountModal() {
  return mount(BrowserLoginModal, {
    props: { show: false, proxyId: null },
    global: { stubs: { BaseDialog: BaseDialogStub } }
  })
}

describe('BrowserLoginModal lifecycle', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    api.startSession.mockResolvedValue(session)
    api.getResult.mockResolvedValue({ callback_url: null, code: null, current_url: 'about:blank' })
    api.stopSession.mockResolvedValue({ ok: true })
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
