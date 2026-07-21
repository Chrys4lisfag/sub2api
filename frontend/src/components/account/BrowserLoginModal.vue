<template>
  <BaseDialog
    :show="show"
    :title="modalTitle"
    width="full"
    :close-on-click-outside="false"
    @close="handleClose"
  >
    <div class="space-y-3">
      <!-- Above the stream: generate/open the OAuth link inside the streamed browser -->
      <div v-if="!isLaunch" class="flex items-center gap-2">
        <button
          type="button"
          class="btn btn-primary whitespace-nowrap"
          :disabled="!session || generating"
          @click="openOAuthLink"
        >
          {{ generating ? 'Opening…' : 'Open OAuth link' }}
        </button>
        <input
          v-if="authUrl"
          :value="authUrl"
          readonly
          class="input flex-1 text-xs"
          @focus="(e) => (e.target as HTMLInputElement).select()"
        />
        <button v-if="authUrl" type="button" class="btn btn-secondary" @click="copyUrl">Copy</button>
      </div>

      <!-- The streamed stealth browser (noVNC over the account's proxy) -->
      <div
        class="relative w-full overflow-hidden rounded-lg border border-gray-200 dark:border-dark-600"
        style="height: 60vh; background: #000"
      >
        <div
          v-if="starting"
          class="absolute inset-0 flex items-center justify-center text-sm text-gray-300"
        >
          Starting stealth browser…
        </div>
        <iframe
          v-if="vncUrl"
          :src="vncUrl"
          class="h-full w-full"
          style="border: 0"
          allow="clipboard-read; clipboard-write"
        ></iframe>
        <p class="absolute bottom-1 left-2 rounded bg-black/70 px-2 py-1 text-[11px] text-gray-200">
          Ctrl/Cmd shortcuts stay inside the remote browser. For device paste, use the noVNC clipboard panel.
        </p>
      </div>

      <!-- Below the stream: the OAuth callback code (auto-captured or pasted) -->
      <div v-if="!isLaunch">
        <label class="input-label">OAuth callback code</label>
        <input
          v-model="authCode"
          type="text"
          class="input font-mono"
          placeholder="Auto-captured from the stream, or paste the callback code / full callback URL"
        />
      </div>
      <p v-if="statusText" class="text-xs text-gray-500 dark:text-gray-400">{{ statusText }}</p>
    </div>

    <template #footer>
      <div class="flex justify-between gap-3">
        <button type="button" class="btn btn-secondary" @click="handleClose">{{ isLaunch ? 'Close' : 'Cancel' }}</button>
        <button
          v-if="!isLaunch"
          type="button"
          class="btn btn-primary"
          :disabled="!authCode.trim() || completing"
          @click="complete"
        >
          {{ completing ? 'Verifying…' : 'Complete' }}
        </button>
      </div>
    </template>
  </BaseDialog>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted, onUnmounted } from 'vue'
import BaseDialog from '@/components/common/BaseDialog.vue'
import { useAntigravityNativeOAuth } from '@/composables/useAntigravityNativeOAuth'
import browserLoginAPI, { type BrowserLoginSession } from '@/api/admin/browserLogin'
import { useAppStore } from '@/stores/app'
import { buildApiUrl } from '@/api/client'

const props = defineProps<{
  show: boolean
  proxyId: number | null
  profileId?: string
  mode?: 'login' | 'launch'
  accountName?: string
}>()

const emit = defineEmits<{
  (e: 'authorized', payload: { credentials: Record<string, unknown>; profileId: string }): void
  (e: 'profile-created', profileId: string): void
  (e: 'close'): void
}>()

const appStore = useAppStore()

const isLaunch = computed(() => props.mode === 'launch')
const modalTitle = computed(() =>
  isLaunch.value ? `Browser${props.accountName ? ' — ' + props.accountName : ''}` : 'Login with Browser'
)

// Fresh OAuth composable instance — the modal runs the whole flow
// (generate URL → open in stream → capture code → exchange) internally and
// emits the final credentials, so the parent only creates the account.
const { authUrl, sessionId, state, generateAuthUrl, exchangeAuthCode, buildCredentials, resetState } =
  useAntigravityNativeOAuth()

const session = ref<BrowserLoginSession | null>(null)
const authCode = ref('')
const starting = ref(false)
const generating = ref(false)
const completing = ref(false)
const statusText = ref('')
let pollTimer: ReturnType<typeof setInterval> | null = null
let lifecycleGeneration = 0
let pollGeneration = 0
let activePollRequest = 0

const vncUrl = computed(() => {
  const current = session.value
  if (!current) return ''
  const url = new URL('/vnc.html', current.vnc_url)
  url.searchParams.set('autoconnect', '1')
  url.searchParams.set('resize', 'scale')
  url.searchParams.set('path', 'websockify')
  url.searchParams.set('password', current.vnc_token)
  return url.toString()
})

const stopPoll = () => {
  pollGeneration++
  if (pollTimer) {
    clearInterval(pollTimer)
    pollTimer = null
  }
}

// Extract the OAuth code from a raw code or a full callback URL.
const extractCode = (raw: string): string => {
  const trimmed = raw.trim()
  if (trimmed.includes('?') && trimmed.includes('code=')) {
    try {
      const u = new URL(trimmed)
      const c = u.searchParams.get('code')
      if (c) return c
    } catch {
      const m = trimmed.match(/[?&]code=([^&]+)/)
      if (m && m[1]) return m[1]
    }
  }
  return trimmed
}

const errorText = (err: any): string => [
  err?.message,
  err?.response?.data?.message,
  err?.response?.data?.detail
].filter(Boolean).join(' ')

const startPoll = () => {
  stopPoll()
  const generation = ++pollGeneration
  pollTimer = setInterval(async () => {
    const current = session.value
    if (!current || activePollRequest === generation) return
    activePollRequest = generation
    try {
      const r = await browserLoginAPI.getResult(current.session_id)
      if (generation !== pollGeneration || session.value?.session_id !== current.session_id) return
      if (!isLaunch.value && r.code && !authCode.value.trim()) {
        authCode.value = r.code
        statusText.value = 'Callback code captured automatically from the stream.'
        stopPoll()
      }
    } catch (err) {
      if (generation !== pollGeneration || session.value?.session_id !== current.session_id) return
      if (/stale or missing browser session|no active session/i.test(errorText(err))) {
        stopPoll()
        session.value = null
        statusText.value = 'Browser session ended. Close and reopen this dialog to start a new one.'
      }
    } finally {
      if (activePollRequest === generation) activePollRequest = 0
    }
  }, 1500)
}

const isBusyError = (err: any): boolean => /session busy/i.test(errorText(err))


const open = async (attempt = 0, generation = ++lifecycleGeneration) => {
  let retryScheduled = false
  starting.value = true
  if (attempt === 0) {
    statusText.value = ''
    authCode.value = ''
    session.value = null
    generating.value = false
    completing.value = false
    resetState()
  }
  try {
    const started = await browserLoginAPI.startSession({
      proxy_id: props.proxyId,
      profile_id: props.profileId
    })
    if (generation !== lifecycleGeneration || !props.show) {
      await browserLoginAPI.stopSession(started.session_id).catch(() => undefined)
      return
    }
    session.value = started
    // Launch mode: persist a freshly-minted profile back to the account so a
    // later launch / re-login reuses the same signed-in profile.
    if (isLaunch.value && !props.profileId && session.value.profile_id) {
      emit('profile-created', session.value.profile_id)
    }
    statusText.value = isLaunch.value
      ? 'Browser ready — use it in the stream (account proxy + saved profile). Close when done.'
      : 'Stealth browser ready. Click "Open OAuth link", sign in inside the stream.'
    startPoll()
  } catch (err: any) {
    if (generation !== lifecycleGeneration || !props.show) return
    if (isBusyError(err) && attempt < 4) {
      retryScheduled = true
      statusText.value = 'Previous browser session is closing…'
      window.setTimeout(() => {
        if (generation === lifecycleGeneration && props.show) void open(attempt + 1, generation)
      }, 2500)
      return
    }
    appStore.showError(
      err.response?.data?.message || err.response?.data?.detail || err.message || 'Failed to start browser session'
    )
    emit('close')
  } finally {
    if (generation === lifecycleGeneration && !retryScheduled) starting.value = false
  }
}

const openOAuthLink = async () => {
  const generation = lifecycleGeneration
  const browserSessionId = session.value?.session_id
  if (!browserSessionId) return
  generating.value = true
  try {
    const ok = await generateAuthUrl(props.proxyId)
    if (generation !== lifecycleGeneration || !props.show || session.value?.session_id !== browserSessionId) return
    const targetUrl = authUrl.value
    if (!ok || !targetUrl) return
    const result = await browserLoginAPI.navigate(browserSessionId, { url: targetUrl })
    if (generation !== lifecycleGeneration || !props.show || session.value?.session_id !== browserSessionId) return
    statusText.value = result.warning
      ? `Browser navigation warning: ${result.warning}`
      : 'OAuth page opened in the stream. Complete the Google login there.'
  } catch (err: any) {
    if (generation !== lifecycleGeneration || !props.show) return
    appStore.showError(
      err.response?.data?.message || err.response?.data?.detail || 'Failed to open OAuth link'
    )
  } finally {
    if (generation === lifecycleGeneration) generating.value = false
  }
}

const copyUrl = async () => {
  try {
    await navigator.clipboard.writeText(authUrl.value)
    appStore.showSuccess('OAuth link copied')
  } catch {
    /* clipboard unavailable — the readonly input is still selectable */
  }
}

const cleanup = async () => {
  lifecycleGeneration++
  stopPoll()
  const current = session.value
  session.value = null
  if (!current) return
  try {
    await browserLoginAPI.stopSession(current.session_id)
  } catch {
    /* best-effort teardown; server lease reclaims abandoned sessions */
  }
}

const handleClose = async () => {
  await cleanup()
  emit('close')
}

const complete = async () => {
  const code = extractCode(authCode.value)
  const current = session.value
  if (!code || !current) return
  const generation = lifecycleGeneration
  const browserSessionId = current.session_id
  const profileId = current.profile_id || props.profileId || ''
  const oauthSessionId = sessionId.value
  const oauthState = state.value
  const proxyId = props.proxyId
  completing.value = true
  try {
    const tokenInfo = await exchangeAuthCode({
      code,
      sessionId: oauthSessionId,
      state: oauthState,
      proxyId
    })
    if (
      !tokenInfo ||
      generation !== lifecycleGeneration ||
      !props.show ||
      session.value?.session_id !== browserSessionId
    ) return
    const credentials = buildCredentials(tokenInfo)
    await cleanup()
    if (lifecycleGeneration !== generation + 1 || !props.show) return
    completing.value = false
    emit('authorized', { credentials, profileId })
  } finally {
    if (generation === lifecycleGeneration) completing.value = false
  }
}

watch(
  () => props.show,
  (val) => {
    if (val) void open()
    else void cleanup()
  }
)

const releaseOnPageHide = (event: PageTransitionEvent) => {
  if (event.persisted || !session.value) return
  const token = localStorage.getItem('auth_token')
  const headers: Record<string, string> = {
    'X-Browser-Session-ID': session.value.session_id
  }
  if (token) headers.Authorization = `Bearer ${token}`
  void fetch(buildApiUrl('/admin/browser-login/session'), {
    method: 'DELETE',
    headers,
    credentials: 'include',
    keepalive: true
  }).catch(() => undefined)
}

onMounted(() => window.addEventListener('pagehide', releaseOnPageHide))

onUnmounted(() => {
  window.removeEventListener('pagehide', releaseOnPageHide)
  void cleanup()
})
</script>
