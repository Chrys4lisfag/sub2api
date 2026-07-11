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
import { ref, computed, watch, onUnmounted } from 'vue'
import BaseDialog from '@/components/common/BaseDialog.vue'
import { useAntigravityNativeOAuth } from '@/composables/useAntigravityNativeOAuth'
import browserLoginAPI, { type BrowserLoginSession } from '@/api/admin/browserLogin'
import { useAppStore } from '@/stores/app'

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

const vncUrl = computed(() => {
  if (!session.value) return ''
  const host = window.location.hostname
  return `http://${host}:6080/vnc.html?autoconnect=1&resize=scale&path=websockify&password=${encodeURIComponent(
    session.value.vnc_token
  )}`
})

const stopPoll = () => {
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

const startPoll = () => {
  stopPoll()
  pollTimer = setInterval(async () => {
    try {
      const r = await browserLoginAPI.getResult()
      // In launch mode the poll is only a keep-alive (refreshes the session's
      // idle-TTL); no OAuth code capture.
      if (!isLaunch.value && r.code && !authCode.value.trim()) {
        authCode.value = r.code
        statusText.value = 'Callback code captured automatically from the stream.'
        stopPoll()
      }
    } catch {
      /* session may not be active yet — keep polling */
    }
  }, 1500)
}

const open = async () => {
  starting.value = true
  statusText.value = ''
  authCode.value = ''
  session.value = null
  resetState()
  try {
    session.value = await browserLoginAPI.startSession({
      proxy_id: props.proxyId,
      profile_id: props.profileId
    })
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
    appStore.showError(
      err.response?.data?.message || err.response?.data?.detail || 'Failed to start browser session'
    )
    emit('close')
  } finally {
    starting.value = false
  }
}

const openOAuthLink = async () => {
  generating.value = true
  try {
    const ok = await generateAuthUrl(props.proxyId)
    if (ok && authUrl.value) {
      await browserLoginAPI.navigate({ url: authUrl.value })
      statusText.value = 'OAuth page opened in the stream. Complete the Google login there.'
    }
  } catch (err: any) {
    appStore.showError(
      err.response?.data?.message || err.response?.data?.detail || 'Failed to open OAuth link'
    )
  } finally {
    generating.value = false
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
  stopPoll()
  try {
    await browserLoginAPI.stopSession()
  } catch {
    /* best-effort teardown */
  }
  session.value = null
}

const handleClose = async () => {
  await cleanup()
  emit('close')
}

const complete = async () => {
  const code = extractCode(authCode.value)
  if (!code) return
  completing.value = true
  try {
    const tokenInfo = await exchangeAuthCode({
      code,
      sessionId: sessionId.value,
      state: state.value,
      proxyId: props.proxyId
    })
    if (!tokenInfo) return // composable already surfaced the error toast
    const credentials = buildCredentials(tokenInfo)
    const profileId = session.value?.profile_id || props.profileId || ''
    await cleanup()
    emit('authorized', { credentials, profileId })
  } finally {
    completing.value = false
  }
}

watch(
  () => props.show,
  (val) => {
    if (val) open()
    // Modal hidden (incl. parent closing the whole account modal): tear the
    // browser session down so it doesn't leak into a 409 "session busy".
    else if (session.value) void cleanup()
    else stopPoll()
  }
)

onUnmounted(() => {
  stopPoll()
  if (session.value) void cleanup()
})
</script>
