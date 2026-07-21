import { ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { useAppStore } from '@/stores/app'
import { adminAPI } from '@/api/admin'
import type { AntigravityNativeTokenInfo } from '@/api/admin/antigravity_native'

/**
 * useAntigravityNativeOAuth — composable wrapping the admin
 * /antigravity-native/oauth/* admin endpoints. Same interface as
 * useAntigravityOAuth so OAuthAuthorizationFlow and ReAuthAccountModal can
 * dispatch by platform without conditional argument shapes.
 *
 * Differences from legacy:
 *   - Hits the native admin route prefix (handled by adminAPI.antigravityNative).
 *   - buildCredentials passes through the extra identity fields
 *     (installation_id, instance_label, connection_id, tier_id) which the
 *     native gateway service expects in JSONB credentials.
 *   - i18n keys reuse the legacy `admin.accounts.oauth.antigravity.*` namespace
 *     so users don't need a separate translation block for V1.
 */
export function useAntigravityNativeOAuth() {
  const appStore = useAppStore()
  const { t } = useI18n()

  const authUrl = ref('')
  const sessionId = ref('')
  const state = ref('')
  const loading = ref(false)
  const error = ref('')
  let operationGeneration = 0

  const resetState = () => {
    operationGeneration++
    authUrl.value = ''
    sessionId.value = ''
    state.value = ''
    loading.value = false
    error.value = ''
  }

  const generateAuthUrl = async (proxyId: number | null | undefined): Promise<boolean> => {
    const generation = ++operationGeneration
    loading.value = true
    authUrl.value = ''
    sessionId.value = ''
    state.value = ''
    error.value = ''

    try {
      const payload: Record<string, unknown> = {}
      if (proxyId) payload.proxy_id = proxyId

      const response = await adminAPI.antigravityNative.generateAuthUrl(payload as any)
      if (generation !== operationGeneration) return false
      authUrl.value = response.auth_url
      sessionId.value = response.session_id
      state.value = response.state
      return true
    } catch (err: any) {
      if (generation !== operationGeneration) return false
      error.value =
        err.response?.data?.detail || t('admin.accounts.oauth.antigravity.failedToGenerateUrl')
      appStore.showError(error.value)
      return false
    } finally {
      if (generation === operationGeneration) loading.value = false
    }
  }

  const exchangeAuthCode = async (params: {
    code: string
    sessionId: string
    state: string
    proxyId?: number | null
  }): Promise<AntigravityNativeTokenInfo | null> => {
    const code = params.code?.trim()
    if (!code || !params.sessionId || !params.state) {
      error.value = t('admin.accounts.oauth.antigravity.missingExchangeParams')
      return null
    }
    const generation = ++operationGeneration

    loading.value = true
    error.value = ''

    try {
      const payload: Record<string, unknown> = {
        session_id: params.sessionId,
        state: params.state,
        code
      }
      if (params.proxyId) payload.proxy_id = params.proxyId

      const tokenInfo = await adminAPI.antigravityNative.exchangeCode(payload as any)
      if (generation !== operationGeneration) return null
      return tokenInfo as AntigravityNativeTokenInfo
    } catch (err: any) {
      if (generation !== operationGeneration) return null
      // Prefer the verbatim backend message — backend now ships the raw
      // upstream OAuth error body (e.g. {"error":"invalid_grant",...})
      // or the underlying transport failure (proxy refused / TLS / DNS)
      // so the operator sees exactly what went wrong. Fall back to the
      // older `detail` field, then the generic i18n string only if
      // nothing else is available.
      error.value =
        err.response?.data?.message ||
        err.response?.data?.detail ||
        t('admin.accounts.oauth.antigravity.failedToExchangeCode')
      appStore.showError(error.value)
      return null
    } finally {
      if (generation === operationGeneration) loading.value = false
    }
  }

  const validateRefreshToken = async (
    refreshToken: string,
    proxyId?: number | null
  ): Promise<AntigravityNativeTokenInfo | null> => {
    if (!refreshToken.trim()) {
      error.value = t('admin.accounts.oauth.antigravity.pleaseEnterRefreshToken')
      return null
    }
    const generation = ++operationGeneration

    loading.value = true
    error.value = ''

    try {
      const tokenInfo = await adminAPI.antigravityNative.refreshAntigravityNativeToken(
        refreshToken.trim(),
        proxyId
      )
      if (generation !== operationGeneration) return null
      return tokenInfo as AntigravityNativeTokenInfo
    } catch (err: any) {
      if (generation !== operationGeneration) return null
      error.value =
        err.response?.data?.detail || t('admin.accounts.oauth.antigravity.failedToValidateRT')
      return null
    } finally {
      if (generation === operationGeneration) loading.value = false
    }
  }

  const buildCredentials = (
    tokenInfo: AntigravityNativeTokenInfo
  ): Record<string, unknown> => {
    let expiresAt: string | undefined
    if (typeof tokenInfo.expires_at === 'number' && Number.isFinite(tokenInfo.expires_at)) {
      expiresAt = Math.floor(tokenInfo.expires_at).toString()
    } else if (typeof tokenInfo.expires_at === 'string' && tokenInfo.expires_at.trim()) {
      expiresAt = tokenInfo.expires_at.trim()
    }

    return {
      access_token: tokenInfo.access_token,
      refresh_token: tokenInfo.refresh_token,
      token_type: tokenInfo.token_type,
      expires_at: expiresAt,
      project_id: tokenInfo.project_id,
      email: tokenInfo.email,
      installation_id: tokenInfo.installation_id,
      instance_label: tokenInfo.instance_label,
      connection_id: tokenInfo.connection_id,
      tier_id: tokenInfo.tier_id
    }
  }

  return {
    authUrl,
    sessionId,
    state,
    loading,
    error,
    resetState,
    generateAuthUrl,
    exchangeAuthCode,
    validateRefreshToken,
    buildCredentials
  }
}
