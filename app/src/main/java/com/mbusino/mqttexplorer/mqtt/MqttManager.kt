package com.mbusino.mqttexplorer.mqtt

import android.util.Log
import com.mbusino.mqttexplorer.data.ConnectionSettings
import com.mbusino.mqttexplorer.data.TopicMessage
import com.mbusino.mqttexplorer.data.TopicNode
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import org.eclipse.paho.client.mqttv3.IMqttActionListener
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken
import org.eclipse.paho.client.mqttv3.IMqttToken
import org.eclipse.paho.client.mqttv3.MqttCallback
import org.eclipse.paho.client.mqttv3.MqttConnectOptions
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.eclipse.paho.client.mqttv3.MqttAsyncClient
import org.eclipse.paho.client.mqttv3.MqttException
import org.eclipse.paho.client.mqttv3.MqttSecurityException
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
import java.security.SecureRandom
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.util.concurrent.ConcurrentHashMap
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLSocketFactory
import javax.net.ssl.TrustManager
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509TrustManager
import java.security.KeyStore

enum class ConnectionState {
    DISCONNECTED,
    CONNECTING,
    CONNECTED,
    ERROR
}

/** Subscription mode: "#" (everything) or a single path filter "<path>/#". */
enum class SubscribeMode {
    WILDCARD,
    PATH
}

class MqttManager private constructor() {

    companion object {
        private const val TAG = "MqttManager"

        @Volatile
        private var instance: MqttManager? = null

        fun getInstance(): MqttManager {
            return instance ?: synchronized(this) {
                instance ?: MqttManager().also { instance = it }
            }
        }
    }

    private var client: MqttAsyncClient? = null
    private var lastSettings: ConnectionSettings? = null
    private val topicMessages = ConcurrentHashMap<String, MutableList<TopicMessage>>()
    private val _connectionState = MutableStateFlow(ConnectionState.DISCONNECTED)
    val connectionState: StateFlow<ConnectionState> = _connectionState.asStateFlow()

    private val _topicTree = MutableStateFlow(TopicNode("root", ""))
    val topicTree: StateFlow<TopicNode> = _topicTree.asStateFlow()

    private val _errorMessage = MutableStateFlow<String?>(null)
    val errorMessage: StateFlow<String?> = _errorMessage.asStateFlow()

    private val _currentConnection = MutableStateFlow<ConnectionSettings?>(null)
    val currentConnection: StateFlow<ConnectionSettings?> = _currentConnection.asStateFlow()

    private val _subscribedTopics = MutableStateFlow(setOf<String>())
    val subscribedTopics: StateFlow<Set<String>> = _subscribedTopics.asStateFlow()

    private val _subscribeMode = MutableStateFlow(SubscribeMode.WILDCARD)
    val subscribeMode: StateFlow<SubscribeMode> = _subscribeMode.asStateFlow()

    /** Path without "/#" when in path mode, empty string in wildcard mode. */
    private val _pathFilter = MutableStateFlow("")
    val pathFilter: StateFlow<String> = _pathFilter.asStateFlow()

    /** Non-null while connected in path mode and the Ja/Nein wildcard decision is pending. */
    private val _reconnectDecision = MutableStateFlow<String?>(null)
    val reconnectDecision: StateFlow<String?> = _reconnectDecision.asStateFlow()

    fun connect(settings: ConnectionSettings) {
        if (_connectionState.value == ConnectionState.CONNECTING) return

        disconnect()
        lastSettings = settings
        restoreMode(settings.subscribeMode, settings.subscribeFilter)

        _currentConnection.value = settings
        _connectionState.value = ConnectionState.CONNECTING
        _errorMessage.value = null

        doConnect(settings)
    }

    fun reconnect() {
        val settings = lastSettings ?: return
        if (_connectionState.value == ConnectionState.CONNECTING) return

        disconnect()
        lastSettings = settings  // disconnect() clears this, restore it
        restoreMode(settings.subscribeMode, settings.subscribeFilter)

        _currentConnection.value = settings
        _connectionState.value = ConnectionState.CONNECTING
        _errorMessage.value = null

        doConnect(settings)
    }

    private fun doConnect(settings: ConnectionSettings) {
        try {
            val serverUri = settings.fullUrl
            val clientId = "MQTTBrowser_${System.currentTimeMillis()}"
            val persistence = MemoryPersistence()

            client = MqttAsyncClient(serverUri, clientId, persistence).apply {
                setCallback(object : MqttCallback {
                    override fun connectionLost(cause: Throwable?) {
                        Log.w(TAG, "Connection lost", cause)
                        _connectionState.value = ConnectionState.DISCONNECTED
                        _errorMessage.value = "Connection lost: ${cause?.message ?: "Unknown error"}"
                    }

                    override fun messageArrived(topic: String, message: MqttMessage) {
                        val bytes = message.payload
                        val payload = try {
                            String(bytes, Charsets.UTF_8)
                        } catch (_: Exception) {
                            String(bytes)
                        }
                        handleIncomingMessage(topic, payload, bytes, message.isRetained)
                    }

                    override fun deliveryComplete(token: IMqttDeliveryToken?) {
                        // Not used for subscribe-only
                    }
                })
            }

            val connectOptions = MqttConnectOptions().apply {
                isCleanSession = true
                connectionTimeout = 10
                keepAliveInterval = 30
                isAutomaticReconnect = true
                if (settings.username.isNotBlank()) {
                    userName = settings.username
                }
                if (settings.password.isNotBlank()) {
                    password = settings.password.toCharArray()
                }
                if (settings.tls) {
                    socketFactory = if (settings.trustAll) {
                        createTrustAllSocketFactory()
                    } else if (settings.caCertUri.isNotBlank()) {
                        createCustomCaSocketFactory(settings.caCertUri)
                    } else {
                        SSLSocketFactory.getDefault()
                    }
                }
            }

            client?.connect(connectOptions, null, object : IMqttActionListener {
                override fun onSuccess(asyncActionToken: IMqttToken?) {
                    Log.i(TAG, "Connected to $serverUri")
                    _connectionState.value = ConnectionState.CONNECTED
                    onConnectedSubscribe()
                }

                override fun onFailure(asyncActionToken: IMqttToken?, exception: Throwable?) {
                    Log.e(TAG, "Connect failed", exception)
                    _connectionState.value = ConnectionState.ERROR
                    _errorMessage.value = "Connection failed: ${exception?.message ?: "Unknown error"}"
                }
            })

        } catch (e: MqttSecurityException) {
            Log.e(TAG, "Security error connecting", e)
            _connectionState.value = ConnectionState.ERROR
            _errorMessage.value = "Security error: ${e.message}"
        } catch (e: MqttException) {
            Log.e(TAG, "Error connecting", e)
            _connectionState.value = ConnectionState.ERROR
            _errorMessage.value = "Error: ${e.message}"
        } catch (e: Exception) {
            Log.e(TAG, "Unexpected error connecting", e)
            _connectionState.value = ConnectionState.ERROR
            _errorMessage.value = "Error: ${e.message}"
        }
    }

    fun subscribeToWildcard(topic: String) {
        try {
            client?.subscribe(topic, 1, null, object : IMqttActionListener {
                override fun onSuccess(asyncActionToken: IMqttToken?) {
                    Log.i(TAG, "Subscribed to $topic")
                    _subscribedTopics.value = _subscribedTopics.value + topic
                }

                override fun onFailure(asyncActionToken: IMqttToken?, exception: Throwable?) {
                    Log.e(TAG, "Subscribe failed for $topic", exception)
                    _errorMessage.value = "Subscribe failed: ${exception?.message}"
                }
            })
        } catch (e: MqttException) {
            Log.e(TAG, "Error subscribing", e)
            _errorMessage.value = "Subscribe error: ${e.message}"
        }
    }

    fun unsubscribe(topic: String) {
        try {
            client?.unsubscribe(topic, null, object : IMqttActionListener {
                override fun onSuccess(asyncActionToken: IMqttToken?) {
                    Log.i(TAG, "Unsubscribed from $topic")
                    _subscribedTopics.value = _subscribedTopics.value - topic
                }

                override fun onFailure(asyncActionToken: IMqttToken?, exception: Throwable?) {
                    Log.e(TAG, "Unsubscribe failed for $topic", exception)
                }
            })
        } catch (e: MqttException) {
            Log.e(TAG, "Error unsubscribing", e)
        }
    }

    /** Called from the Paho connect listener. Wildcard mode subscribes immediately;
     *  path mode waits for the user's Ja/Nein decision (nothing is subscribed while undecided). */
    private fun onConnectedSubscribe() {
        when (_subscribeMode.value) {
            SubscribeMode.WILDCARD -> subscribeToWildcard("#")
            SubscribeMode.PATH -> {
                val filter = _pathFilter.value
                if (filter.isBlank()) {
                    // Defensive fallback: path mode without a filter -> wildcard
                    _subscribeMode.value = SubscribeMode.WILDCARD
                    persistModeToLastSettings()
                    subscribeToWildcard("#")
                } else {
                    Log.i(TAG, "Connected in path mode, waiting for wildcard decision (filter=$filter)")
                    _reconnectDecision.value = filter
                }
            }
        }
    }

    /** Wildcard -> path mode: unsubscribe current filter(s), clear tree, subscribe "<path>/#".
     *  A new path replaces a previous one. Returns false for a blank/invalid path. */
    fun subscribeToPathMode(path: String): Boolean {
        val normalized = normalizePath(path) ?: return false
        _reconnectDecision.value = null
        for (topic in _subscribedTopics.value.toList()) {
            unsubscribe(topic)
        }
        clearTree()
        _subscribeMode.value = SubscribeMode.PATH
        _pathFilter.value = normalized
        persistModeToLastSettings()
        subscribeToWildcard("$normalized/#")
        return true
    }

    /** Path mode -> wildcard: unsubscribe path filter, subscribe "#" (tree is kept). */
    fun switchToWildcardMode() {
        _reconnectDecision.value = null
        for (topic in _subscribedTopics.value.toList()) {
            unsubscribe(topic)
        }
        _subscribeMode.value = SubscribeMode.WILDCARD
        _pathFilter.value = ""
        persistModeToLastSettings()
        subscribeToWildcard("#")
    }

    /** Reconnect popup decision. Ja -> wildcard (persists, clears tree so it refills);
     *  Nein -> keep the saved path filter. */
    fun resolveReconnectDecision(useWildcard: Boolean) {
        val filter = _reconnectDecision.value ?: return
        _reconnectDecision.value = null
        if (useWildcard) {
            _subscribeMode.value = SubscribeMode.WILDCARD
            _pathFilter.value = ""
            persistModeToLastSettings()
            clearTree()
            subscribeToWildcard("#")
        } else {
            _subscribeMode.value = SubscribeMode.PATH
            _pathFilter.value = filter
            persistModeToLastSettings()
            subscribeToWildcard("$filter/#")
        }
    }

    fun subscribeModeName(): String = _subscribeMode.value.name.lowercase()

    fun pathFilterValue(): String = _pathFilter.value

    /** Loads mode + filter from persisted connection settings (lastSettings mechanism). */
    fun restoreMode(subscribeMode: String?, subscribeFilter: String?) {
        val filter = subscribeFilter ?: ""
        if (subscribeMode == "path" && filter.isNotBlank()) {
            _subscribeMode.value = SubscribeMode.PATH
            _pathFilter.value = filter
        } else {
            _subscribeMode.value = SubscribeMode.WILDCARD
            _pathFilter.value = ""
        }
    }

    /** Mirrors the current mode into lastSettings so reconnect()/save use it. */
    private fun persistModeToLastSettings() {
        lastSettings = lastSettings?.copy(
            subscribeMode = _subscribeMode.value.name.lowercase(),
            subscribeFilter = _pathFilter.value
        )
    }

    /** "MBusino/#" / "MBusino/" / "MBusino" -> "MBusino"; blank -> null. */
    private fun normalizePath(path: String): String? {
        var p = path.trim()
        if (p.isBlank()) return null
        if (p.endsWith("/#")) p = p.dropLast(2)
        else if (p.endsWith("#")) p = p.dropLast(1)
        p = p.trimEnd('/')
        return p.ifBlank { null }
    }

    fun clearTree() {
        topicMessages.clear()
        _topicTree.value = TopicNode("root", "")
    }

    fun publish(topic: String, payload: String, qos: Int = 1, retain: Boolean = false): Boolean {
        return try {
            val message = MqttMessage(payload.toByteArray()).apply {
                this.qos = qos
                this.isRetained = retain
            }
            client?.publish(topic, message)
            Log.i(TAG, "Published to $topic (${payload.toByteArray().size} bytes)")
            true
        } catch (e: MqttException) {
            Log.e(TAG, "Publish failed for $topic", e)
            _errorMessage.value = "Publish failed: ${e.message}"
            false
        } catch (e: Exception) {
            Log.e(TAG, "Unexpected publish error", e)
            _errorMessage.value = "Publish error: ${e.message}"
            false
        }
    }

    private fun handleIncomingMessage(topic: String, payload: String, rawBytes: ByteArray? = null, isRetained: Boolean = false) {
        val msg = TopicMessage(payload, rawPayload = rawBytes, isRetained = isRetained)
        val messages = topicMessages.getOrPut(topic) { mutableListOf() }
        messages.add(msg)
        if (messages.size > 500) {
            messages.removeAt(0)
        }
        _topicTree.value = TopicNode.buildTree(topicMessages.toMap())
    }

    fun getMessagesForTopic(topicPath: String): List<TopicMessage> {
        return topicMessages[topicPath]?.toList()?.reversed() ?: emptyList()
    }

    fun disconnect() {
        try {
            client?.let { mqttClient ->
                if (mqttClient.isConnected) {
                    mqttClient.disconnect()
                }
                mqttClient.close()
            }
        } catch (e: MqttException) {
            Log.e(TAG, "Error disconnecting", e)
        }
        client = null
        clearTree()
        _connectionState.value = ConnectionState.DISCONNECTED
        _currentConnection.value = null
        _subscribedTopics.value = emptySet()
        _reconnectDecision.value = null
    }

    fun clearError() {
        _errorMessage.value = null
    }

    fun isConnected(): Boolean {
        return client?.isConnected == true
    }

    fun hasLastSettings(): Boolean {
        return lastSettings != null
    }

    private fun createTrustAllSocketFactory(): SSLSocketFactory {
        val trustAllCerts = arrayOf<TrustManager>(object : X509TrustManager {
            override fun checkClientTrusted(chain: Array<out X509Certificate>?, authType: String?) {}
            override fun checkServerTrusted(chain: Array<out X509Certificate>?, authType: String?) {}
            override fun getAcceptedIssuers(): Array<X509Certificate> = emptyArray()
        })
        val sslContext = SSLContext.getInstance("TLS")
        sslContext.init(null, trustAllCerts, SecureRandom())
        return sslContext.socketFactory
    }

    fun createCustomCaSocketFactory(caCertUri: String): SSLSocketFactory {
        val context = com.mbusino.mqttexplorer.MqttExplorerApp.getAppContext()
        val cf = CertificateFactory.getInstance("X.509")
        val internalFile = java.io.File(context.filesDir, "custom_ca.crt")

        // If caCertUri is an internal path, load from file directly
        val cert = if (caCertUri.startsWith(context.filesDir.absolutePath) && internalFile.exists()) {
            internalFile.inputStream().use { cf.generateCertificate(it) as X509Certificate }
        } else {
            // Copy from content URI to internal storage
            val loaded = context.contentResolver.openInputStream(android.net.Uri.parse(caCertUri))?.use {
                cf.generateCertificate(it) as X509Certificate
            } ?: throw IllegalArgumentException("Could not read CA certificate")
            context.contentResolver.openInputStream(android.net.Uri.parse(caCertUri))?.use { inp ->
                internalFile.outputStream().use { out -> inp.copyTo(out) }
            }
            loaded
        }

        val keyStore = KeyStore.getInstance(KeyStore.getDefaultType()).apply {
            load(null, null)
            setCertificateEntry("ca", cert)
        }

        val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()).apply {
            init(keyStore)
        }

        val sslContext = SSLContext.getInstance("TLS").apply {
            init(null, tmf.trustManagers, SecureRandom())
        }
        return sslContext.socketFactory
    }
}
