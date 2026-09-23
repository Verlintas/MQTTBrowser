package com.mbusino.mqttexplorer.viewmodel

import android.app.Application
import android.content.Context
import androidx.lifecycle.AndroidViewModel
import androidx.security.crypto.EncryptedSharedPreferences
import androidx.security.crypto.MasterKeys
import com.mbusino.mqttexplorer.data.ConnectionSettings
import com.mbusino.mqttexplorer.data.ConnectionStorage
import com.mbusino.mqttexplorer.mqtt.ConnectionState
import com.mbusino.mqttexplorer.mqtt.MqttManager
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

class ConnectionViewModel(application: Application) : AndroidViewModel(application) {

    private val mqttManager = MqttManager.getInstance()

    private val masterKeyAlias = MasterKeys.getOrCreate(MasterKeys.AES256_GCM_SPEC)

    private val prefs = try {
        EncryptedSharedPreferences.create(
            "mqtt_connections_encrypted",
            masterKeyAlias,
            application,
            EncryptedSharedPreferences.PrefKeyEncryptionScheme.AES256_SIV,
            EncryptedSharedPreferences.PrefValueEncryptionScheme.AES256_GCM
        )
    } catch (e: Exception) {
        // Fallback: corrupted encrypted prefs, delete and recreate
        application.deleteSharedPreferences("mqtt_connections_encrypted")
        EncryptedSharedPreferences.create(
            "mqtt_connections_encrypted",
            masterKeyAlias,
            application,
            EncryptedSharedPreferences.PrefKeyEncryptionScheme.AES256_SIV,
            EncryptedSharedPreferences.PrefValueEncryptionScheme.AES256_GCM
        )
    }

    init {
        // Migrate old unencrypted SharedPreferences (only once)
        if (!prefs.contains("migration_done")) {
            val oldPrefs = application.getSharedPreferences("mqtt_connections", Context.MODE_PRIVATE)
            if (oldPrefs.all.isNotEmpty()) {
                val oldConnections = ConnectionStorage.getConnections(oldPrefs)
                for (conn in oldConnections) {
                    ConnectionStorage.saveConnection(prefs, conn)
                }
                oldPrefs.edit().clear().apply()
                application.deleteSharedPreferences("mqtt_connections")
            }
            prefs.edit().putBoolean("migration_done", true).apply()
        }
    }

    private val _savedConnections = MutableStateFlow(ConnectionStorage.getConnections(prefs))
    val savedConnections: StateFlow<List<ConnectionSettings>> = _savedConnections.asStateFlow()

    private val _brokerUrl = MutableStateFlow("192.168.1.8")
    val brokerUrl: StateFlow<String> = _brokerUrl.asStateFlow()

    private val _port = MutableStateFlow("1883")
    val port: StateFlow<String> = _port.asStateFlow()

    private val _username = MutableStateFlow("")
    val username: StateFlow<String> = _username.asStateFlow()

    private val _password = MutableStateFlow("")
    val password: StateFlow<String> = _password.asStateFlow()

    private val _connectionName = MutableStateFlow("")
    val connectionName: StateFlow<String> = _connectionName.asStateFlow()

    private val _tls = MutableStateFlow(false)
    val tls: StateFlow<Boolean> = _tls.asStateFlow()

    private val _trustAll = MutableStateFlow(false)
    val trustAll: StateFlow<Boolean> = _trustAll.asStateFlow()

    private val _caCertUri = MutableStateFlow("")
    val caCertUri: StateFlow<String> = _caCertUri.asStateFlow()

    val connectionState: StateFlow<ConnectionState> = mqttManager.connectionState
    val errorMessage: StateFlow<String?> = mqttManager.errorMessage

    fun onBrokerUrlChange(url: String) { _brokerUrl.value = url }
    fun onPortChange(port: String) { _port.value = port }
    fun onUsernameChange(username: String) { _username.value = username }
    fun onPasswordChange(password: String) { _password.value = password }
    fun onConnectionNameChange(name: String) { _connectionName.value = name }
    fun onTlsChange(tls: Boolean) {
        _tls.value = tls
        // Auto-switch port: 1883 (plain) ↔ 8883 (TLS)
        if (tls && _port.value == "1883") {
            _port.value = "8883"
        } else if (!tls && _port.value == "8883") {
            _port.value = "1883"
        }
    }
    fun onTrustAllChange(trustAll: Boolean) { _trustAll.value = trustAll }
    fun onCaCertUriChange(uri: String) {
        // Copy cert to internal storage immediately and store internal path
        try {
            val context = getApplication<Application>()
            val internalFile = java.io.File(context.filesDir, "custom_ca.crt")
            context.contentResolver.openInputStream(android.net.Uri.parse(uri))?.use { inp ->
                internalFile.outputStream().use { out -> inp.copyTo(out) }
            }
            _caCertUri.value = internalFile.absolutePath
        } catch (e: Exception) {
            _caCertUri.value = uri
        }
    }

    fun connect() {
        val settings = ConnectionSettings(
            name = _connectionName.value.ifBlank { "${_brokerUrl.value}:${_port.value}" },
            brokerUrl = _brokerUrl.value,
            port = _port.value,
            username = _username.value,
            password = _password.value,
            tls = _tls.value,
            trustAll = _trustAll.value,
            caCertUri = _caCertUri.value,
            subscribeMode = mqttManager.subscribeModeName(),
            subscribeFilter = mqttManager.pathFilterValue()
        )
        mqttManager.connect(settings)
    }

    fun reconnect() {
        mqttManager.reconnect()
    }

    fun hasLastSettings(): Boolean {
        return mqttManager.hasLastSettings()
    }

    fun loadConnection(settings: ConnectionSettings) {
        _brokerUrl.value = settings.brokerUrl
        _port.value = settings.port
        _username.value = settings.username
        _password.value = settings.password
        _connectionName.value = settings.name
        _tls.value = settings.tls
        _trustAll.value = settings.trustAll
        _caCertUri.value = settings.caCertUri
        // v1.21: adopt the connection's persisted subscription mode
        mqttManager.restoreMode(settings.subscribeMode, settings.subscribeFilter)
    }

    fun saveCurrentConnection() {
        val settings = ConnectionSettings(
            name = _connectionName.value.ifBlank { "${_brokerUrl.value}:${_port.value}" },
            brokerUrl = _brokerUrl.value,
            port = _port.value,
            username = _username.value,
            password = _password.value,
            tls = _tls.value,
            trustAll = _trustAll.value,
            caCertUri = _caCertUri.value,
            subscribeMode = mqttManager.subscribeModeName(),
            subscribeFilter = mqttManager.pathFilterValue()
        )
        ConnectionStorage.saveConnection(prefs, settings)
        _savedConnections.value = ConnectionStorage.getConnections(prefs)
    }

    fun deleteConnection(name: String) {
        ConnectionStorage.deleteConnection(prefs, name)
        _savedConnections.value = ConnectionStorage.getConnections(prefs)
    }

    fun disconnect() {
        mqttManager.disconnect()
    }

    fun clearError() {
        mqttManager.clearError()
    }
}
