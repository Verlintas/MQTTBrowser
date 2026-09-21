package com.mbusino.mqttexplorer

import android.app.Application
import android.content.Context

class MqttExplorerApp : Application() {

    init {
        instance = this
    }

    override fun onCreate() {
        super.onCreate()
    }

    companion object {
        private lateinit var instance: MqttExplorerApp
        fun getAppContext(): Context = instance.applicationContext
    }
}
