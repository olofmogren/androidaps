package app.aaps.plugins.sync.gadgetbridge

import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.content.ServiceConnection
import android.os.IBinder
import app.aaps.core.interfaces.notifications.NotificationHolder
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class GadgetBridgeServiceHelper @Inject constructor(
    private val notificationHolder: NotificationHolder
) {

    fun startService(context: Context) {
        val intent = Intent(context, GadgetBridgeService::class.java)
        // This helper safely starts the service as a foreground service.
        context.startForegroundService(intent)
    }

    fun stopService(context: Context) {
        context.stopService(Intent(context, GadgetBridgeService::class.java))
    }
}