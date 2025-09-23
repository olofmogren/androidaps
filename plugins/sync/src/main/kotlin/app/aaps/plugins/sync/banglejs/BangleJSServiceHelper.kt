package app.aaps.plugins.sync.banglejs

import android.content.Context
import android.content.Intent
import app.aaps.core.interfaces.notifications.NotificationHolder
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class BangleJSServiceHelper @Inject constructor(
    private val notificationHolder: NotificationHolder
) {

    fun startService(context: Context) {
        val intent = Intent(context, BangleJSService::class.java)
        // This helper safely starts the service as a foreground service.
        context.startForegroundService(intent)
    }

    fun stopService(context: Context) {
        context.stopService(Intent(context, BangleJSService::class.java))
    }
}