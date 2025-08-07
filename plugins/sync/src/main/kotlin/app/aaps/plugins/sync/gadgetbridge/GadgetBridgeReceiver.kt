package app.aaps.plugins.sync.gadgetbridge

import android.content.Context
import android.content.Intent
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.rx.bus.RxBus
import app.aaps.plugins.sync.wear.WearPlugin
import dagger.android.DaggerBroadcastReceiver
import javax.inject.Inject

class GadgetBridgeReceiver : DaggerBroadcastReceiver() {
    @Inject lateinit var gadgetBridgePlugin: GadgetBridgePlugin
    @Inject lateinit var rxBus: RxBus
    @Inject lateinit var aapsLogger: AAPSLogger

    companion object {
        const val INCOMING_ACTION = "app.aaps.gadgetbridge.COMMAND"
    }

    override fun onReceive(context: Context, intent: Intent) {
        super.onReceive(context, intent)
        // You can now add a check at the very beginning.

        if (intent.action != INCOMING_ACTION) return

        aapsLogger.debug(LTag.GADGETBRIDGE, "GadgetBridgeReceiver received command.")

        val commandType = intent.getStringExtra("commandType")
        val commandJson = intent.getStringExtra("commandJson")

        if (commandType.isNullOrBlank() || commandJson.isNullOrBlank()) {
            aapsLogger.error(LTag.GADGETBRIDGE, "GadgetBridge command missing type or json.")
            return
        }

        try {
            // Use your mapper to convert the incoming message to a standard AAPS event
            val eventData = GadgetBridgeDataMapper.deserialize(commandType, commandJson)

            if (eventData != null) {
                // The receiver's ONLY job is to put the event on the bus.
                // The correct handler (DataHandlerMobile) will pick it up from here.
                rxBus.send(eventData)
                aapsLogger.debug(LTag.GADGETBRIDGE, "Sent $commandType to RxBus.")
            } else {
                aapsLogger.error(LTag.GADGETBRIDGE, "Unknown command type received: $commandType")
            }
        } catch (e: Exception) {
            aapsLogger.error(LTag.GADGETBRIDGE, "Failed to process GadgetBridge command", e)
        }
    }
}