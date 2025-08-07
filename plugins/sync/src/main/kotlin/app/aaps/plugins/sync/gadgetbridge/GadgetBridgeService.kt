package app.aaps.plugins.sync.gadgetbridge

import android.app.Service
import android.content.Intent
import android.os.Binder
import android.os.IBinder
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.notifications.NotificationHolder
import app.aaps.core.interfaces.rx.AapsSchedulers
import app.aaps.core.interfaces.rx.bus.RxBus
import app.aaps.core.interfaces.rx.events.EventMobileToWear
import app.aaps.core.interfaces.rx.weardata.EventData
import app.aaps.core.keys.interfaces.Preferences
import app.aaps.plugins.sync.gadgetbridge.keys.GadgetBridgeStringKey
import dagger.android.AndroidInjection
import io.reactivex.rxjava3.disposables.CompositeDisposable
import io.reactivex.rxjava3.kotlin.plusAssign
import org.json.JSONObject
import javax.inject.Inject

class GadgetBridgeService : Service() {

    // The service now only needs these simple, globally-available dependencies.
    @Inject lateinit var rxBus: RxBus
    @Inject lateinit var aapsSchedulers: AapsSchedulers
    @Inject lateinit var aapsLogger: AAPSLogger
    @Inject lateinit var notificationHolder: NotificationHolder
    @Inject lateinit var preferences: Preferences
    @Inject lateinit var gadgetBridgePlugin: GadgetBridgePlugin

    private val disposable = CompositeDisposable()
    private val binder = LocalBinder()

    inner class LocalBinder : Binder() {
        fun getService(): GadgetBridgeService = this@GadgetBridgeService
    }

    override fun onBind(intent: Intent?): IBinder = binder

    override fun onCreate() {
        AndroidInjection.inject(this)
        super.onCreate()
        startForeground(NOTIFICATION_ID, notificationHolder.notification)
        aapsLogger.debug(LTag.GADGETBRIDGE, "GadgetBridgeService created and listening.")


        // Now, you can check if the plugin is enabled before doing work.
        if (!gadgetBridgePlugin.isEnabled()) {
            stopSelf() // Stop the service if the plugin has been disabled.
            return
        }


        // --- SUBSCRIBE TO ALL NECESSARY EVENT TYPES ---

        // 1. Listen for wrapped events (like ConfirmAction, OpenSettings)
        disposable += rxBus
            .toObservable(EventMobileToWear::class.java)
            .observeOn(aapsSchedulers.io)
            .subscribe({ event ->
                           aapsLogger.debug(LTag.GADGETBRIDGE, "Caught EventMobileToWear. Forwarding payload: ${event.payload.javaClass.simpleName}")
                           forwardEventToDevice(event.payload)
                       }, { error ->
                           aapsLogger.error(LTag.GADGETBRIDGE, "Error in EventMobileToWear subscription", error)
                       })

        // 2. Listen for DIRECT Status updates
        disposable += rxBus
            .toObservable(EventData.Status::class.java)
            .observeOn(aapsSchedulers.io)
            .subscribe({ event ->
                           aapsLogger.debug(LTag.GADGETBRIDGE, "Caught direct Status event. Forwarding.")
                           forwardEventToDevice(event)
                       }, { error ->
                           aapsLogger.error(LTag.GADGETBRIDGE, "Error in Status subscription", error)
                       })

        // 3. Listen for DIRECT SingleBg updates (CRITICAL FOR REAL-TIME BG)
        disposable += rxBus
            .toObservable(EventData.SingleBg::class.java)
            .observeOn(aapsSchedulers.io)
            .subscribe({ event ->
                           aapsLogger.debug(LTag.GADGETBRIDGE, "Caught direct SingleBg event. Forwarding.")
                           forwardEventToDevice(event)
                       }, { error ->
                           aapsLogger.error(LTag.GADGETBRIDGE, "Error in SingleBg subscription", error)
                       })

        // 4. Listen for DIRECT GraphData updates (CRITICAL FOR INITIAL SYNC)
        disposable += rxBus
            .toObservable(EventData.GraphData::class.java)
            .observeOn(aapsSchedulers.io)
            .subscribe({ event ->
                           aapsLogger.debug(LTag.GADGETBRIDGE, "Caught direct GraphData event. Forwarding.")
                           forwardEventToDevice(event)
                       }, { error ->
                           aapsLogger.error(LTag.GADGETBRIDGE, "Error in GraphData subscription", error)
                       })

        // 5. Listen for DIRECT TreatmentData updates (CRITICAL FOR INITIAL SYNC)
        disposable += rxBus
            .toObservable(EventData.TreatmentData::class.java)
            .observeOn(aapsSchedulers.io)
            .subscribe({ event ->
                           aapsLogger.debug(LTag.GADGETBRIDGE, "Caught direct TreatmentData event. Forwarding.")
                           forwardEventToDevice(event)
                       }, { error ->
                           aapsLogger.error(LTag.GADGETBRIDGE, "Error in TreatmentData subscription", error)
                       })
    }

    // This forwardEventToDevice method is now the single point of entry
    // for all events, wrapped or direct.
    private fun forwardEventToDevice(event: EventData) {
        val payloads = GadgetBridgeDataMapper.serialize(event)
        if (payloads != null) {
            for (payload in payloads) {
                broadcastPayload(payload)
            }
        } else {
            aapsLogger.debug(LTag.GADGETBRIDGE, "Skipping unsupported EventData type: ${event.javaClass.simpleName}")
        }
    }

    private fun broadcastPayload(payload: JSONObject) {
        try {
            val configuredAction = preferences.get(GadgetBridgeStringKey.OutgoingIntentAction)
            val intent = Intent(configuredAction)

            // 1. Convert our JSON object to a string.
            val jsonString = payload.toString()

            // 2. Escape the string so it can be safely placed inside another string.
            val escapedJson = jsonString.replace("'", "\\'")

            // 3. Create the final JavaScript command to execute on the watch.
            // This calls a function 'handleAAPSData' that we will define on the watch.
            val jsCommand = "handleAAPSData('$escapedJson');\n"

            // 4. THE CRITICAL FIX: Send the JavaScript command in the "line" extra.
            intent.putExtra("line", jsCommand)

            sendBroadcast(intent)
            aapsLogger.debug(LTag.GADGETBRIDGE, "Broadcasted JS command to '$configuredAction'")
        } catch (e: Exception) {
            aapsLogger.error(LTag.GADGETBRIDGE, "Failed to broadcast GadgetBridge payload", e)
        }
    }

    override fun onDestroy() {
        disposable.clear()
        super.onDestroy()
        aapsLogger.debug(LTag.GADGETBRIDGE, "GadgetBridgeService destroyed.")
    }

    companion object {
        private const val NOTIFICATION_ID = 457
    }
}