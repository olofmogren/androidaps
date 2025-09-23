package app.aaps.plugins.sync.banglejs

import android.content.Context
import androidx.preference.PreferenceCategory
import androidx.preference.PreferenceManager
import androidx.preference.PreferenceScreen
import app.aaps.core.data.model.HR
import app.aaps.core.data.model.SC
import app.aaps.core.data.plugin.PluginType
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.plugin.PluginBaseWithPreferences
import app.aaps.core.interfaces.plugin.PluginDescription
import app.aaps.core.interfaces.resources.ResourceHelper
import app.aaps.core.interfaces.rx.bus.RxBus
import app.aaps.core.keys.interfaces.Preferences
import app.aaps.core.validators.DefaultEditTextValidator
import app.aaps.core.validators.preferences.AdaptiveStringPreference
import app.aaps.plugins.sync.R
import app.aaps.plugins.sync.banglejs.keys.BangleJSBooleanKey
import app.aaps.plugins.sync.banglejs.keys.BangleJSStringKey
import app.aaps.plugins.sync.wear.wearintegration.DataHandlerMobile
import java.net.HttpURLConnection
import java.net.SocketAddress
import java.net.URI
import javax.inject.Inject
import javax.inject.Singleton
import app.aaps.core.interfaces.db.PersistenceLayer

@Singleton
class BangleJSPlugin @Inject constructor(
    private val context: Context,
    aapsLogger: AAPSLogger,
    rh: ResourceHelper,
    preferences: Preferences, // Inject the base Preferences interface
    private val bangleJSServiceHelper: BangleJSServiceHelper,
    private val dataHandlerMobile: DataHandlerMobile,
    private val persistenceLayer: PersistenceLayer

) : PluginBaseWithPreferences(
    pluginDescription = PluginDescription()
        .mainType(PluginType.SYNC)
        .pluginIcon(app.aaps.core.objects.R.drawable.ic_watch)
        .pluginName(R.string.banglejs)
        .shortName(R.string.banglejs_short)
        .preferencesId(PluginDescription.PREFERENCE_SCREEN)
        .description(R.string.banglejs_description)
        .fragmentClass(BangleJSFragment::class.java.name),
    // Declare the custom preference keys your plugin owns
    ownPreferences = listOf(BangleJSBooleanKey::class.java, BangleJSStringKey::class.java),
    aapsLogger, rh, preferences


) {
    @Inject lateinit var rxBus: RxBus
    private var httpServer: HttpServer? = null

    override fun onStart() {
        super.onStart()
        aapsLogger.debug(LTag.BANGLEJS, "onStart() called. Plugin is becoming active.")
        //if (isEnabled()) {
            //aapsLogger.debug(LTag.BANGLEJS, "GadgetBridge Plugin enabled. Starting Service.")
            bangleJSServiceHelper.startService(context)
            Thread {
                try {
                    Thread.sleep(5000) // Wait 5 seconds
                    aapsLogger.debug(LTag.BANGLEJS, "Triggering initial data sync from onStart().")
                    dataHandlerMobile.resendData("BangleJSPlugin")
                } catch (e: InterruptedException) {
                    // Ignore
                }
            }.start()

        if (isEnabled()) {
            startHttpServer()
        }
        //}
        //else {
        //    aapsLogger.debug(LTag.BANGLEJS, "onStart() called but plguin is not enabled.")
        //}

    }

    override fun onStop() {
        aapsLogger.debug(LTag.BANGLEJS, "BangleJS Plugin stopped. Stopping Service.")
        bangleJSServiceHelper.stopService(context)
        stopHttpServer()
        super.onStop()
    }

    //override fun isEnabled(): Boolean {
    //    // The enabled state is determined by our custom boolean key
    //    val isEnabled = preferences.get(BangleJSBooleanKey.Enabled)
    //    aapsLogger.debug(LTag.BANGLEJS, "isEnabled() check: returning $isEnabled")
    //    return isEnabled
    //}
    private fun startHttpServer() {
        if (httpServer != null) return
        aapsLogger.debug(LTag.BANGLEJS, "Starting local HTTP server on port 28891.")
        httpServer = HttpServer(aapsLogger, 28891).apply {
            // Register an endpoint for our commands
            registerEndpoint("/command", ::handleWatchRequest)
            registerEndpoint("/heartrate", ::handleHeartRateUpload)
            registerEndpoint("/steps", ::handleStepsUpload)
        }
    }

    private fun stopHttpServer() {
        httpServer?.close()
        httpServer = null
    }

    // This function will be called by the server when a request arrives.
    private fun handleWatchRequest(caller: SocketAddress, uri: URI, body: String?): Pair<Int, CharSequence> {
        val commandType = getQueryParameter(uri, "commandType")
        val commandJson = getQueryParameter(uri, "commandJson") // Get the new parameter

        aapsLogger.debug(LTag.BANGLEJS, "HTTP Server received command: $commandType with data: $commandJson")

        if (commandType != null && commandJson != null) {
            // Use your existing mapper to deserialize the JSON and send it to the RxBus.
            aapsLogger.debug(LTag.BANGLEJS, "handleWatchRequest: commandType: \"$commandType\", jsonString: \"$commandJson\"")

            val eventData = BangleJSDataMapper.deserialize(commandType, commandJson)
            if (eventData != null) {
                rxBus.send(eventData)
            }
        }

        // Send a simple "OK" response back to the watch
        return HttpURLConnection.HTTP_OK to "{\"status\":\"ok\"}"
    }

    // Handler for incoming heart rate data
    private fun handleHeartRateUpload(caller: SocketAddress, uri: URI, body: String?): Pair<Int, CharSequence> {
        try {
            // Get the 'bpm' value from the URL's query string
            val bpm = getQueryParameter(uri, "bpm")?.toDoubleOrNull()

            if (bpm == null) {
                aapsLogger.warn(LTag.BANGLEJS, "Received HR upload without valid 'bpm' parameter.")
                return HttpURLConnection.HTTP_BAD_REQUEST to "{\"status\":\"error\", \"message\":\"bpm parameter missing or invalid\"}"
            }

            aapsLogger.debug(LTag.BANGLEJS, "Received Heart Rate from watch: $bpm BPM")

            // Create a Heart Rate (HR) database object
            val hr = HR(
                timestamp = System.currentTimeMillis(),
                beatsPerMinute = bpm,
                duration = 0L,
                device = "Bangle.js" // Identify the source
            )

            // Save the new HR reading to the AAPS database
            persistenceLayer.insertOrUpdateHeartRate(hr).subscribe()

            // Send a success response back to the watch
            return HttpURLConnection.HTTP_OK to "{\"status\":\"ok\"}"

        } catch (e: Exception) {
            aapsLogger.error(LTag.BANGLEJS, "Error processing heart rate upload", e)
            return HttpURLConnection.HTTP_INTERNAL_ERROR to "{\"status\":\"error\"}"
        }
    }

    // Handler for incoming step count data
    private fun handleStepsUpload(caller: SocketAddress, uri: URI, body: String?): Pair<Int, CharSequence> {
        try {
            // Get the 'steps' value from the URL's query string
            val steps = getQueryParameter(uri, "steps")?.toIntOrNull()

            if (steps == null) {
                aapsLogger.warn(LTag.BANGLEJS, "Received steps upload without valid 'steps' parameter.")
                return HttpURLConnection.HTTP_BAD_REQUEST to "{\"status\":\"error\", \"message\":\"steps parameter missing or invalid\"}"
            }

            aapsLogger.debug(LTag.BANGLEJS, "Received Step Count from watch: $steps steps")

            // The SC (Step Count) object in AAPS is complex and designed for intervals.
            // For simplicity, we will log the daily step count in a way that AAPS can use.
            // A full implementation might require more complex logic to fit the SC model.
            // For now, we can create a simple entry.
            val sc = SC(
                timestamp = System.currentTimeMillis(),
                duration = 0L, // This is an instantaneous reading, so duration is 0
                steps5min = 0,
                steps10min = 0,
                steps15min = 0,
                steps30min = 0,
                steps60min = 0,
                steps180min = steps,
                device = "Bangle.js"
            )

            // Save the new step count data to the AAPS database
            persistenceLayer.insertOrUpdateStepsCount(sc).subscribe()

            return HttpURLConnection.HTTP_OK to "{\"status\":\"ok\"}"

        } catch (e: Exception) {
            aapsLogger.error(LTag.BANGLEJS, "Error processing steps upload", e)
            return HttpURLConnection.HTTP_INTERNAL_ERROR to "{\"status\":\"error\"}"
        }
    }

    // Helper function to parse query parameters from the URI
    private fun getQueryParameter(uri: URI, name: String): String? =
        uri.query?.split('&')?.mapNotNull {
            val parts = it.split('=', limit = 2)
            if (parts.size == 2 && parts[0] == name) parts[1] else null
        }?.firstOrNull()

    override fun addPreferenceScreen(preferenceManager: PreferenceManager, parent: PreferenceScreen, context: Context, requiredKey: String?) {
        val category = PreferenceCategory(context)
        parent.addPreference(category)
        category.apply {
            key = "banglejs_settings"
            title = "BangleJS Settings"
            initialExpandedChildrenCount = 0

            //addPreference(AdaptiveSwitchPreference(ctx = context, booleanKey = BangleJSBooleanKey.Enabled, summary = R.string.banglejs_enable_sync_summary, title = R.string.banglejs_enable_sync))

            // This preference configures the OUTGOING intent action.
            /*
            addPreference(
                AdaptiveStringPreference(
                    ctx = context,
                    stringKey = BangleJSStringKey.OutgoingIntentAction,
                    title = R.string.banglejs_setting_intent_action_title,
                    summary = R.string.gadgetbridge_setting_intent_action_summary,
                    validatorParams = DefaultEditTextValidator.Parameters(emptyAllowed = false)
                )
            )*/
        }
    }

}