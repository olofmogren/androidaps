package app.aaps.plugins.sync.gadgetbridge

import android.app.Service
import android.content.Intent
import android.os.Binder
import android.os.IBinder
import app.aaps.core.data.iob.InMemoryGlucoseValue
import app.aaps.core.data.model.GV
import app.aaps.core.interfaces.db.PersistenceLayer
import app.aaps.core.interfaces.db.ProcessedTbrEbData
import app.aaps.core.interfaces.iob.IobCobCalculator
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.notifications.NotificationHolder
import app.aaps.core.interfaces.profile.ProfileFunction
import app.aaps.core.interfaces.rx.AapsSchedulers
import app.aaps.core.interfaces.rx.bus.RxBus
import app.aaps.core.interfaces.rx.events.EventLoopUpdateGui
import app.aaps.core.interfaces.rx.events.EventMobileToWear
import app.aaps.core.interfaces.rx.events.EventNewBG
import app.aaps.core.interfaces.rx.weardata.EventData
import app.aaps.core.interfaces.utils.DecimalFormatter
import app.aaps.core.keys.interfaces.Preferences
import app.aaps.core.objects.extensions.round
import app.aaps.plugins.sync.gadgetbridge.keys.GadgetBridgeStringKey
import dagger.android.AndroidInjection
import io.reactivex.rxjava3.disposables.CompositeDisposable
import io.reactivex.rxjava3.kotlin.plusAssign
import org.json.JSONArray
import org.json.JSONObject
import java.util.concurrent.TimeUnit
import javax.inject.Inject

import app.aaps.core.objects.extensions.convertedToAbsolute



class GadgetBridgeService : Service() {

    // The service now only needs these simple, globally-available dependencies.
    @Inject lateinit var rxBus: RxBus
    @Inject lateinit var aapsSchedulers: AapsSchedulers
    @Inject lateinit var aapsLogger: AAPSLogger
    @Inject lateinit var notificationHolder: NotificationHolder
    @Inject lateinit var preferences: Preferences
    @Inject lateinit var gadgetBridgePlugin: GadgetBridgePlugin
    @Inject lateinit var decimalFormatter: DecimalFormatter
    @Inject lateinit var iobCobCalculator: IobCobCalculator
    @Inject lateinit var persistenceLayer: PersistenceLayer
    @Inject lateinit var profileFunction: ProfileFunction
    @Inject lateinit var processedTbrEbData: ProcessedTbrEbData

    private val disposable = CompositeDisposable()
    private val binder = LocalBinder()

    inner class LocalBinder : Binder() {
        fun getService(): GadgetBridgeService = this@GadgetBridgeService
    }

    private data class SimpleTreatment(
        val timestamp: Long,
        val insulin: Double,
        val carbs: Double
    )
    override fun onBind(intent: Intent?): IBinder = binder

    override fun onCreate() {
        AndroidInjection.inject(this)
        super.onCreate()
        startForeground(NOTIFICATION_ID, notificationHolder.notification)
        aapsLogger.debug(LTag.GADGETBRIDGE, "GadgetBridgeService created and listening.")

        // It listens for the generic "data for a watch" container.
        // DataHandlerMobile puts ALL watch-related data inside this wrapper.
        disposable.clear() // Clear any old subscriptions
        disposable += rxBus
            .toObservable(EventMobileToWear::class.java)
            .observeOn(aapsSchedulers.io)
            .subscribe({ event ->
                           // The actual data (Status, SingleBg, GraphData, etc.) is in the 'payload'.
                           val payload = event.payload
                           aapsLogger.debug(LTag.GADGETBRIDGE, "Caught EventMobileToWear. Forwarding payload: ${payload.javaClass.simpleName}")
                           // We pass this payload to our forwarding logic.
                           forwardEventToDevice(payload)
                       }, { error ->
                           aapsLogger.error(LTag.GADGETBRIDGE, "Error in EventMobileToWear subscription", error)
                       })
        // loop updates
        disposable += rxBus
            .toObservable(EventLoopUpdateGui::class.java)
            .throttleLast(5, TimeUnit.MINUTES, aapsSchedulers.io)
            .observeOn(aapsSchedulers.io)
            .subscribe({
                           aapsLogger.debug(LTag.GADGETBRIDGE, "Loop update triggered. Sending full history in chunks.")
                           sendHistoryToWatch()
                       }, { error ->
                           aapsLogger.error(LTag.GADGETBRIDGE, "Error in EventLoopUpdateGui subscription", error)
                       })
        disposable += rxBus
            .toObservable(EventNewBG::class.java)
            .observeOn(aapsSchedulers.io)
            .subscribe({ event ->
                           aapsLogger.debug(LTag.GADGETBRIDGE, "Caught EventNewBG. Sending to watch.")
                           sendCurrentBgToWatch(event)
                       }, { error ->
                           aapsLogger.error(LTag.GADGETBRIDGE, "Error in EventNewBG subscription", error)
                       })

    }

    // This router now receives ALL event types and decides what to do.
    private fun forwardEventToDevice(event: EventData) {
        aapsLogger.debug(LTag.GADGETBRIDGE, "Skipping unsupported EventData type: ${event.javaClass.simpleName}")

        /*when (event) {
            // For lightweight updates, send the simple file
            //is EventData.SingleBg -> sendCurrentBgToWatch(event)
            // For full updates, trigger the chunked history sync
            is EventData.Status -> sendCurrentStatusToWatch(event)
            // For other events, use the generic mapper
            else -> {
                /*val payloadJson = GadgetBridgeDataMapper.serialize(event)
                if (payloadJson != null) {
                    broadcastPayload(payloadJson)
                } else {
                    aapsLogger.debug(LTag.GADGETBRIDGE, "Skipping unsupported EventData type: ${event.javaClass.simpleName}")
                }*/
                aapsLogger.debug(LTag.GADGETBRIDGE, "Skipping unsupported EventData type: ${event.javaClass.simpleName}")
            }
        }*/
    }

    // Sends ONLY the current BG data to be written to 'current_bg.json'
    private fun sendCurrentBgToWatch(bg: EventNewBG) {
        aapsLogger.debug(LTag.GADGETBRIDGE, "Sending BG to watch: ${bg.javaClass.simpleName}")

        val now = System.currentTimeMillis()
        val fifteenMinutesAgo = now - TimeUnit.MINUTES.toMillis(15)
        val glucoseHistory = persistenceLayer.getBgReadingsDataFromTime(fifteenMinutesAgo, true).blockingGet()

        // Safety check in case the database is empty
        if (glucoseHistory.isEmpty()) {
            aapsLogger.warn(LTag.GADGETBRIDGE, "Queried for recent BG but found none.")
            return
        }

        val e = glucoseHistory.last()
        var fiveMinDiff = 0.0
        for (i in glucoseHistory.size-1 downTo 0) {
            if (glucoseHistory[i].timestamp < e.timestamp-241000) {
                fiveMinDiff = e.value-glucoseHistory[i].value
                val fiveMinTimeDiff =  e.timestamp - glucoseHistory[i].timestamp
                aapsLogger.debug(LTag.GADGETBRIDGE, "Calculated five minutes diff on a time difference of ${fiveMinTimeDiff}")
                break
            }
        }



        val cobInfo = iobCobCalculator.getCobInfo("Wizard wear")
        if (cobInfo.displayCob == null) {
            aapsLogger.warn(LTag.GADGETBRIDGE,"No current COB found.")
            return
        }

        var currentBasal = "---"
        val profile = profileFunction.getProfile(now)
        if (profile == null) {
            aapsLogger.warn(LTag.GADGETBRIDGE, "No profile found at timestamp $now, skipping basal point.")
        }
        else {
            // Check for an active temporary basal at this specific timestamp
            val activeTempBasal = processedTbrEbData.getTempBasalIncludingConvertedExtended(now)

            currentBasal = // If a temp basal is active, use its absolute rate
                String.format("%.2f", activeTempBasal?.convertedToAbsolute(now, profile) ?: // Otherwise, use the scheduled basal rate from the profile
                profile.getBasal(now))
            //aapsLogger.debug(LTag.GADGETBRIDGE, "Sending current basal to watch: $currentBasal")
        }

        val bolusIob = iobCobCalculator.calculateIobFromBolus().round()
        val basalIob = iobCobCalculator.calculateIobFromTempBasalsIncludingConvertedExtended().round()
        val iobSum = decimalFormatter.to2Decimal(bolusIob.iob + basalIob.basaliob)

        aapsLogger.debug(LTag.GADGETBRIDGE, "Sending current status to watch, along with the BG.")

        val payload = JSONObject().apply {
            put("sgv", e.value)
            put("trend", e.trendArrow)
            put("delta", fiveMinDiff)
            put("ts", e.timestamp)
            // Also include current IOB/COB for the main display
            put("iob", iobSum)
            put("cob", cobInfo.displayCob)
            put("basal", currentBasal)
        }
        val jsCommand = "require('Storage').writeJSON('aaps_status.json', ${payload.toString()});\n"
        sendJsCommand(jsCommand)
    }


    // This is the master function for sending history.
    private fun sendHistoryToWatch() {
        //val twoHoursAgo = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2)
        val now = System.currentTimeMillis()
        val startTime = now - TimeUnit.MINUTES.toMillis(90)

        // --- Send Glucose History in Chunks ---
        val glucoseHistory = persistenceLayer.getBgReadingsDataFromTime(startTime, true).blockingGet()
        val fiveMinHistory = mutableListOf<GV>()
        val moreThanFourMinutes = 4 * 60 * 1000 + 1000L

        // Always add the first element.
        var lastAddedTimestamp = glucoseHistory[0].timestamp
        fiveMinHistory.add(glucoseHistory[0])

        // Iterate through the rest of the list.
        for (i in 1 until glucoseHistory.size) {
            val currentGlucose = glucoseHistory[i]

            if (currentGlucose.timestamp >= lastAddedTimestamp + moreThanFourMinutes) {
                fiveMinHistory.add(currentGlucose)
                lastAddedTimestamp = currentGlucose.timestamp
            }
        }

        val glucoseChunks = fiveMinHistory.chunked(20) // Split into chunks of 20

        glucoseChunks.forEachIndexed { index, chunk ->
            val payload = JSONObject()
            val history = JSONArray()
            chunk.forEach { reading ->
                history.put(JSONObject().apply { put("ts", reading.timestamp); put("sgv", reading.value) })
            }
            payload.put("glucose", history)

            // Write each chunk to a uniquely named file
            val fileName = "'aaps_h_gl_${index}.json'"
            val jsCommand = "require('Storage').writeJSON($fileName, ${payload.toString()});\n"
            sendJsCommand(jsCommand)
            Thread.sleep(200) // Small delay between writes
        }

        // --- Send Treatment History in Chunks ---
        //val treatments = mutableListOf<SimpleTreatment>()
        // (Logic to get and map boluses/carbs is correct)
        // ...
        val treatments = persistenceLayer.getBolusesFromTimeToTime(startTime, now, true)
        val treatmentChunks = treatments.chunked(15) // Split into chunks of 15

        treatmentChunks.forEachIndexed { index, chunk ->
            val payload = JSONObject()
            val history = JSONArray()
            chunk.forEach { treatment ->
                //TODO: carbs are not communicated!
                history.put(JSONObject().apply { put("ts", treatment.timestamp); put("insulin", treatment.amount); put("carbs", 0) })
            }
            payload.put("treatments", history)

            // Write each chunk to a uniquely named file
            val fileName = "'aaps_h_tr_${index}.json'"
            val jsCommand = "require('Storage').writeJSON($fileName, ${payload.toString()});\n"
            sendJsCommand(jsCommand)
            Thread.sleep(200)
        }

        // --- Send Meta File ---
        // This tells the watch how many chunks to look for.
        val metaPayload = JSONObject()
        metaPayload.put("glucoseChunks", glucoseChunks.size)
        metaPayload.put("treatmentChunks", treatmentChunks.size)
        val metaJsCommand = "require('Storage').writeJSON('aaps_h_meta.json', ${metaPayload.toString()});\n"
        sendJsCommand(metaJsCommand)

        // BASAL HISTORY:
        aapsLogger.debug(LTag.GADGETBRIDGE, "Gathering and sending basal history.")
        val timeStep = TimeUnit.MINUTES.toMillis(1) // Check every minute

        val basalHistoryPoints = JSONArray()
        var lastRate: Double? = null

        // Iterate from 90 minutes ago to now, one minute at a time
        for (timestamp in startTime..now step timeStep) {
            val profile = profileFunction.getProfile(timestamp)
            if (profile == null) {
                aapsLogger.warn(LTag.GADGETBRIDGE, "No profile found at timestamp $timestamp, skipping basal point.")
                continue // Skip this iteration if no profile is active
            }

            // Check for an active temporary basal at this specific timestamp
            val activeTempBasal = processedTbrEbData.getTempBasalIncludingConvertedExtended(timestamp)

            val currentRate = // If a temp basal is active, use its absolute rate
                activeTempBasal?.convertedToAbsolute(timestamp, profile) ?: // Otherwise, use the scheduled basal rate from the profile
                profile.getBasal(timestamp)

            // Round to 3 decimal places to avoid tiny floating point differences
            val roundedRate = decimalFormatter.to3Decimal(currentRate).toDouble()

            // If this is the first point, or if the rate has changed, record it.
            if (lastRate == null || roundedRate != lastRate) {
                basalHistoryPoints.put(JSONObject().apply {
                    put("ts", timestamp)
                    put("rate", roundedRate)
                })
                lastRate = roundedRate
            }
        }

        // Create the final payload and send it to the watch
        val payload = JSONObject()
        payload.put("basals", basalHistoryPoints)

        // Write the data to its own dedicated file on the watch
        val jsCommandBasal = "require('Storage').writeJSON('aaps_basal.json', ${payload.toString()});\n"
        sendJsCommand(jsCommandBasal)
    }


    /*private fun sendHistoryToWatch() {
        aapsLogger.debug(LTag.GADGETBRIDGE, "Sending history to watch.")
        val payload = ""
        val twoHoursAgo = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2)
        val fileName = "'aaps_history.json'"

        // 1. Start the file
        payload.plus("{\"glucose\":[');\n")

        // 2. Append glucose data
        val glucoseHistory = persistenceLayer.getBgReadingsDataFromTime(twoHoursAgo, true).blockingGet()
        var isFirstChunk = true
        glucoseHistory.chunked(15).forEach { chunk ->
            val chunkJson = JSONArray()
            chunk.forEach { reading ->
                chunkJson.put(JSONObject().apply {
                    put("ts", reading.timestamp)
                    put("sgv", reading.value)
                })
            }
            var chunkString = chunkJson.toString().drop(1).dropLast(1)
            if (!isFirstChunk && chunkString.isNotEmpty()) chunkString = ",$chunkString"
            if (chunkString.isNotEmpty()) {
                payload.plus(chunkString)
                isFirstChunk = false
            }
        }

        // 3. Middle of the file
        payload.plus("],\"treatments\":[")

        // 4. Append treatment data (THE CORRECTED LOGIC)
        // a. Create a mutable list of our new simple type
        val simpleTreatments = mutableListOf<SimpleTreatment>()

        // b. Get boluses, MAP them to SimpleTreatment, and add to the list
        persistenceLayer.getBolusesFromTimeIncludingInvalid(twoHoursAgo, true).blockingGet().forEach { bolus ->
            simpleTreatments.add(SimpleTreatment(
                timestamp = bolus.timestamp,
                insulin = bolus.amount,
                carbs = 0.0 // Bolus records don't have carbs
            ))
        }

        // c. Get carbs, MAP them to SimpleTreatment, and add to the list
        persistenceLayer.getCarbsFromTimeExpanded(twoHoursAgo, true).forEach { carbs ->
            simpleTreatments.add(SimpleTreatment(
                timestamp = carbs.timestamp,
                insulin = 0.0, // Carb records don't have insulin
                carbs = carbs.amount
            ))
        }

        // d. Sort the combined list
        simpleTreatments.sortBy { it.timestamp }

        // e. Chunk and send the list of simple objects
        isFirstChunk = true
        simpleTreatments.chunked(10).forEach { chunk ->
            val chunkJson = JSONArray()
            chunk.forEach { treatment ->
                chunkJson.put(JSONObject().apply {
                    put("ts", treatment.timestamp)
                    put("insulin", treatment.insulin)
                    put("carbs", treatment.carbs)
                })
            }
            var chunkString = chunkJson.toString().drop(1).dropLast(1)
            if (!isFirstChunk && chunkString.isNotEmpty()) chunkString = ",$chunkString"
            if (chunkString.isNotEmpty()) {
                payload.plus(chunkString)
                isFirstChunk = false
            }
        }

        // 5. Close the file
        payload.plus("]}")
        sendJsAppendCommand(fileName, payload)
    }

     */

    // Sends a command to the watch to clean up old data points
    private fun sendCleanupCommandToWatch() {
        // JavaScript to read the history, filter out entries older than 3 hours, and write it back.
        val threeHoursAgo = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(3)
        val cleanupJs = """
            (function() {
              var d = require('Storage').readJSON('aaps_history.json',1);
              if (!d) return;
              var changed = false;
              if (d.glucose && d.glucose.length > 0) {
                var o_len = d.glucose.length;
                d.glucose = d.glucose.filter(p => p.ts > ${threeHoursAgo}L);
                if (o_len != d.glucose.length) changed = true;
              }
              if (d.treatments && d.treatments.length > 0) {
                var o_len = d.treatments.length;
                d.treatments = d.treatments.filter(p => p.ts > ${threeHoursAgo}L);
                if (o_len != d.treatments.length) changed = true;
              }
              if (changed) require('Storage').writeJSON('aaps_history.json', d);
            })();
        """.trimIndent() + "\n"

        sendJsCommand(cleanupJs)
    }

    // Generic function to send any JS command
    private fun sendJsCommand(command: String) {
        try {
            val intent = Intent("com.banglejs.uart.tx")
            intent.putExtra("line", command)
            sendBroadcast(intent)
            aapsLogger.debug(LTag.GADGETBRIDGE, "Sent JS command: ${command.take(80)}...")
        } catch (e: Exception) {
            aapsLogger.error(LTag.GADGETBRIDGE, "Failed to broadcast JS command", e)
        }
    }

    // Helper function to create and send an append command
    private fun sendJsAppendCommand(fileName: String, data: String) {
        // We must escape the data string to safely put it inside the JS command string
        val escapedData = data.replace("'", "\\'")
        val jsCommand = "require('Storage').append($fileName, '$escapedData');\n"
        sendJsCommand(jsCommand)
        Thread.sleep(100) // Delay after each append
    }


    /*private fun broadcastPayload(payload: JSONObject) {
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
    }*/

    override fun onDestroy() {
        disposable.clear()
        super.onDestroy()
        aapsLogger.debug(LTag.GADGETBRIDGE, "GadgetBridgeService destroyed.")
    }

    companion object {
        private const val NOTIFICATION_ID = 457
    }
}