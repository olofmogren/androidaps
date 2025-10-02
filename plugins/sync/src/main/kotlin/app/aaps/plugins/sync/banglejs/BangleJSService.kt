package app.aaps.plugins.sync.banglejs

import android.app.Service
import android.content.Intent
import android.os.Binder
import android.os.IBinder
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
import dagger.android.AndroidInjection
import io.reactivex.rxjava3.disposables.CompositeDisposable
import io.reactivex.rxjava3.kotlin.plusAssign
import org.json.JSONObject
import java.util.concurrent.TimeUnit
import javax.inject.Inject

import app.aaps.core.objects.extensions.convertedToAbsolute
import org.json.JSONArray

class BangleJSService : Service() {

    // The service now only needs these simple, globally-available dependencies.
    @Inject lateinit var rxBus: RxBus
    @Inject lateinit var aapsSchedulers: AapsSchedulers
    @Inject lateinit var aapsLogger: AAPSLogger
    @Inject lateinit var notificationHolder: NotificationHolder
    @Inject lateinit var preferences: Preferences
    @Inject lateinit var decimalFormatter: DecimalFormatter
    @Inject lateinit var iobCobCalculator: IobCobCalculator
    @Inject lateinit var persistenceLayer: PersistenceLayer
    @Inject lateinit var profileFunction: ProfileFunction
    @Inject lateinit var processedTbrEbData: ProcessedTbrEbData

    private val disposable = CompositeDisposable()
    private val binder = LocalBinder()
    private var lastHistorySyncTimestamp: Long = 0L
    private var lastWrittenEventFile: Int = 0

    val BASALS_HISTORY_FILENAME = "aaps.history.basals"
    val INSULIN_HISTORY_FILENAME = "aaps.history.insulin"
    val CARBS_HISTORY_FILENAME = "aaps.history.carbs"
    val BG_HISTORY_FILENAME = "aaps.history.bg"
    val CURRENT_STATUS_FILENAME = "aaps.current.status"
    val EVENT_FILE_PREFIX = "aaps.events."
    val MAX_NUMBER_OF_EVENT_FILES = 5


    inner class LocalBinder : Binder() {
        fun getService(): BangleJSService = this@BangleJSService
    }
    override fun onBind(intent: Intent?): IBinder = binder

    // --- THIS IS THE NEW "FRONT DOOR" for commands from the watch ---
    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        aapsLogger.debug(LTag.BANGLEJS, "Service received a service intent.")
        return START_STICKY
    }

    override fun onCreate() {
        AndroidInjection.inject(this)
        super.onCreate()
        startForeground(NOTIFICATION_ID, notificationHolder.notification)
        aapsLogger.debug(LTag.BANGLEJS, "BangleJSService created and listening.")

        // --- THE NEW, SIMPLIFIED SUBSCRIPTION LOGIC ---
        disposable.clear()

        // This subscription listens for data prepared by DataHandlerMobile for any watch.
        disposable += rxBus
            .toObservable(EventMobileToWear::class.java)
            .observeOn(aapsSchedulers.io)
            .subscribe({ event ->
                           // The actual data (Status, SingleBg, GraphData, etc.) is in the 'payload'.
                           val payload = event.payload
                           aapsLogger.debug(LTag.BANGLEJS, "Caught EventMobileToWear. Forwarding payload: ${payload.javaClass.simpleName}")
                           forwardEventToDevice(payload)
                       }, { error ->
                           aapsLogger.error(LTag.BANGLEJS, "Error in EventMobileToWear subscription", error)
                       })
        // loop updates
        disposable += rxBus
            .toObservable(EventLoopUpdateGui::class.java)
            .throttleLast(5, TimeUnit.MINUTES, aapsSchedulers.io)
            .observeOn(aapsSchedulers.io)
            .subscribe({
                           aapsLogger.debug(LTag.BANGLEJS, "Loop update triggered. Sending full history.")
                           sendHistoryToWatch(null)
                       }, { error ->
                           aapsLogger.error(LTag.BANGLEJS, "Error in EventLoopUpdateGui subscription", error)
                       })
        disposable += rxBus
            .toObservable(EventNewBG::class.java)
            .observeOn(aapsSchedulers.io)
            .subscribe({ event ->
                           aapsLogger.debug(LTag.BANGLEJS, "Caught EventNewBG. Sending to watch.")
                           sendCurrentBgToWatch(event)
                       }, { error ->
                           aapsLogger.error(LTag.BANGLEJS, "Error in EventNewBG subscription", error)
                       })

    }

    // This router now receives ALL event types and decides what to do.
    private fun forwardEventToDevice(event: EventData) {
        when (event) {
            //is EventData.SingleBg -> sendCurrentBgToWatch(event as EventData.SingleBg)
            //is EventData.Status -> sendCurrentStatusToWatch(event)
            is EventData.GraphData -> sendHistoryToWatch("graphdata")
            is EventData.TreatmentData -> sendHistoryToWatch("treatments")
            is EventData.ConfirmAction -> sendConfirmAction(event)
            else -> {
                aapsLogger.debug(LTag.BANGLEJS, "Skipping unsupported EventData type: ${event.javaClass.simpleName}")
            }
        }
    }

    private fun sendCurrentBgToWatch(bg: EventNewBG) {
        aapsLogger.debug(LTag.BANGLEJS, "Sending BG to watch: ${bg.javaClass.simpleName}")

        val now = System.currentTimeMillis()
        val fifteenMinutesAgo = now - TimeUnit.MINUTES.toMillis(15)
        val glucoseHistory = persistenceLayer.getBgReadingsDataFromTime(fifteenMinutesAgo, true).blockingGet()

        // Safety check in case the database is empty
        if (glucoseHistory.isEmpty()) {
            aapsLogger.warn(LTag.BANGLEJS, "Queried for recent BG but found none.")
            return
        }

        val e = glucoseHistory.last()
        var fiveMinDiff = 0.0
        for (i in glucoseHistory.size-1 downTo 0) {
            if (glucoseHistory[i].timestamp < e.timestamp-241000) {
                fiveMinDiff = e.value-glucoseHistory[i].value
                val fiveMinTimeDiff =  e.timestamp - glucoseHistory[i].timestamp
                aapsLogger.debug(LTag.BANGLEJS, "Calculated five minutes diff on a time difference of ${fiveMinTimeDiff}")
                break
            }
        }

        val cobInfo = iobCobCalculator.getCobInfo("Wizard wear")
        if (cobInfo.displayCob == null) {
            aapsLogger.warn(LTag.BANGLEJS, "No current COB found.")
            return
        }

        var currentBasal = "---"
        val profile = profileFunction.getProfile(now)
        if (profile == null) {
            aapsLogger.warn(LTag.BANGLEJS, "No profile found at timestamp $now, skipping basal point.")
        }
        else {
            // Check for an active temporary basal at this specific timestamp
            val activeTempBasal = processedTbrEbData.getTempBasalIncludingConvertedExtended(now)

            currentBasal = // If a temp basal is active, use its absolute rate
                String.format("%.2f", activeTempBasal?.convertedToAbsolute(now, profile) ?: // Otherwise, use the scheduled basal rate from the profile
                profile.getBasal(now))
            //aapsLogger.debug(LTag.BANGLEJS, "Sending current basal to watch: $currentBasal")
        }

        val bolusIob = iobCobCalculator.calculateIobFromBolus().round()
        val basalIob = iobCobCalculator.calculateIobFromTempBasalsIncludingConvertedExtended().round()
        val iobSum = decimalFormatter.to2Decimal(bolusIob.iob + basalIob.basaliob)

        aapsLogger.debug(LTag.BANGLEJS, "Sending current status to watch, along with the BG.")
        val dataObject = JSONObject().apply {
            put("ts", e.timestamp)
            put("sgv", e.value)
            put("trend", e.trendArrow)
            put("delta", fiveMinDiff)
            put("iob", iobSum)
            put("cob", cobInfo.displayCob)
            put("basal", currentBasal)
        }

        val dataString = dataObject.toString()
        sendDataToWatch(dataString, CURRENT_STATUS_FILENAME)
    }


    private fun sendConfirmAction(action: EventData.ConfirmAction) {
        aapsLogger.debug(LTag.BANGLEJS, "Sending action to watch: ${action.title}")
        //TODO: this function is a bit of a hack. json objects are discarded and custom events are sent to watch.

        // 1. Create the JSON payload from the ConfirmAction event.
        val jsonEvent = JSONObject().apply {
            put("eventType", "ConfirmAction")
            put("title", action.title)
            //put("message", action.message)
            val message = action.message.replace("\"", "").replace("\n", "<br/>")
            put("message", message)

            // 2. Serialize the 'returnCommand' if it exists.
            // This is the command the watch must send back if the user confirms.
            action.returnCommand?.let { returnCmd ->
                when (returnCmd) {
                    is EventData.ActionBolusConfirmed         -> {
                        val returnCmdJson = JSONObject().apply {
                            put("insulin", returnCmd.insulin)
                            put("carbs", returnCmd.carbs)
                        }
                        put("returnCommandType", "ActionBolusConfirmed")
                        //val returnCmdJsonStr = returnCmdJson.toString().replace("\n", "")
                        put("returnCommandJson", returnCmdJson)
                        //val message = action.message.replace("\"", "\\\"").replace(",", ".").replace("\n", "\\\\n")
                        //TODO: removing quotation marks below works because we only have them on the keys. the values are all numerical.
                        //sendCSVEventToWatchQueue("eventType:ConfirmAction,returnCommandType:ActionBolusConfirmed,returnCommandUrl:insulin=${returnCmd.insulin}&carbs=${returnCmd.carbs},message:${message}")
                    }

                    is EventData.ActionProfileSwitchConfirmed -> {
                        val returnCmdJson = JSONObject().apply {
                            put("percentage", returnCmd.percentage)
                            put("timeShift", returnCmd.timeShift)
                            put("duration", returnCmd.duration)
                        }
                        put("returnCommandType", "ActionProfileSwitchConfirmed")
                        //val returnCmdJsonStr = returnCmdJson.toString().replace("\n", "")
                        put("returnCommandJson", returnCmdJson)
                        //val message = action.message.replace("\"", "\\\"").replace(",", ".").replace("\n", "\\\\n")
                        //TODO: removing quotation marks below works because we only have them on the keys. the values are all numerical.
                        //sendCSVEventToWatchQueue("eventType:ConfirmAction,returnCommandType:ActionProfileSwitchConfirmed,returnCommandUrl:percentage=${returnCmd.percentage}&timeShift=${returnCmd.timeShift}&duration=${returnCmd.duration},message:${message}")
                    }

                    is EventData.ActionTempTargetConfirmed    -> {
                        val returnCmdJson = JSONObject().apply {
                            put("duration", returnCmd.duration)
                            put("low", returnCmd.low)
                            put("high", returnCmd.high)
                            put("isMgdl", returnCmd.isMgdl)
                        }
                        put("returnCommandType", "ActionTempTargetConfirmed")
                        //val returnCmdJsonStr = returnCmdJson.toString().replace("\n", "")
                        put("returnCommandJson", returnCmdJson)
                        //val message = action.message.replace("\"", "\\\"").replace(",", ".").replace("\n", "\\\\n")
                        //TODO: removing quotation marks below works because we only have them on the keys. the values are all numerical.
                        //sendCSVEventToWatchQueue("eventType:ConfirmAction,returnCommandType:ActionTempTargetConfirmed,returnCommandUrl:duration=${returnCmd.duration}&low=${returnCmd.low}&high=${returnCmd.high}&isMgdl=${returnCmd.isMgdl},message:${message}")
                    }

                    // Add other 'is' checks here for other command types
                    // (e.g., ActionTempTargetConfirmed) as you implement them.
                    else                                      -> {
                        // Do nothing if we don't know how to serialize the return command
                        aapsLogger.debug(LTag.BANGLEJS, "Unknown ConfirmAction. Not sending to watch: ${action.title}")
                        return
                    }
                }
            }
        }

        val eventStr = jsonEvent.toString().replace("\n", "")
        aapsLogger.debug(LTag.BANGLEJS, "Sending to watch: ${eventStr}")
        sendEventToWatchQueue(eventStr)
    }

    /**
     * Checks if enough time has passed and triggers a history sync if needed.
     */
    private fun triggerHistorySyncIfNeeded() {
        val now = System.currentTimeMillis()
        val fiveMinutesInMillis = TimeUnit.MINUTES.toMillis(5)

        // The core logic: check if 5 minutes have passed since the last sync.
        if (now - lastHistorySyncTimestamp > fiveMinutesInMillis) {
            aapsLogger.debug(LTag.BANGLEJS, "Sufficient time has passed. Starting history sync.")

            // Perform the sync.
            sendHistoryToWatch(null)

            // IMPORTANT: Update the timestamp after the sync is complete.
            lastHistorySyncTimestamp = now

        } else {
            // Not enough time has passed, so we skip this trigger.
            val minutesSinceLastSync = TimeUnit.MILLISECONDS.toMinutes(now - lastHistorySyncTimestamp)
            aapsLogger.debug(LTag.BANGLEJS, "Skipping history sync. Last sync was $minutesSinceLastSync minutes ago.")
        }
    }
    // This is the master function for sending history.
    private fun sendHistoryToWatch(t:String?) {
        //val twoHoursAgo = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(2)
        val now = System.currentTimeMillis()
        val startTime = (now - TimeUnit.MINUTES.toMillis(90) ) % 60000L // look at beginning of minute. Zero seconds, zero millis. Neccessary to not get duplicates of basals when rechecking them.

        if (t == null || t == "graphdata") {
            val glucoseHistory = persistenceLayer.getBgReadingsDataFromTime(startTime, true).blockingGet()
            val fiveMinHistory = mutableListOf<GV>()
            val moreThanFourMinutes = 271_000L // 4.5 * 60 * 1000 + 1000L

            // Always add the first element.
            if (glucoseHistory.isNullOrEmpty()) {
                return
            }
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

            //var csvSnippet = mutableListOf<String>()
            val bgList = JSONArray()
            //val shortFiveMinHistory = fiveMinHistory.subList(fiveMinHistory.size-15, fiveMinHistory.size)
            fiveMinHistory.forEach { reading ->
                //csvSnippet.add("ts:${reading.timestamp},sgv:${reading.value}")
                bgList.put(JSONObject().apply {
                    put("ts", reading.timestamp)
                    put("sgv", reading.value)
                })
            }
            val bgObject = JSONObject().apply {
                put("data", bgList)
            }

            //var dataString = csvSnippet.joinToString( "\\n" )
            //var escapedData = dataString.replace("\"", "\\\"")
            val bgString = bgObject.toString().replace("\n", "")

            if (bgString.isNotEmpty()) {
                //sendJsCommandHelper("require(\"Storage\").write(\"${BG_HISTORY_FILENAME}\", \"${escapedData}\"); console.log(\"AAPS Phone wrote ${BG_HISTORY_FILENAME}.\")")
                sendDataToWatch(bgString, BG_HISTORY_FILENAME)
            }
        }


        if (t == null || t == "treatments") {
            val insulinTreatments = persistenceLayer.getBolusesFromTimeToTime(startTime, now, true)

            /*csvSnippet = mutableListOf<String>()
        insulinTreatments.forEach { treatment ->
            csvSnippet.add("ts:${treatment.timestamp},insulin:${treatment.amount}")
        }

        dataString = csvSnippet.joinToString( "\\n" )*/
            //escapedData = dataString.replace("\"", "\\\"")

            val insulinList = JSONArray()
            insulinTreatments.forEach { treatment ->
                insulinList.put(JSONObject().apply {
                    put("ts", treatment.timestamp)
                    put("insulin", treatment.amount)
                })
            }
            val insulinObject = JSONObject().apply {
                put("data", insulinList)
            }
            val insulinString = insulinObject.toString().replace("\n", "")

            if (insulinString.isNotEmpty()) {
                //sendJsCommandHelper("require(\"Storage\").write(\"${INSULIN_HISTORY_FILENAME}\", \"${escapedData}\"); console.log(\"AAPS Phone wrote ${INSULIN_HISTORY_FILENAME}.\")")
                sendDataToWatch(insulinString, INSULIN_HISTORY_FILENAME)
            }

            val carbsTreatments = persistenceLayer.getCarbsFromTimeToTimeExpanded(startTime, now, true)
            /*
        carbsTreatments.forEach { treatment ->
            csvSnippet.add("ts:${treatment.timestamp},carbs:${treatment.amount}")
        }

        dataString = csvSnippet.joinToString( "\\n" )
        //escapedData = dataString.replace("\"", "\\\"")


 */

            val carbsList = JSONArray()
            carbsTreatments.forEach { treatment ->
                carbsList.put(JSONObject().apply {
                    put("ts", treatment.timestamp)
                    put("carbs", treatment.amount)
                })
            }
            val carbsObject = JSONObject().apply {
                put("data", carbsList)
            }
            val carbsString = carbsObject.toString().replace("\n", "")

            if (carbsString.isNotEmpty()) {
                sendDataToWatch(carbsString, CARBS_HISTORY_FILENAME)
            }

            // BASAL HISTORY:
            aapsLogger.debug(LTag.BANGLEJS, "Gathering and sending basal history.")
            val timeStep = TimeUnit.MINUTES.toMillis(1) // Check every minute

            var lastRate: Double? = null
            //csvSnippet = mutableListOf<String>()

            val basalsList = JSONArray()

            // Iterate from 90 minutes ago to now, one minute at a time
            for (timestamp in startTime..now step timeStep) {
                aapsLogger.debug(LTag.BANGLEJS, "Gathering basal from ts $timestamp.")

                val profile = profileFunction.getProfile(timestamp)
                if (profile == null) {
                    aapsLogger.warn(LTag.BANGLEJS, "No profile found at timestamp $timestamp, skipping basal point.")
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
                    //csvSnippet.add("ts:${timestamp},rate:${roundedRate}")

                    aapsLogger.debug(LTag.BANGLEJS, "Basal changed at ts $timestamp. Old basal: $lastRate, new basal: $roundedRate.")
                    basalsList.put(JSONObject().apply {
                        put("ts", timestamp)
                        put("rate", roundedRate)
                    })
                    lastRate = roundedRate
                }
            }

            //dataString = csvSnippet.joinToString( "\\n" )

            val basalsObject = JSONObject().apply {
                put("data", basalsList)
            }
            val basalsString = basalsObject.toString().replace("\n", "")

            if (basalsString.isNotEmpty()) {
                sendDataToWatch(basalsString, BASALS_HISTORY_FILENAME)
            }
        }
    }

    private fun sendEventToWatchQueue(e: String?) {
        if (e != null) {
            val escapedE = e.replace("\"", "\\\"")
            lastWrittenEventFile = (lastWrittenEventFile+1)%MAX_NUMBER_OF_EVENT_FILES
            val fileName = EVENT_FILE_PREFIX+lastWrittenEventFile.toString().padStart(2, '0')
            val jsCommand = "require(\"Storage\").write(\"${fileName}\", \"${escapedE}\"); console.log(\"AAPS Phone wrote ${fileName}.\")"
            sendJsCommandHelper(jsCommand)
        }
    }


    private fun sendDataToWatch(data: String?, fileName: String) {
        if (data != null) {
            val escapedData = data.replace("\"", "\\\"")
            val jsCommand = "require(\"Storage\").write(\"${fileName}\", \"${escapedData}\"); console.log(\"AAPS Phone wrote ${fileName}.\")"
            sendJsCommandHelper(jsCommand)
        }
    }


    // Generic function to send any JS command
    private fun sendJsCommandHelper(command: String) {
        try {
            val intent = Intent("com.banglejs.uart.tx")
            intent.putExtra("line", command)
            sendBroadcast(intent)
            aapsLogger.debug(LTag.BANGLEJS, "Sent JS command: ${command.take(80)}...")
        } catch (e: Exception) {
            aapsLogger.error(LTag.BANGLEJS, "Failed to broadcast JS command", e)
        }
    }

    override fun onDestroy() {
        disposable.clear()
        super.onDestroy()
        aapsLogger.debug(LTag.BANGLEJS, "BangleJSService destroyed.")
    }

    companion object {
        private const val NOTIFICATION_ID = 457
    }
}