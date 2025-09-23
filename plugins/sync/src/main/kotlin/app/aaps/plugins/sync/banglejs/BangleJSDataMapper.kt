package app.aaps.plugins.sync.banglejs

import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.rx.bus.RxBus
import app.aaps.core.interfaces.rx.weardata.EventData
import org.json.JSONArray
import org.json.JSONObject
import javax.inject.Inject

//import kotlin.time.ExperimentalTime

object BangleJSDataMapper
{
    private const val MAX_JSON_LENGTH = 3
    private const val JSON_LENGTH_CUTOFF = 6

    fun deserialize(commandType: String, commandJson: String): EventData? {
        return when (commandType) {
            "ActionBolusPreCheck" -> deserializeBolusPreCheck(commandJson)
            "ActionBolusConfirmed" -> deserializeBolusConfirmed(commandJson)
            "ActionProfileSwitchPreCheck" -> deserializeProfileSwitchPreCheck(commandJson)
            "ActionProfileSwitchConfirmed" -> deserializeProfileSwitchConfirmed(commandJson)
            "ActionTempTargetPreCheck" -> deserializeTempTargetPreCheck(commandJson)
            "ActionTempTargetConfirmed" -> deserializeTempTargetConfirmed(commandJson)
            "RequestInitialData" -> EventData.ActionResendData("BangleJS")
            else -> null
        }
    }

    // The main serialize function now acts as a router.
    fun serialize(event: EventData): JSONObject? {
        return when (event) {
            is EventData.ConfirmAction -> serializeConfirmAction(event)
            is EventData.Status -> serializeStatus(event)
            is EventData.SingleBg -> serializeSingleBg(event)
            is EventData.GraphData -> serializeGraphData(event)
            is EventData.TreatmentData -> serializeTreatmentData(event)
            // Add other event types here as you need them (e.g., SingleBg, GraphData)
            else -> null
        }
    }

    // --- Serializers for Outgoing Events ---

    private fun serializeConfirmAction(action: EventData.ConfirmAction): JSONObject {
        val payload = JSONObject()
        payload.put("eventType", "ConfirmAction")
        payload.put("title", action.title)
        payload.put("message", action.message)
        action.returnCommand?.let {
            if (it is EventData.ActionBolusConfirmed) {
                val returnCmdJson = JSONObject()
                returnCmdJson.put("insulin", it.insulin)
                returnCmdJson.put("carbs", it.carbs)
                payload.put("returnCommandJson", returnCmdJson.toString())
                payload.put("returnCommandType", "ActionBolusConfirmed")
            }
            if (it is EventData.ActionProfileSwitchConfirmed) {
                val returnCmdJson = JSONObject()
                returnCmdJson.put("percentage", it.percentage)
                returnCmdJson.put("timeShift", it.timeShift)
                returnCmdJson.put("duration", it.duration)
                payload.put("returnCommandJson", returnCmdJson.toString())
                payload.put("returnCommandType", "ActionProfileSwitchConfirmed")
            }
            if (it is EventData.ActionTempTargetConfirmed) {
                val returnCmdJson = JSONObject()
                returnCmdJson.put("duration", it.duration)
                returnCmdJson.put("isMgdl", it.isMgdl)
                returnCmdJson.put("low", it.low)
                returnCmdJson.put("high", it.high)
                payload.put("returnCommandJson", returnCmdJson.toString())
                payload.put("returnCommandType", "ActionTempTargetConfirmed")
            }
        }
        return payload
    }

    private fun serializeGraphData(graphData: EventData.GraphData): JSONObject {
        val payloads = mutableListOf<JSONObject>()
        var payload = JSONObject()
        payload.put("eventType", "GraphData")
        val history = JSONArray()
        for (bgPoint in graphData.entries) {
            val point = JSONObject()
            point.put("ts", bgPoint.timeStamp)
            point.put("sgv", bgPoint.sgv)
            history.put(point)
            if (history.length() > MAX_JSON_LENGTH) {
                payload.put("history", history)
                payloads.add(payload)
                payload = JSONObject()
                payload.put("eventType", "GraphData")
            }
            if (history.length() > JSON_LENGTH_CUTOFF) {
                break
            }
        }
        payload.put("history", history)
        payloads.add(payload)

        //TODO fix the chunking!
        return payloads[0]
    }

    //@OptIn(ExperimentalTime::class)
    private fun serializeTreatmentData(treatmentData: EventData.TreatmentData): JSONObject {
        val payloads = mutableListOf<JSONObject>()
        //val now = Clock.System.now()
        //val twoHoursAgoInstant = now - 2.hours
        //val twoHoursAgoTimestamp = twoHoursAgoInstant.toEpochMilliseconds()
        var payload = JSONObject()
        payload.put("eventType", "TreatmentData")

        val basals = JSONArray()
        for (it in treatmentData.basals) {
            //if (it.startTime < twoHoursAgoTimestamp)
            //    break
            val b = JSONObject(); b.put("ts", it.startTime); b.put("end_ts", it.endTime); b.put("rate", it.amount)
            basals.put(b)
            if (basals.length() > MAX_JSON_LENGTH) {
                payload.put("basals", basals)
                payloads.add(payload)
                payload = JSONObject()
                payload.put("eventType", "TreatmentData")
            }
            if (basals.length() > JSON_LENGTH_CUTOFF) {
                break
            }
        }
        payload.put("basals", basals)
        payloads.add(payload)
        payload = JSONObject()
        payload.put("eventType", "TreatmentData")

        val temps = JSONArray()
        for (it in treatmentData.temps) {
            //if (it.startTime < twoHoursAgoTimestamp)
            //    break
            val t = JSONObject(); t.put("ts", it.startTime); t.put("end_ts", it.endTime); t.put("rate", it.amount)
            temps.put(t)
            if (temps.length() > MAX_JSON_LENGTH) {
                payload.put("temps", temps)
                payloads.add(payload)
                payload = JSONObject()
                payload.put("eventType", "TreatmentData")
            }
            if (temps.length() > JSON_LENGTH_CUTOFF) {
                break
            }
        }
        payload.put("temps", temps)
        payloads.add(payload)
        payload = JSONObject()
        payload.put("eventType", "TreatmentData")

        val treatments = JSONArray()
        for (it in treatmentData.boluses) {
            //if (it.date < twoHoursAgoTimestamp)
            //    break
            if (it.isValid) {
                val tr = JSONObject(); tr.put("ts", it.date); tr.put("insulin", it.bolus); tr.put("carbs", it.carbs); tr.put("isSMB", it.isSMB)
                treatments.put(tr)
            }
            if (treatments.length() > MAX_JSON_LENGTH) {
                payload.put("treatments", treatments)
                payloads.add(payload)
                payload = JSONObject()
                payload.put("eventType", "TreatmentData")
            }
            if (treatments.length() > JSON_LENGTH_CUTOFF) {
                break
            }
        }
        payload.put("treatments", treatments)
        payloads.add(payload)

        //TODO fix the chunking!
        return payloads[0]
    }

    // Serializer for the main status update
    private fun serializeStatus(status: EventData.Status): JSONObject {
        val payload = JSONObject()
        payload.put("eventType", "StatusUpdate")
        // We now take the data directly from the Status object,
        // which DataHandlerMobile has already prepared for us.
        payload.put("iob", status.iobSum)
        payload.put("cob", status.cob)
        return payload
    }
    /**
     * Serializes a lightweight, single BG update.
     */
    private fun serializeSingleBg(bg: EventData.SingleBg): JSONObject {
        val payload = JSONObject()
        // Use a distinct eventType so the watch knows this is a lightweight update
        payload.put("eventType", "SingleBgUpdate")
        payload.put("sgv", bg.sgvString)
        payload.put("trend", bg.slopeArrow)
        payload.put("ts", bg.timeStamp) // Include timestamp for age-of-reading checks
        return payload
    }

    // --- Deserializers for Incoming Commands ---

    private fun deserializeBolusPreCheck(jsonString: String): EventData.ActionBolusPreCheck {
        val json = JSONObject(jsonString)
        return EventData.ActionBolusPreCheck(json.getDouble("insulin"), json.getInt("carbs"))
    }

    private fun deserializeBolusConfirmed(jsonString: String): EventData.ActionBolusConfirmed {
        val json = JSONObject(jsonString)
        return EventData.ActionBolusConfirmed(json.getDouble("insulin"), json.getInt("carbs"))
    }

    private fun deserializeProfileSwitchPreCheck(jsonString: String): EventData.ActionProfileSwitchPreCheck {
        val json = JSONObject(jsonString)
        return EventData.ActionProfileSwitchPreCheck(json.getInt("timeShift"), json.getInt("percentage"), json.getInt("duration"))
    }
    private fun  deserializeProfileSwitchConfirmed(jsonString: String): EventData.ActionProfileSwitchConfirmed {
        val json = JSONObject(jsonString)
        return EventData.ActionProfileSwitchConfirmed(json.getInt("timeShift"), json.getInt("percentage"), json.getInt("duration"))
    }
    private fun  deserializeTempTargetPreCheck(jsonString: String): EventData.ActionTempTargetPreCheck {
        val json = JSONObject(jsonString)
        val command: EventData.ActionTempTargetPreCheck.TempTargetCommand = enumValueOf(json.getString("command"))
        return EventData.ActionTempTargetPreCheck(command)
    }
    private fun  deserializeTempTargetConfirmed(jsonString: String): EventData.ActionTempTargetConfirmed {
        val json = JSONObject(jsonString)
        return EventData.ActionTempTargetConfirmed(json.getBoolean("isMgdl"), json.getInt("duration"), json.getDouble("low"), json.getDouble("high"))
    }
}