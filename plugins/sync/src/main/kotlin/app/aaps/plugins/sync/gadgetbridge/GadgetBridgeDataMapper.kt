package app.aaps.plugins.sync.gadgetbridge

import app.aaps.core.interfaces.rx.weardata.EventData
import org.json.JSONArray
import org.json.JSONObject
import kotlin.time.Clock
import kotlin.time.Duration.Companion.hours
import kotlin.time.ExperimentalTime

object GadgetBridgeDataMapper {
    private const val MAX_JSON_LENGTH = 3
    private const val JSON_LENGTH_CUTOFF = 6

    fun deserialize(commandType: String, commandJson: String): EventData? {
        return when (commandType) {
            "ActionBolusPreCheck" -> deserializeBolusPreCheck(commandJson)
            "ActionBolusConfirmed" -> deserializeBolusConfirmed(commandJson)
            "RequestInitialData" -> EventData.ActionResendData("GadgetBridge")
            else -> null
        }
    }

    // The main serialize function now acts as a router.
    fun serialize(event: EventData): List<JSONObject>? {
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

    private fun serializeConfirmAction(action: EventData.ConfirmAction): List<JSONObject> {
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
        }
        return listOf(payload)
    }

    private fun serializeGraphData(graphData: EventData.GraphData): List<JSONObject> {
        val payload = JSONObject()
        payload.put("eventType", "GraphData")
        val history = JSONArray()
        graphData.entries.forEach { bgPoint ->
            val point = JSONObject()
            point.put("ts", bgPoint.timeStamp)
            point.put("sgv", bgPoint.sgv)
            history.put(point)
        }
        payload.put("history", history)
        return listOf(payload)
    }

    @OptIn(ExperimentalTime::class)
    private fun serializeTreatmentData(treatmentData: EventData.TreatmentData): List<JSONObject> {
        val payloads = mutableListOf<JSONObject>()
        val now = Clock.System.now()
        val twoHoursAgoInstant = now - 2.hours
        val twoHoursAgoTimestamp = twoHoursAgoInstant.toEpochMilliseconds()
        var payload = JSONObject()
        payload.put("eventType", "TreatmentData")

        val basals = JSONArray()
        for (it in treatmentData.basals) {
            if (it.startTime < twoHoursAgoTimestamp)
                break
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
            if (it.startTime < twoHoursAgoTimestamp)
                break
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
            if (it.date < twoHoursAgoTimestamp)
                break
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

        return payloads
    }

    // Serializer for the main status update
    private fun serializeStatus(status: EventData.Status): List<JSONObject> {
        val payload = JSONObject()
        payload.put("eventType", "StatusUpdate")
        // We now take the data directly from the Status object,
        // which DataHandlerMobile has already prepared for us.
        payload.put("iob", status.iobSum)
        payload.put("cob", status.cob)
        return listOf(payload)
    }
    /**
     * Serializes a lightweight, single BG update.
     */
    private fun serializeSingleBg(bg: EventData.SingleBg): List<JSONObject> {
        val payload = JSONObject()
        // Use a distinct eventType so the watch knows this is a lightweight update
        payload.put("eventType", "SingleBgUpdate")
        payload.put("sgv", bg.sgvString)
        payload.put("trend", bg.slopeArrow)
        payload.put("ts", bg.timeStamp) // Include timestamp for age-of-reading checks
        return listOf(payload)
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
}