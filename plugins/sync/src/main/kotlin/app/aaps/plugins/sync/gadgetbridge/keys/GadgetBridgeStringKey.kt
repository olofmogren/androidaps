package app.aaps.plugins.sync.gadgetbridge.keys

import app.aaps.core.keys.interfaces.BooleanPreferenceKey
import app.aaps.core.keys.interfaces.StringPreferenceKey

enum class GadgetBridgeStringKey(
    override val key: String,
    override val defaultValue: String,
    override val defaultedBySM: Boolean = false,
    override val showInApsMode: Boolean = true,
    override val showInNsClientMode: Boolean = true,
    override val showInPumpControlMode: Boolean = true,
    override val dependency: BooleanPreferenceKey? = null,
    override val negativeDependency: BooleanPreferenceKey? = null,
    override val hideParentScreenIfHidden: Boolean = false,
    override val isPassword: Boolean = false,
    override val isPin: Boolean = false,
    override val exportable: Boolean = true
) : StringPreferenceKey {

    // This key now defines the OUTGOING action.
    // The default is the standard for Bangle.js communication from Gadgetbridge.

    OutgoingIntentAction(key = "gadgetbridge_outgoing_intent_action", defaultValue = "com.banglejs.uart.tx"),
    //OutgoingIntentAction(key = "gadgetbridge_outgoing_intent_action", defaultValue = "nodomain.gadgetbridge.banglejs.JSON"),
}

