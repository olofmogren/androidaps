package app.aaps.plugins.sync.gadgetbridge

import android.content.Context
import androidx.preference.PreferenceCategory
import androidx.preference.PreferenceManager
import androidx.preference.PreferenceScreen
import app.aaps.core.data.plugin.PluginType
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.interfaces.plugin.PluginBaseWithPreferences
import app.aaps.core.interfaces.plugin.PluginDescription
import app.aaps.core.interfaces.resources.ResourceHelper
import app.aaps.core.interfaces.rx.bus.RxBus
import app.aaps.core.interfaces.rx.events.EventPreferenceChange
import app.aaps.core.keys.interfaces.Preferences
import app.aaps.core.validators.DefaultEditTextValidator
import app.aaps.core.validators.preferences.AdaptiveStringPreference
import app.aaps.core.validators.preferences.AdaptiveSwitchPreference
import app.aaps.plugins.sync.R
import app.aaps.plugins.sync.gadgetbridge.keys.GadgetBridgeBooleanKey
import app.aaps.plugins.sync.gadgetbridge.keys.GadgetBridgeStringKey
import app.aaps.plugins.sync.wear.wearintegration.DataHandlerMobile
import io.reactivex.rxjava3.disposables.CompositeDisposable
import javax.inject.Inject
import javax.inject.Singleton
import io.reactivex.rxjava3.kotlin.plusAssign


@Singleton
class GadgetBridgePlugin @Inject constructor(
    private val context: Context,
    aapsLogger: AAPSLogger,
    rh: ResourceHelper,
    preferences: Preferences, // Inject the base Preferences interface
    private val rxBus: RxBus, // RxBus injection
    private val gadgetBridgeServiceHelper: GadgetBridgeServiceHelper,
    private val dataHandlerMobile: DataHandlerMobile
) : PluginBaseWithPreferences(
    pluginDescription = PluginDescription()
        .mainType(PluginType.SYNC)
        .pluginIcon(app.aaps.core.objects.R.drawable.ic_watch)
        .pluginName(R.string.gadgetbridge)
        .shortName(R.string.gadgetbridge_short)
        .preferencesId(PluginDescription.PREFERENCE_SCREEN)
        .description(R.string.gadgetbridge_description)
        .fragmentClass(GadgetBridgeFragment::class.java.name),
    // Declare the custom preference keys your plugin owns
    ownPreferences = listOf(GadgetBridgeBooleanKey::class.java, GadgetBridgeStringKey::class.java),
    aapsLogger, rh, preferences
) {

    private val disposable = CompositeDisposable()

    override fun onStart() {
        super.onStart()
        aapsLogger.debug(LTag.GADGETBRIDGE, "onStart() called. Plugin is becoming active.")
        if (isEnabled()) {
            aapsLogger.debug(LTag.GADGETBRIDGE, "GadgetBridge Plugin enabled. Starting Service.")
            gadgetBridgeServiceHelper.startService(context)
            Thread {
                try {
                    Thread.sleep(5000) // Wait 5 seconds
                    aapsLogger.debug(LTag.GADGETBRIDGE, "Triggering initial data sync from onStart().")
                    dataHandlerMobile.resendData("GadgetBridgePlugin")
                } catch (e: InterruptedException) {
                    // Ignore
                }
            }.start()
        }
        else {
            aapsLogger.debug(LTag.GADGETBRIDGE, "onStart() called but plguin is not enabled.")
        }

        // Start or stop the service based on the CURRENT preference state
        updateServiceState()

        // --- ADD THIS SUBSCRIPTION ---
        // Listen for future changes to ANY preference
        disposable += rxBus
            .toObservable(EventPreferenceChange::class.java)
            .subscribe({ event ->
                           // Check if the key that changed is OUR enable key
                           if (event.isChanged(GadgetBridgeBooleanKey.Enabled.key)) {
                               aapsLogger.debug(LTag.GADGETBRIDGE, "Enable preference changed. Updating service state.")
                               updateServiceState()
                           }
                       }, { error ->
                           aapsLogger.error(LTag.GADGETBRIDGE, "Error in preference change subscription", error)
                       })
    }

    override fun onStop() {
        aapsLogger.debug(LTag.GADGETBRIDGE, "GadgetBridge Plugin stopped. Stopping Service.")
        gadgetBridgeServiceHelper.stopService(context)
        super.onStop()
    }
    // This is a private helper method to avoid code duplication
    private fun updateServiceState() {
        if (isEnabled()) {
            aapsLogger.debug(LTag.GADGETBRIDGE, "Plugin is enabled. Ensuring service is started.")
            gadgetBridgeServiceHelper.startService(context)
        } else {
            aapsLogger.debug(LTag.GADGETBRIDGE, "Plugin is disabled. Ensuring service is stopped.")
            gadgetBridgeServiceHelper.stopService(context)
        }
    }

    override fun isEnabled(): Boolean {
        // The enabled state is determined by our custom boolean key
        val isEnabled = preferences.get(GadgetBridgeBooleanKey.Enabled)
        aapsLogger.debug(LTag.GADGETBRIDGE, "isEnabled() check: returning $isEnabled")
        return isEnabled
    }


    override fun addPreferenceScreen(preferenceManager: PreferenceManager, parent: PreferenceScreen, context: Context, requiredKey: String?) {
        val category = PreferenceCategory(context)
        parent.addPreference(category)
        category.apply {
            key = "gadgetbridge_settings"
            title = "GadgetBridge Settings"
            initialExpandedChildrenCount = 0

            addPreference(AdaptiveSwitchPreference(ctx = context, booleanKey = GadgetBridgeBooleanKey.Enabled, summary = R.string.gadgetbridge_enable_sync_summary, title = R.string.gadgetbridge_enable_sync))

            // This preference configures the OUTGOING intent action.
            addPreference(
                AdaptiveStringPreference(
                    ctx = context,
                    stringKey = GadgetBridgeStringKey.OutgoingIntentAction,
                    title = R.string.gadgetbridge_setting_intent_action_title,
                    summary = R.string.gadgetbridge_setting_intent_action_summary,
                    validatorParams = DefaultEditTextValidator.Parameters(emptyAllowed = false)
                )
            )
        }
    }

}